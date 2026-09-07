import { createHash } from 'node:crypto';
import type { CorridorId, GeoPoint, StreetcarNetwork, StreetcarSite } from '../dashboard/src/streetcar-data';

export interface WaitSnapshot {
  vid: string; route: string; trip_id: string | null;
  received_at: number; provider_at: number | null;
  lat: number; lon: number; off_route: boolean; speed: number | null;
}
export interface WaitEvent {
  id: string; site_id: string; corridor: CorridorId; route: string;
  context: 'signal_only' | 'stop_only' | 'both';
  started_at: number; ended_at: number; duration_seconds: number;
}
export const WAIT_METHOD = {
  name: 'streetcar-receipt-waits-v2', stationary_diameter_meters: 12,
  required_reported_speed: 0,
  minimum_seconds: 20, minimum_snapshots: 3, maximum_receipt_gap_seconds: 25,
  maximum_provider_age_seconds: 120, movement_meters: 25, movement_bracket_seconds: 45,
  site_radius_meters: 40, terminal_radius_meters: 60, track_tolerance_meters: 35,
} as const;

type XY = { x: number; y: number };
type Point = { snapshot: WaitSnapshot; xy: XY; valid: boolean };
type Track = { a: XY; b: XY };
type RouteGeometry = { corridor: CorridorId; tracks: Track[]; terminals: XY[]; sites: Array<{ site: StreetcarSite; point: XY }> };
const DEGREE_METERS = Math.PI * 6371000 / 180;
const LONGITUDE_SCALE = DEGREE_METERS * Math.cos(29.95 * Math.PI / 180);
const geometryCache = new WeakMap<StreetcarNetwork, Map<string, RouteGeometry>>();

function xy(point: GeoPoint): XY { return { x: (point.lon + 90.1) * LONGITUDE_SCALE, y: (point.lat - 29.95) * DEGREE_METERS }; }
function distance(a: XY, b: XY): number { return Math.hypot(a.x - b.x, a.y - b.y); }
function distanceToTrack(point: XY, track: Track): number {
  const dx = track.b.x - track.a.x; const dy = track.b.y - track.a.y;
  const lengthSquared = dx * dx + dy * dy;
  if (!lengthSquared) return distance(point, track.a);
  const t = Math.max(0, Math.min(1, ((point.x - track.a.x) * dx + (point.y - track.a.y) * dy) / lengthSquared));
  return Math.hypot(point.x - track.a.x - dx * t, point.y - track.a.y - dy * t);
}
function geometry(network: StreetcarNetwork): Map<string, RouteGeometry> {
  const cached = geometryCache.get(network);
  if (cached) return cached;
  const routes = new Map<string, RouteGeometry>();
  for (const path of network.paths) {
    const route = routes.get(path.route) ?? { corridor: path.corridor, tracks: [], terminals: [], sites: [] };
    const points = path.points.map(xy);
    if (points.length) route.terminals.push(points[0], points.at(-1)!);
    for (let n = 1; n < points.length; n++) route.tracks.push({ a: points[n - 1], b: points[n] });
    routes.set(path.route, route);
  }
  for (const site of network.sites) for (const routeId of site.routes) {
    const route = routes.get(routeId);
    if (route?.corridor === site.corridor) route.sites.push({ site, point: xy(site) });
  }
  geometryCache.set(network, routes);
  return routes;
}
function identical(a: WaitSnapshot, b: WaitSnapshot): boolean {
  return a.route === b.route && a.trip_id === b.trip_id && a.provider_at === b.provider_at &&
    a.lat === b.lat && a.lon === b.lon && a.off_route === b.off_route && a.speed === b.speed;
}

/** Candidate near-stationary episodes on the collector receipt clock.
 *
 * Repeated positions may be cached or smoothed by the provider, even while a
 * minute timestamp is considered fresh. These events do not establish physical
 * stop time, red-light status, boarding time, or signal-caused delay. The output
 * duration is only the first-to-last receipt span of stable positions, with
 * observed movement on both sides. Every snapshot in the stable cluster must
 * explicitly report zero speed. Missing speed is unknown; any positive or
 * invalid speed vetoes the entire cluster, rather than selecting its zero-speed
 * subset. Callers must preserve missing speed as null, never coerce it to zero.
 * Zero speed and repeated coordinates can still both be cached observations.
 *
 * A mixed episode is assigned once to its nearest road signal. Without a
 * directional fix/boarding observation, any route-applicable platform within
 * 40 m conservatively makes the context mixed; the detector cannot prove which
 * platform was served. Terminus and railway-control contexts are excluded.
 */
export function detectStreetcarWaits(network: StreetcarNetwork, snapshots: WaitSnapshot[]): WaitEvent[] {
  const routes = geometry(network);
  const groups = new Map<string, WaitSnapshot[]>();
  for (const snapshot of snapshots) {
    if (!snapshot.vid || !routes.has(snapshot.route)) continue;
    const group = groups.get(snapshot.vid) ?? [];
    group.push(snapshot); groups.set(snapshot.vid, group);
  }
  const events: WaitEvent[] = [];
  const analyzeRun = (points: Point[]) => {
    if (points.length < WAIT_METHOD.minimum_snapshots + 2) return;
    const route = routes.get(points[0].snapshot.route)!;
    let start = 0;
    while (start < points.length) {
      const first = points[start];
      let end = start + 1;
      let minX = first.xy.x; let maxX = first.xy.x; let minY = first.xy.y; let maxY = first.xy.y;
      let sumX = first.xy.x; let sumY = first.xy.y;
      let explicitlyStopped = first.snapshot.speed === WAIT_METHOD.required_reported_speed;
      while (end < points.length) {
        const point = points[end];
        const nextMinX = Math.min(minX, point.xy.x); const nextMaxX = Math.max(maxX, point.xy.x);
        const nextMinY = Math.min(minY, point.xy.y); const nextMaxY = Math.max(maxY, point.xy.y);
        // Bound the diameter against a fixed cluster, not the preceding point:
        // continuous motion must not become a wait by chaining small steps.
        if (Math.hypot(nextMaxX - nextMinX, nextMaxY - nextMinY) > WAIT_METHOD.stationary_diameter_meters) break;
        minX = nextMinX; maxX = nextMaxX; minY = nextMinY; maxY = nextMaxY;
        sumX += point.xy.x; sumY += point.xy.y;
        explicitlyStopped &&= point.snapshot.speed === WAIT_METHOD.required_reported_speed;
        end++;
      }
      const last = points[end - 1]; const count = end - start;
      const elapsed = last.snapshot.received_at - first.snapshot.received_at;
      if (explicitlyStopped && count >= WAIT_METHOD.minimum_snapshots && elapsed >= WAIT_METHOD.minimum_seconds) {
        const center = { x: sumX / count, y: sumY / count };
        const movement = (index: number, increment: number, boundaryTime: number): boolean => {
          for (let n = index; n >= 0 && n < points.length; n += increment) {
            if (Math.abs(points[n].snapshot.received_at - boundaryTime) > WAIT_METHOD.movement_bracket_seconds) return false;
            if (distance(points[n].xy, center) >= WAIT_METHOD.movement_meters) return true;
          }
          return false;
        };
        const bracketed = movement(start - 1, -1, first.snapshot.received_at) && movement(end, 1, last.snapshot.received_at);
        const onTrack = route.tracks.some(track => distanceToTrack(center, track) <= WAIT_METHOD.track_tolerance_meters);
        const terminal = route.terminals.some(point => distance(point, center) <= WAIT_METHOD.terminal_radius_meters);
        if (bracketed && onTrack && !terminal) {
          const nearby = route.sites.map(entry => ({ ...entry, distance: distance(entry.point, center) }))
            .filter(entry => entry.distance <= WAIT_METHOD.site_radius_meters)
            .sort((a, b) => a.distance - b.distance || a.site.id.localeCompare(b.site.id));
          if (!nearby.some(entry => entry.site.kind === 'rail_signal')) {
            const signal = nearby.find(entry => entry.site.kind === 'signal');
            const stop = nearby.find(entry => entry.site.kind === 'stop');
            const site = signal?.site ?? stop?.site;
            if (site) {
              const context = signal ? (stop ? 'both' : 'signal_only') : 'stop_only';
              const id = createHash('sha256').update(JSON.stringify([
                first.snapshot.vid, first.snapshot.route, first.snapshot.trip_id,
                first.snapshot.received_at, last.snapshot.received_at, site.id,
              ])).digest('hex');
              events.push({ id, site_id: site.id, corridor: route.corridor, route: first.snapshot.route,
                context, started_at: first.snapshot.received_at, ended_at: last.snapshot.received_at, duration_seconds: elapsed });
            }
          }
        }
      }
      start = end;
    }
  };
  for (const group of groups.values()) {
    group.sort((a, b) => a.received_at - b.received_at);
    const points: Point[] = [];
    for (const snapshot of group) {
      const previous = points.at(-1);
      if (previous?.snapshot.received_at === snapshot.received_at) {
        if (!identical(previous.snapshot, snapshot)) previous.valid = false;
        continue;
      }
      const age = snapshot.provider_at == null ? NaN : snapshot.received_at - snapshot.provider_at;
      const valid = [snapshot.received_at, snapshot.lat, snapshot.lon, age].every(Number.isFinite) &&
        age >= 0 && age <= WAIT_METHOD.maximum_provider_age_seconds && Math.abs(snapshot.lat) <= 90 &&
        Math.abs(snapshot.lon) <= 180 && !snapshot.off_route;
      points.push({ snapshot, xy: xy(snapshot), valid });
    }
    let run: Point[] = [];
    for (const point of points) {
      const previous = run.at(-1);
      if (!point.valid || previous && (point.snapshot.route !== previous.snapshot.route ||
          point.snapshot.trip_id !== previous.snapshot.trip_id ||
          point.snapshot.received_at - previous.snapshot.received_at > WAIT_METHOD.maximum_receipt_gap_seconds ||
          point.snapshot.provider_at! < previous.snapshot.provider_at!)) {
        analyzeRun(run); run = [];
      }
      if (point.valid) run.push(point);
    }
    analyzeRun(run);
  }
  return events.sort((a, b) => a.started_at - b.started_at || a.id.localeCompare(b.id));
}
