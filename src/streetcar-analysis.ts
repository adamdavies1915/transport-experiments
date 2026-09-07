import type {
  CorridorId, ExposureCategory, GeoPoint, StreetcarBin, StreetcarData,
  StreetcarNetwork, StreetcarPassage, StreetcarPath, StreetcarQuality, StreetcarSite, StreetcarSiteBin,
} from '../dashboard/src/streetcar-data';
import { createHash } from 'node:crypto';

export interface StreetcarObservation {
  vid: string; route: string; trip_id: string | null; at: number;
  lat: number; lon: number; off_route: boolean; heading?: number;
}

export const METHOD: StreetcarData['method'] = {
  timestamp_quantization_seconds: 60,
  name: 'streetcar-fixed-window-v2', max_gap_seconds: 90, window_meters: 200,
  feature_radius_meters: 40, track_tolerance_meters: 35,
  terminal_radius_meters: 60, slow_mph: 3,
  limitations: [
    'Speeds describe complete passages through fixed, non-overlapping 200 m track windows. Signal associations do not establish seconds caused by a red light; signal phases are unavailable.',
    'Every window is classified by all mapped stops and traffic signals within 40 m of that fixed track section, independently of vehicle speed. Windows meeting both remain combined.',
    'OSM traffic signals are nearby mapped controls; their applicability to streetcars and historical presence are unverified. Missing mapped signals can affect the baseline.',
    'The source relay polls about every 10 seconds but requests minute-resolution vehicle timestamps. Coordinates can change while that timestamp stays the same; a 60-second timestamp step does not establish the GPS update cadence.',
    'Boundary times are estimated by linear interpolation on retained provider timestamps. Displayed duration bounds include bracketing observations and up to 60 seconds of timestamp truncation uncertainty per passage in each direction; interpolation does not locate a stop or a signal phase.',
    'The timing ranges assume each position belongs to its reported minute. They are not statistical confidence intervals and do not cover GPS position error, incorrect timestamp-to-position association, provider smoothing or stale data. Receipt time is not a GPS fix time.',
    'Seeing a vehicle exactly at a boundary does not establish when it first arrived there; a passage needs a prior observation outside its entry boundary.',
    'Railway-signal exposure, terminal proximity, ambiguous tracks or directions, off-route points and reported timestamp gaps over 90 seconds are excluded. Timestamp truncation can make actual gaps longer than reported.',
    'Site totals overlap: the same complete 200 m passage can appear at several sites. Do not sum sites to obtain corridor totals.',
    'Direction, estimated local entry hour and weekday/weekend remain separate. Timestamp precision can shift assignments near an hour boundary. Unadjusted comparisons can reflect traffic, street geometry, boarding or service differences.',
    'Only pairs within the requested Chicago calendar day are analyzed; intervals across midnight are omitted.',
    'Incomplete windows and wholly stationary runs do not produce passages. No-mapped-feature windows are descriptive comparisons, not a free-flow or causal baseline.',
    'Slow time is the estimated duration of complete passages whose average speed is below 3 mph; it is not measured stopped time.',
    'GPS quality counts accepted observation intervals; passage counts count completed 200 m windows and need not equal GPS interval counts.',
    'Historical coordinates sharing a minute timestamp may represent distinct updates whose original receipt order was not saved. Those timestamp collisions are excluded; coverage differs across collection periods.',
  ],
};

const GRID_METERS = 80;
const METERS_PER_DEGREE = Math.PI * 6371000 / 180;
const LONGITUDE_SCALE = METERS_PER_DEGREE * Math.cos(29.95 * Math.PI / 180);
const MAX_SPEED_METERS_SECOND = 30;
const REVERSE_JITTER_METERS = 8;
const AMBIGUOUS_SCORE_METERS = 8;
const MPH_TO_METERS_SECOND = 0.44704;
type XY = { x: number; y: number };
type Segment = { path: number; a: XY; b: XY; length: number; start: number };
type Projection = { position: number; distance: number };
type Exposure = { from: number; to: number; site: StreetcarSite };
type IndexedPath = { path: StreetcarPath; length: number; exposure: Exposure[] };
type IndexedNetwork = {
  paths: IndexedPath[]; segments: Segment[]; grid: Map<string, number[]>;
  corridors: Map<string, CorridorId>;
};
type IndexedPoint = {
  observation: StreetcarObservation; projections: Map<number, Projection[]>;
  valid: boolean; conflict: boolean;
};
type PairOption = { path: number; from: number; to: number; distance: number; score: number };
type Pair = { a: IndexedPoint; b: IndexedPoint; options: PairOption[] };
type MatchedPair = { pair: Pair; option: PairOption };
type Accumulator<T extends StreetcarBin> = { bin: T; vehicles: Set<string> };

const indexCache = new WeakMap<StreetcarNetwork, IndexedNetwork>();
const hourFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/Chicago', hour: 'numeric', hourCycle: 'h23',
});

function xy(point: GeoPoint): XY {
  return { x: (point.lon + 90.1) * LONGITUDE_SCALE, y: (point.lat - 29.95) * METERS_PER_DEGREE };
}
function gridKey(x: number, y: number): string { return `${x},${y}`; }
function nearbySegments(index: IndexedNetwork, point: XY): number[] {
  return index.grid.get(gridKey(Math.floor(point.x / GRID_METERS), Math.floor(point.y / GRID_METERS))) ?? [];
}
function projection(segment: Segment, point: XY): Projection {
  const dx = segment.b.x - segment.a.x; const dy = segment.b.y - segment.a.y;
  const t = Math.max(0, Math.min(1, ((point.x - segment.a.x) * dx + (point.y - segment.a.y) * dy) / segment.length ** 2));
  return { position: segment.start + t * segment.length,
    distance: Math.hypot(point.x - segment.a.x - t * dx, point.y - segment.a.y - t * dy) };
}
function stopOnPath(site: StreetcarSite, path: StreetcarPath): boolean {
  if (site.kind !== 'stop') return true;
  const ids = new Set(path.stop_ids);
  return site.source_ids.some(id => ids.has(id) || ids.has(id.replace(/^gtfs:(?:stop:)?/, '')));
}

// Circle/segment intersections preserve exposure along curves and intervals
// whose endpoints both lie outside a feature buffer. A nearest-point label
// would silently omit crossings between the GPS observations.
function featureExposure(segment: Segment, point: XY): [number, number] | null {
  const ux = (segment.b.x - segment.a.x) / segment.length;
  const uy = (segment.b.y - segment.a.y) / segment.length;
  const dx = point.x - segment.a.x; const dy = point.y - segment.a.y;
  const along = dx * ux + dy * uy;
  const perpendicularSquared = Math.max(0, dx * dx + dy * dy - along * along);
  const radiusSquared = METHOD.feature_radius_meters ** 2;
  if (perpendicularSquared > radiusSquared) return null;
  const reach = Math.sqrt(radiusSquared - perpendicularSquared);
  const from = Math.max(0, along - reach); const to = Math.min(segment.length, along + reach);
  return from <= to ? [segment.start + from, segment.start + to] : null;
}

function buildIndex(network: StreetcarNetwork): IndexedNetwork {
  const cached = indexCache.get(network);
  if (cached) return cached;
  const index: IndexedNetwork = { paths: [], segments: [], grid: new Map(), corridors: new Map() };
  for (const corridor of network.corridors) for (const route of corridor.routes) index.corridors.set(route, corridor.id);
  for (const path of network.paths) {
    let length = 0;
    const pathIndex = index.paths.length;
    for (let n = 1; n < path.points.length; n++) {
      const a = xy(path.points[n - 1]); const b = xy(path.points[n]);
      const segmentLength = Math.hypot(a.x - b.x, a.y - b.y);
      if (!Number.isFinite(segmentLength) || segmentLength < 0.01) continue;
      const segment: Segment = { path: pathIndex, a, b, length: segmentLength, start: length };
      const id = index.segments.push(segment) - 1;
      length += segmentLength;
      const padding = Math.max(METHOD.feature_radius_meters, METHOD.track_tolerance_meters);
      const x0 = Math.floor((Math.min(a.x, b.x) - padding) / GRID_METERS);
      const x1 = Math.floor((Math.max(a.x, b.x) + padding) / GRID_METERS);
      const y0 = Math.floor((Math.min(a.y, b.y) - padding) / GRID_METERS);
      const y1 = Math.floor((Math.max(a.y, b.y) + padding) / GRID_METERS);
      for (let x = x0; x <= x1; x++) for (let y = y0; y <= y1; y++) {
        const key = gridKey(x, y); const bucket = index.grid.get(key) ?? [];
        bucket.push(id); index.grid.set(key, bucket);
      }
    }
    index.paths.push({ path, length, exposure: [] });
  }
  for (const site of network.sites) {
    const point = xy(site);
    for (const id of nearbySegments(index, point)) {
      const segment = index.segments[id]; const path = index.paths[segment.path];
      if (site.corridor !== path.path.corridor || !site.routes.includes(path.path.route) || !stopOnPath(site, path.path)) continue;
      const bounds = featureExposure(segment, point);
      if (bounds) path.exposure.push({ from: bounds[0], to: bounds[1], site });
    }
  }
  for (const path of index.paths) path.exposure.sort((a, b) => a.from - b.from);
  indexCache.set(network, index);
  return index;
}

function indexPoint(index: IndexedNetwork, observation: StreetcarObservation): IndexedPoint {
  const projections = new Map<number, Projection[]>();
  const valid = [observation.at, observation.lat, observation.lon].every(Number.isFinite) &&
    Math.abs(observation.lat) <= 90 && Math.abs(observation.lon) <= 180;
  if (!valid || observation.off_route) return { observation, projections, valid, conflict: false };
  const point = xy(observation);
  for (const id of nearbySegments(index, point)) {
    const segment = index.segments[id];
    if (index.paths[segment.path].path.route !== observation.route) continue;
    const p = projection(segment, point);
    if (p.distance > METHOD.track_tolerance_meters) continue;
    const positions = projections.get(segment.path) ?? [];
    const same = positions.find(existing => Math.abs(existing.position - p.position) < 1);
    if (same) { if (p.distance < same.distance) Object.assign(same, p); }
    else positions.push(p);
    projections.set(segment.path, positions);
  }
  // Adjacent segment endpoints can yield several projections. Keep every
  // materially competitive location, including repeated track in a loop, so
  // ambiguity is rejected rather than resolved by array ordering.
  for (const [path, positions] of projections) {
    const best = Math.min(...positions.map(p => p.distance));
    const distinct: Projection[] = [];
    for (const candidate of positions.filter(p => p.distance <= best + AMBIGUOUS_SCORE_METERS).sort((a, b) => a.distance - b.distance)) {
      // Dense GTFS geometry contains many adjacent vertices within the GPS
      // tolerance. These describe the same local projection, not a loop.
      if (!distinct.some(p => Math.abs(p.position - candidate.position) <= 50)) distinct.push(candidate);
    }
    projections.set(path, distinct);
  }
  return { observation, projections, valid, conflict: false };
}

function optionsForPair(a: IndexedPoint, b: IndexedPoint): PairOption[] {
  const options: PairOption[] = [];
  const elapsed = b.observation.at - a.observation.at;
  for (const [path, starts] of a.projections) {
    const ends = b.projections.get(path);
    if (!ends) continue;
    for (const start of starts) for (const end of ends) {
      const progress = end.position - start.position;
      if (progress < -REVERSE_JITTER_METERS || progress / elapsed > MAX_SPEED_METERS_SECOND) continue;
      options.push({ path, from: start.position, to: end.position,
        distance: Math.max(0, progress), score: start.distance + end.distance });
    }
  }
  return options.sort((a, b) => a.score - b.score);
}

function pairDirection(index: IndexedNetwork, option: PairOption): string {
  return index.paths[option.path].path.direction;
}
function chooseOption(index: IndexedNetwork, pair: Pair, contextDirection: string | undefined): PairOption | string {
  let options = pair.options;
  if (!options.length) return pair.a.projections.size && pair.b.projections.size ? 'inconsistent_track_progress' : 'unmatched_track';
  options = options.filter(p => p.score <= options[0].score + AMBIGUOUS_SCORE_METERS);
  if (new Set(options.map(p => pairDirection(index, p))).size > 1 && contextDirection != null) {
    options = options.filter(p => pairDirection(index, p) === contextDirection);
  }
  if (!options.length) return 'ambiguous_direction';
  const best = options[0];
  for (const candidate of options.slice(1)) {
    if (pairDirection(index, best) !== pairDirection(index, candidate)) return 'ambiguous_direction';
    if (Math.abs(candidate.distance - best.distance) > 15 ||
        (candidate.path === best.path && Math.abs(candidate.from - best.from) > 50)) return 'ambiguous_track';
    // Distinct shapes are safe alternatives only if their local geometry and
    // feature exposure agree; checked after projection below.
  }
  return best;
}

function exposedSites(path: IndexedPath, option: PairOption): StreetcarSite[] {
  const from = Math.min(option.from, option.to); const to = Math.max(option.from, option.to);
  const sites = new Map<string, StreetcarSite>();
  for (const exposure of path.exposure) {
    if (exposure.from > to) break;
    if (exposure.to >= from) sites.set(exposure.site.id, exposure.site);
  }
  return [...sites.values()];
}
function terminalExposure(path: IndexedPath, option: PairOption): boolean {
  return Math.min(option.from, option.to) < METHOD.terminal_radius_meters ||
    Math.max(option.from, option.to) > path.length - METHOD.terminal_radius_meters;
}
function categoryFor(sites: StreetcarSite[]): ExposureCategory {
  const signal = sites.some(site => site.kind === 'signal'); const stop = sites.some(site => site.kind === 'stop');
  return signal ? (stop ? 'both' : 'signal_only') : (stop ? 'stop_only' : 'neither');
}
function sameExposure(index: IndexedNetwork, pair: Pair, best: PairOption): boolean {
  const bestPath = index.paths[best.path];
  const ids = exposedSites(bestPath, best).map(s => s.id).sort().join('|');
  const terminal = terminalExposure(bestPath, best);
  return pair.options.filter(p => p.path !== best.path && p.score <= pair.options[0].score + AMBIGUOUS_SCORE_METERS &&
    pairDirection(index, p) === pairDirection(index, best)).every(p =>
    terminalExposure(index.paths[p.path], p) === terminal && exposedSites(index.paths[p.path], p).map(s => s.id).sort().join('|') === ids);
}

/** Pure, idempotent analysis. Each accepted interval occurs once in corridor
 * bins. Site bins intentionally repeat an interval at every exposed site.
 * Raw-point exclusions have a `_points` suffix; other reasons count intervals.
 */
export function analyzeStreetcarIntervals(network: StreetcarNetwork, date: string, observations: StreetcarObservation[],
  onAccepted?: (match: MatchedPair) => void): {
  bins: StreetcarBin[]; site_bins: StreetcarSiteBin[]; quality: StreetcarQuality[];
} {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(`${date}T12:00:00Z`))) throw new Error('Expected YYYY-MM-DD date');
  const index = buildIndex(network);
  const bins = new Map<string, Accumulator<StreetcarBin>>();
  const siteBins = new Map<string, Accumulator<StreetcarSiteBin>>();
  const quality = new Map<CorridorId, StreetcarQuality>(network.corridors.map(c => [c.id,
    { date, corridor: c.id, raw_points: 0, candidate_intervals: 0, accepted_intervals: 0, excluded: {} }]));
  const weekday = new Date(`${date}T12:00:00Z`).getUTCDay();
  const dayType = weekday === 0 || weekday === 6 ? 'weekend' : 'weekday';
  const exclude = (q: StreetcarQuality, reason: string) => { q.excluded[reason] = (q.excluded[reason] ?? 0) + 1; };
  const groups = new Map<string, StreetcarObservation[]>();
  for (const observation of observations) {
    const corridor = index.corridors.get(observation.route);
    if (!corridor) continue;
    quality.get(corridor)!.raw_points++;
    const group = groups.get(observation.vid) ?? [];
    group.push(observation); groups.set(observation.vid, group);
  }
  const add = <T extends StreetcarBin>(map: Map<string, Accumulator<T>>, key: string, initial: T, observation: StreetcarObservation,
    elapsed: number, distance: number) => {
    let aggregate = map.get(key);
    if (!aggregate) { aggregate = { bin: initial, vehicles: new Set() }; map.set(key, aggregate); }
    aggregate.bin.intervals++;
    aggregate.bin.duration_seconds += elapsed;
    aggregate.bin.distance_meters += distance;
    if (distance / elapsed < METHOD.slow_mph * MPH_TO_METERS_SECOND) aggregate.bin.slow_seconds += elapsed;
    aggregate.vehicles.add(observation.vid);
  };
  const analyzeRun = (pairs: Pair[]) => {
    if (!pairs.length) return;
    const votes = new Map<string, number>();
    for (const pair of pairs) {
      if (!pair.options.length) continue;
      const options = pair.options.filter(p => p.score <= pair.options[0].score + AMBIGUOUS_SCORE_METERS && p.distance >= 15);
      const directions = new Set(options.map(p => pairDirection(index, p)));
      if (directions.size === 1) {
        const direction = [...directions][0]; votes.set(direction, (votes.get(direction) ?? 0) + 1);
      }
    }
    const sortedVotes = [...votes].sort((a, b) => b[1] - a[1]);
    const totalVotes = sortedVotes.reduce((sum, [, count]) => sum + count, 0);
    const contextDirection = sortedVotes[0]?.[1] >= Math.max(1, totalVotes * 0.8) ? sortedVotes[0][0] : undefined;
    for (const pair of pairs) {
      const observation = pair.a.observation;
      const q = quality.get(index.corridors.get(observation.route)!)!;
      const chosen = chooseOption(index, pair, contextDirection);
      if (typeof chosen === 'string') { exclude(q, chosen); continue; }
      const path = index.paths[chosen.path];
      if (!sameExposure(index, pair, chosen)) { exclude(q, 'ambiguous_feature_exposure'); continue; }
      if (terminalExposure(path, chosen)) { exclude(q, 'terminal_proximity'); continue; }
      const sites = exposedSites(path, chosen);
      if (sites.some(s => s.kind === 'rail_signal')) { exclude(q, 'rail_signal_exposure'); continue; }
      if (onAccepted) {
        q.accepted_intervals++;
        onAccepted({ pair, option: chosen });
        continue;
      }
      const category = categoryFor(sites);
      const hour = Number(hourFormatter.format(new Date(observation.at * 1000)));
      const elapsed = pair.b.observation.at - observation.at;
      const initial: StreetcarBin = { date, corridor: path.path.corridor, route: observation.route,
        direction: path.path.direction, hour, day_type: dayType, category,
        intervals: 0, duration_seconds: 0, distance_meters: 0, slow_seconds: 0, vehicle_ids: [] };
      const key = `${initial.corridor}|${initial.route}|${initial.direction}|${hour}|${category}`;
      add(bins, key, initial, observation, elapsed, chosen.distance);
      for (const site of sites) add(siteBins, `${key}|${site.id}`, { ...initial, intervals: 0, duration_seconds: 0,
        distance_meters: 0, slow_seconds: 0, site_id: site.id }, observation, elapsed, chosen.distance);
      q.accepted_intervals++;
    }
  };
  for (const observations of groups.values()) {
    observations.sort((a, b) => a.at - b.at);
    const points: IndexedPoint[] = [];
    for (const observation of observations) {
      const q = quality.get(index.corridors.get(observation.route)!)!;
      const previous = points.at(-1);
      if (previous?.observation.at === observation.at) {
        exclude(q, 'duplicate_points');
        if (previous.observation.route !== observation.route || previous.observation.trip_id !== observation.trip_id ||
            previous.observation.lat !== observation.lat || previous.observation.lon !== observation.lon ||
            previous.observation.off_route !== observation.off_route) previous.conflict = true;
        continue;
      }
      const point = indexPoint(index, observation);
      if (!point.valid) exclude(q, 'invalid_points');
      points.push(point);
    }
    let run: Pair[] = [];
    for (let n = 1; n < points.length; n++) {
      const a = points[n - 1]; const b = points[n];
      const q = quality.get(index.corridors.get(a.observation.route)!)!;
      q.candidate_intervals++;
      const elapsed = b.observation.at - a.observation.at;
      let reason: string | undefined;
      if (!a.valid || !b.valid) reason = 'invalid_coordinates_or_time';
      else if (a.conflict || b.conflict) reason = 'conflicting_duplicate';
      else if (a.observation.route !== b.observation.route) reason = 'route_change';
      else if (a.observation.trip_id !== b.observation.trip_id) reason = 'trip_change';
      else if (a.observation.off_route || b.observation.off_route) reason = 'off_route';
      else if (elapsed <= 0 || elapsed > METHOD.max_gap_seconds) reason = 'observation_gap';
      else {
        const start = xy(a.observation); const end = xy(b.observation);
        if (Math.hypot(end.x - start.x, end.y - start.y) / elapsed > MAX_SPEED_METERS_SECOND) reason = 'impossible_speed';
      }
      if (reason) { exclude(q, reason); analyzeRun(run); run = []; continue; }
      run.push({ a, b, options: optionsForPair(a, b) });
    }
    analyzeRun(run);
  }
  const finish = <T extends StreetcarBin>(map: Map<string, Accumulator<T>>): T[] => [...map.entries()]
    .sort(([a], [b]) => a.localeCompare(b)).map(([, aggregate]) => {
      aggregate.bin.vehicle_ids = [...aggregate.vehicles].sort();
      return aggregate.bin;
    });
  return { bins: finish(bins), site_bins: finish(siteBins), quality: [...quality.values()] };
}

type TrackPoint = { at: number; position: number };
type Crossing = { estimated: number; lower: number; upper: number };

/** Estimate the first arrival at a fixed track boundary. A pause exactly on a
 * boundary belongs to the following window in the estimate, avoiding doubled dwell.
 * Bounds use the GPS times bracketing the crossing; interpolation supplies an
 * estimate only. A window crossed entirely between two pings can legitimately
 * have a zero lower duration bound, rather than invented timing precision.
 */
function crossing(points: TrackPoint[], boundary: number): Crossing | null {
  const epsilon = 0.000001;
  let low = 0; let high = points.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if (points[mid].position < boundary - epsilon) low = mid + 1;
    else high = mid;
  }
  const after = points[low];
  if (!after) return null;
  const before = points[low - 1];
  // An initial observation on the boundary is left-censored: the vehicle may
  // already have been dwelling there. An exact later GPS hit still brackets
  // arrival between the prior outside observation and this first boundary hit.
  if (Math.abs(after.position - boundary) <= epsilon) {
    return before && before.position < boundary - epsilon
      ? { estimated: after.at, lower: before.at, upper: after.at } : null;
  }
  if (!before || before.position > boundary || after.position <= before.position) return null;
  return { estimated: before.at + (after.at - before.at) * (boundary - before.position) / (after.position - before.position),
    lower: before.at, upper: after.at };
}

/** Headline analysis uses complete, equal-length spatial passages. Classifying
 * whole GPS intervals instead would mechanically select fast observations
 * into feature categories because they cover more ground between updates.
 * The exported interval helper is retained for GPS matching diagnostics/tests;
 * its speed categories must not be used as a comparison baseline.
 */
export function analyzeStreetcars(network: StreetcarNetwork, date: string, observations: StreetcarObservation[]): {
  bins: StreetcarBin[]; site_bins: StreetcarSiteBin[]; quality: StreetcarQuality[]; passages: StreetcarPassage[];
} {
  const index = buildIndex(network);
  const matches: MatchedPair[] = [];
  const { quality } = analyzeStreetcarIntervals(network, date, observations, match => matches.push(match));
  const bins = new Map<string, Accumulator<StreetcarBin>>();
  const siteBins = new Map<string, Accumulator<StreetcarSiteBin>>();
  const passages: StreetcarPassage[] = [];
  const weekday = new Date(`${date}T12:00:00Z`).getUTCDay();
  const dayType = weekday === 0 || weekday === 6 ? 'weekend' : 'weekday';
  const windowMeters = METHOD.window_meters!;
  const add = <T extends StreetcarBin>(map: Map<string, Accumulator<T>>, key: string, initial: T,
    vid: string, seconds: number, lower: number, upper: number) => {
    let aggregate = map.get(key);
    if (!aggregate) { aggregate = { bin: { ...initial }, vehicles: new Set() }; map.set(key, aggregate); }
    aggregate.bin.intervals++;
    aggregate.bin.distance_meters += windowMeters;
    aggregate.bin.duration_seconds += seconds;
    aggregate.bin.duration_lower_seconds = (aggregate.bin.duration_lower_seconds ?? 0) + lower;
    aggregate.bin.duration_upper_seconds = (aggregate.bin.duration_upper_seconds ?? 0) + upper;
    if (windowMeters / seconds < METHOD.slow_mph * MPH_TO_METERS_SECOND) aggregate.bin.slow_seconds += seconds;
    aggregate.vehicles.add(vid);
  };
  const analyzeTrack = (track: MatchedPair[]) => {
    if (!track.length) return;
    const first = track[0]; const indexedPath = index.paths[first.option.path];
    const observation = first.pair.a.observation;
    // A gap, route/trip switch, or rejected match starts a separate contiguous
    // run, even if the same scheduled trip later reappears. Structured input
    // prevents null/string or delimiter collisions in this stable identifier.
    const runId = createHash('sha256').update(JSON.stringify([
      date, observation.vid, observation.route, observation.trip_id, indexedPath.path.id, observation.at,
    ])).digest('hex');
    const points: TrackPoint[] = [{ at: observation.at, position: first.option.from }];
    for (const match of track) points.push({ at: match.pair.b.observation.at,
      position: Math.max(points.at(-1)!.position, match.option.to) });
    const firstBoundary = Math.ceil((points[0].position - 0.000001) / windowMeters) * windowMeters;
    const lastPosition = points.at(-1)!.position;
    for (let from = firstBoundary; from + windowMeters <= lastPosition + 0.000001; from += windowMeters) {
      const option: PairOption = { path: first.option.path, from, to: from + windowMeters, distance: windowMeters, score: 0 };
      if (terminalExposure(indexedPath, option)) continue;
      const sites = exposedSites(indexedPath, option);
      if (sites.some(site => site.kind === 'rail_signal')) continue;
      const entry = crossing(points, from); const exit = crossing(points, from + windowMeters);
      if (!entry || !exit || exit.estimated <= entry.estimated) continue;
      const duration = exit.estimated - entry.estimated;
      // Persist bounds on the reported clock. The API widens these aggregate
      // bounds for minute timestamp truncation exactly once; doing that here
      // too would double the allowance and change historical stored values.
      const lower = Math.max(0, exit.lower - entry.upper);
      const upper = exit.upper - entry.lower;
      const category = categoryFor(sites);
      const hour = Number(hourFormatter.format(new Date(entry.estimated * 1000)));
      const initial: StreetcarBin = { date, corridor: indexedPath.path.corridor, route: observation.route,
        direction: indexedPath.path.direction, hour, day_type: dayType, category,
        intervals: 0, duration_seconds: 0, duration_lower_seconds: 0, duration_upper_seconds: 0,
        distance_meters: 0, slow_seconds: 0, vehicle_ids: [] };
      const key = `${initial.corridor}|${initial.route}|${initial.direction}|${hour}|${category}`;
      add(bins, key, initial, observation.vid, duration, lower, upper);
      for (const site of sites) add(siteBins, `${key}|${site.id}`, { ...initial, site_id: site.id },
        observation.vid, duration, lower, upper);
      passages.push({ date, corridor: indexedPath.path.corridor, route: observation.route,
        direction: indexedPath.path.direction, path_id: indexedPath.path.id,
        window_id: `${indexedPath.path.id}:${from}`, from_meters: from, to_meters: from + windowMeters,
        run_id: runId, vid: observation.vid, trip_id: observation.trip_id,
        entry_at: entry.estimated, exit_at: exit.estimated, hour, day_type: dayType, category,
        signal_ids: sites.filter(site => site.kind === 'signal').map(site => site.id).sort(),
        stop_ids: sites.filter(site => site.kind === 'stop').map(site => site.id).sort(),
        duration_seconds: duration, duration_lower_seconds: lower, duration_upper_seconds: upper });
    }
  };
  let track: MatchedPair[] = [];
  for (const match of matches) {
    const previous = track.at(-1);
    if (previous && (previous.pair.b !== match.pair.a || previous.option.path !== match.option.path)) {
      analyzeTrack(track); track = [];
    }
    track.push(match);
  }
  analyzeTrack(track);
  const finish = <T extends StreetcarBin>(map: Map<string, Accumulator<T>>): T[] => [...map.entries()]
    .sort(([a], [b]) => a.localeCompare(b)).map(([, aggregate]) => ({ ...aggregate.bin, vehicle_ids: [...aggregate.vehicles].sort() }));
  passages.sort((a, b) => a.entry_at - b.entry_at || a.run_id.localeCompare(b.run_id) || a.from_meters - b.from_meters);
  return { bins: finish(bins), site_bins: finish(siteBins), quality, passages };
}
