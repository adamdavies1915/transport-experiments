import { createHash } from 'node:crypto';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import type { StreetcarNetwork } from '../dashboard/src/streetcar-data';
import type { StudyCatalog, StudyPath, StudyPoint, StudyRowSection, StudySite } from './transit-study-types';
import { distance, pathLength, projectSegment, METERS_PER_DEGREE, LONGITUDE_SCALE } from './transit-study-geometry';

export const CATALOG_LIMITATIONS = [
  'ROW is unknown unless an evidence-backed review explicitly covers the path interval and observation date. Legacy latitude/longitude boxes are not reviewed ROW evidence.',
  'A nearby mapped road signal is not proof that it controls this transit approach. Imagery corroborates hardware presence at capture time, not live phases.',
  'The saved OSM extract has a geographic boundary. Missing mapped features outside or inside it do not establish absence.',
  'GTFS passenger patterns supply paths and stops. Bus congestion measurements from an independent road-traffic feed are deferred to version 2.',
];
export function catalogFromStreetcarNetwork(network: StreetcarNetwork): StudyCatalog {
  const paths: StudyPath[] = network.paths.map(p => ({ id: p.id, route_id: p.route, direction_id: p.direction,
    mode: 'streetcar', name: p.headsign, points: p.points, stop_ids: p.stop_ids }));
  return { version: `transit-study:${network.version}`, generated_at: network.generated_at, schedule_hash: network.schedule_hash,
    paths, sites: network.sites.map(s => ({ id: s.id, kind: s.kind, name: s.name, lat: s.lat, lon: s.lon,
      route_ids: s.routes, source_ids: s.source_ids, verification: s.verification })),
    row_sections: paths.map(unknownRow), sources: network.sources, limitations: [...CATALOG_LIMITATIONS] };
}
function unknownRow(path: StudyPath): StudyRowSection {
  return { id: `${path.id}:row:unknown`, path_id: path.id, from_meters: 0, to_meters: pathLength(path.points),
    row_class: 'unknown', reviewed_at: null, evidence_urls: [], valid_from: null, valid_to: null,
    notes: 'No reviewed path-distance right-of-way classification has been supplied.' };
}
type CsvRow = Record<string, string>;
interface OsmNode extends StudyPoint { id: number; tags: Record<string, string> }
export interface StudyOsmInput {
  elements: OsmNode[]; context_ways: Array<{ id: number; nodes: number[]; tags?: Record<string, string> }>;
  fetched_at: string; source_url: string; attribution: string;
}
export interface CatalogOptions {
  streetcar_network?: StreetcarNetwork; row_sections?: StudyRowSection[]; generated_at?: string;
  /** Previously reviewed exclusions must survive a broader bus catalog rebuild. */
  excluded_osm_node_ids?: number[];
}
/** Offline, deterministic geometry construction. No network or credential access. */
export function buildTransitStudyCatalog(gtfs: Uint8Array, osm: StudyOsmInput, options: CatalogOptions = {}): StudyCatalog {
  const files = unzipSync(gtfs);
  const csv = (name: string): CsvRow[] => {
    if (!files[name]) throw new Error(`Missing GTFS ${name}`);
    return parse(strFromU8(files[name]), { columns: true, bom: true, skip_empty_lines: true }) as CsvRow[];
  };
  const routes = new Map(csv('routes.txt').filter(r => ['0', '3'].includes(r.route_type)).map(r => [r.route_id, r]));
  const trips = csv('trips.txt').filter(t => routes.has(t.route_id) && !/not\s+in\s+service/i.test(t.trip_headsign));
  const tripIds = new Set(trips.map(t => t.trip_id)), stopRows = new Map(csv('stops.txt').map(s => [s.stop_id, s]));
  const tripStops = new Map<string, CsvRow[]>(), shapes = new Map<string, CsvRow[]>();
  for (const s of csv('stop_times.txt')) if (tripIds.has(s.trip_id)) {
    const list = tripStops.get(s.trip_id) ?? []; list.push(s); tripStops.set(s.trip_id, list);
  }
  for (const list of tripStops.values()) list.sort((a, b) => Number(a.stop_sequence) - Number(b.stop_sequence));
  for (const s of csv('shapes.txt')) { const list = shapes.get(s.shape_id) ?? []; list.push(s); shapes.set(s.shape_id, list); }
  const coordinate = (lat: string, lon: string): StudyPoint => {
    const p = { lat: Number(lat), lon: Number(lon) };
    if (!lat || !lon || !Number.isFinite(p.lat) || !Number.isFinite(p.lon) || Math.abs(p.lat) > 90 || Math.abs(p.lon) > 180)
      throw new Error('Invalid GTFS coordinate');
    return p;
  };
  const points = new Map([...shapes].map(([id, rows]) => [id, rows.sort((a, b) => Number(a.shape_pt_sequence) - Number(b.shape_pt_sequence))
    .map(s => coordinate(s.shape_pt_lat, s.shape_pt_lon)).filter((p, i, a) => !i || distance(p, a[i - 1]) > 0.01)]));
  const base = options.streetcar_network ? catalogFromStreetcarNetwork(options.streetcar_network) : null;
  const paths = [...(base?.paths ?? [])], sites = new Map((base?.sites ?? []).map(s => [s.id, { ...s }]));
  const preservedRoutes = new Set(paths.map(p => p.route_id));
  const patterns = new Map<string, { trip: CsvRow; stops: string[] }>();
  for (const trip of trips) {
    if (preservedRoutes.has(trip.route_id)) continue;
    const stops = tripStops.get(trip.trip_id)?.map(s => s.stop_id);
    if (!stops?.length || !points.has(trip.shape_id)) continue;
    const suffix = createHash('sha256').update(JSON.stringify(stops)).digest('hex').slice(0, 8);
    // Preserve different boarding patterns even when GTFS reuses a shape.
    const id = `${trip.shape_id}|${trip.route_id}|${trip.direction_id}|${suffix}`;
    if (!patterns.has(id)) patterns.set(id, { trip, stops });
  }
  for (const [id, { trip, stops }] of [...patterns].sort(([a], [b]) => a.localeCompare(b))) {
    const p = points.get(trip.shape_id)!;
    if (p.length < 2) continue;
    paths.push({ id, route_id: trip.route_id, direction_id: trip.direction_id, mode: routes.get(trip.route_id)!.route_type === '3' ? 'bus' : 'streetcar',
      name: trip.trip_headsign || routes.get(trip.route_id)!.route_long_name, points: p, stop_ids: stops });
    for (const stopId of stops) {
      const id = `gtfs:stop:${stopId}`, row = stopRows.get(stopId);
      if (!row) throw new Error(`Unknown GTFS stop ${stopId}`);
      const s = sites.get(id) ?? { id, kind: 'stop' as const, name: row.stop_name, ...coordinate(row.stop_lat, row.stop_lon),
        route_ids: [], source_ids: [id], verification: 'gtfs' as const };
      if (!s.route_ids.includes(trip.route_id)) s.route_ids.push(trip.route_id);
      sites.set(id, s);
    }
  }
  // Spatial index avoids scanning all bus paths for every OSM node.
  const grid = new Map<string, Array<{ path: StudyPath; a: StudyPoint; b: StudyPoint }>>();
  const cell = (p: StudyPoint) => [Math.floor(p.lon * LONGITUDE_SCALE / 100), Math.floor(p.lat * METERS_PER_DEGREE / 100)];
  for (const path of paths.filter(p => !preservedRoutes.has(p.route_id))) for (let i = 1; i < path.points.length; i++) {
    const a = path.points[i - 1], b = path.points[i]; const [ax, ay] = cell(a), [bx, by] = cell(b);
    for (let x = Math.min(ax, bx) - 1; x <= Math.max(ax, bx) + 1; x++) for (let y = Math.min(ay, by) - 1; y <= Math.max(ay, by) + 1; y++) {
      const key = `${x}:${y}`, entries = grid.get(key) ?? []; entries.push({ path, a, b }); grid.set(key, entries);
    }
  }
  const roadNodes = new Set<number>();
  for (const way of osm.context_ways) if (way.tags?.highway && !['footway', 'path', 'pedestrian', 'cycleway', 'steps', 'platform'].includes(way.tags.highway))
    for (const id of way.nodes) roadNodes.add(id);
  const excluded = new Set(options.excluded_osm_node_ids ?? []);
  const candidates = osm.elements.filter(n => !excluded.has(n.id) &&
    (n.tags.railway === 'signal' || n.tags.highway === 'traffic_signals' && roadNodes.has(n.id)));
  for (const n of candidates) {
    const key = cell(n).join(':'), entries = grid.get(key) ?? [];
    const near = entries.filter(e => projectSegment(n, e.a, e.b).distance <= 40);
    if (!near.length) continue;
    const kind = n.tags.railway === 'signal' ? 'rail_signal' : 'signal';
    const sourceId = `osm/node/${n.id}`;
    const existing = [...sites.values()].find(s => s.kind === kind && s.source_ids.includes(sourceId));
    if (existing) existing.route_ids = [...new Set([...existing.route_ids, ...near.map(e => e.path.route_id)])].sort();
    else sites.set(`${kind}:${sourceId}`, { id: `${kind}:${sourceId}`, kind, name: n.tags.name || `Mapped ${kind.replace('_', ' ')} ${n.id}`,
      lat: n.lat, lon: n.lon, route_ids: [...new Set(near.map(e => e.path.route_id))].sort(), source_ids: [sourceId], verification: 'osm_unverified' });
  }
  const reviewedSections = options.row_sections ?? [];
  const knownPaths = new Map(paths.map(p => [p.id, p]));
  const isoDate = (value: string) => /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value;
  for (const section of reviewedSections) {
    const path = knownPaths.get(section.path_id);
    if (!path || !Number.isFinite(section.from_meters) || !Number.isFinite(section.to_meters) || section.from_meters < 0 ||
        section.to_meters <= section.from_meters || section.to_meters > pathLength(path.points) + 1)
      throw new Error(`Invalid reviewed ROW interval ${section.id}`);
    if (!['reserved', 'shared', 'unknown'].includes(section.row_class) || section.valid_from && !isoDate(section.valid_from) ||
        section.valid_to && (!isoDate(section.valid_to) || section.valid_from && section.valid_to < section.valid_from))
      throw new Error(`Invalid ROW classification or validity dates ${section.id}`);
    if (section.row_class !== 'unknown' && (!section.reviewed_at || !Number.isFinite(Date.parse(section.reviewed_at)) || !section.valid_from || !section.evidence_urls.length))
      throw new Error(`ROW interval ${section.id} needs dated review, evidence and explicit historical applicability`);
  }
  // Map every path, including gaps between reviewed sections. No overlapping gray
  // full-route overlay may hide a reviewed section in the dashboard.
  const sections: StudyRowSection[] = [];
  for (const path of paths) {
    const intervals = reviewedSections.filter(s => s.path_id === path.id).sort((a, b) => a.from_meters - b.from_meters);
    let at = 0;
    const unknown = (from: number, to: number) => ({ ...unknownRow(path), id: `${path.id}:row:unknown:${from.toFixed(3)}`, from_meters: from, to_meters: to });
    for (const section of intervals) {
      if (section.from_meters < at) throw new Error(`Overlapping reviewed ROW intervals on ${path.id}`);
      if (section.from_meters > at) sections.push(unknown(at, section.from_meters));
      sections.push(section); at = section.to_meters;
    }
    if (at < pathLength(path.points)) sections.push(unknown(at, pathLength(path.points)));
  }
  const scheduleHash = createHash('sha256').update(gtfs).digest('hex');
  const identity = createHash('sha256').update(JSON.stringify({ scheduleHash, paths, sites: [...sites.values()], sections })).digest('hex').slice(0, 16);
  return { version: `transit-study-network-v1-${identity}`, generated_at: options.generated_at ?? new Date().toISOString(),
    schedule_hash: scheduleHash, paths, sites: [...sites.values()].sort((a, b) => a.id.localeCompare(b.id)), row_sections: sections,
    sources: [...(base?.sources ?? []), { name: 'Saved OSM signal extract', url: osm.source_url, fetched_at: osm.fetched_at, attribution: osm.attribution }],
    limitations: [...CATALOG_LIMITATIONS] };
}
