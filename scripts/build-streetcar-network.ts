/** Build the streetcar analysis catalog offline from the saved OSM and GTFS inputs. */
import { createHash } from 'node:crypto';
import { readFile, writeFile, mkdir, stat } from 'node:fs/promises';
import { dirname } from 'node:path';
import { parseArgs } from 'node:util';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import type { GeoPoint, StreetcarNetwork, StreetcarPath, StreetcarSite } from '../dashboard/src/streetcar-data.js';

type Row = Record<string, string>;
interface OsmNode extends GeoPoint { id: number; version?: number; tags: Record<string, string> }
interface OsmWay { id: number; nodes: number[]; tags?: Record<string, string> }
interface OsmInput {
  fetched_at: string; osm_timestamp: string; source_url: string; attribution: string;
  license: string; license_url: string; elements: OsmNode[]; context_ways: OsmWay[];
}
const CORRIDORS: StreetcarNetwork['corridors'] = [
  { id: 'st_charles', name: 'St. Charles', routes: ['12'] },
  { id: 'canal', name: 'Canal', routes: ['47', '48'] },
  { id: 'rampart', name: 'Rampart–Loyola', routes: ['46'] },
];
const CORRIDOR_DISTANCE_METERS = 50;
const CLUSTER_DIAMETER_METERS = 30;
const METERS_PER_DEGREE = 6371000 * Math.PI / 180;
const LONGITUDE_SCALE = METERS_PER_DEGREE * Math.cos(29.95 * Math.PI / 180);
const EXCLUDED_HIGHWAYS = new Set(['footway', 'path', 'pedestrian', 'cycleway', 'steps', 'bridleway', 'corridor', 'platform']);

function distance(a: GeoPoint, b: GeoPoint): number {
  return Math.hypot((b.lon - a.lon) * LONGITUDE_SCALE, (b.lat - a.lat) * METERS_PER_DEGREE);
}
function segmentDistance(p: GeoPoint, a: GeoPoint, b: GeoPoint): number {
  const dx = (b.lon - a.lon) * LONGITUDE_SCALE, dy = (b.lat - a.lat) * METERS_PER_DEGREE;
  const px = (p.lon - a.lon) * LONGITUDE_SCALE, py = (p.lat - a.lat) * METERS_PER_DEGREE;
  const t = Math.max(0, Math.min(1, (px * dx + py * dy) / (dx * dx + dy * dy || 1)));
  return Math.hypot(px - t * dx, py - t * dy);
}
function routeDistance(p: GeoPoint, paths: StreetcarPath[]): number {
  let nearest = Infinity;
  for (const path of paths) for (let i = 1; i < path.points.length; i++) {
    nearest = Math.min(nearest, segmentDistance(p, path.points[i - 1], path.points[i]));
  }
  return nearest;
}
function coordinate(lat: string | number, lon: string | number): GeoPoint {
  const point = { lat: Number(lat), lon: Number(lon) };
  if (lat === '' || lon === '' || !Number.isFinite(point.lat) || !Number.isFinite(point.lon) || Math.abs(point.lat) > 90 || Math.abs(point.lon) > 180) {
    throw new Error('Invalid source coordinate');
  }
  return point;
}
function mode(values: string[]): string {
  const counts = new Map<string, number>();
  for (const value of values) counts.set(value, (counts.get(value) ?? 0) + 1);
  return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0][0];
}
function clusterNodes(nodes: OsmNode[]): OsmNode[][] {
  // Complete-link membership bounds the diameter. A chain of closely spaced
  // approaches cannot merge a whole corridor into one signal site.
  const groups: OsmNode[][] = [];
  for (const node of [...nodes].sort((a, b) => a.id - b.id)) {
    const eligible = groups.filter(group => group.every(other => distance(node, other) <= CLUSTER_DIAMETER_METERS));
    const group = eligible.sort((a, b) => Math.min(...a.map(p => distance(p, node))) - Math.min(...b.map(p => distance(p, node))))[0];
    if (group) group.push(node); else groups.push([node]);
  }
  return groups;
}

const { values } = parseArgs({ options: {
  gtfs: { type: 'string', default: '/tmp/norta-gtfs.zip' },
  osm: { type: 'string', default: 'src/data/streetcar-signals-osm.json' },
  output: { type: 'string', default: 'src/data/streetcar-network.json' },
  'gtfs-fetched-at': { type: 'string' },
  mapillary: { type: 'string', default: 'src/data/streetcar-mapillary.json' },
  reviews: { type: 'string', default: 'src/data/streetcar-mapillary-reviews.json' },
}, strict: true });
const [gtfsBytes, osmBytes, gtfsStat] = await Promise.all([readFile(values.gtfs!), readFile(values.osm!), stat(values.gtfs!)]);
const files = unzipSync(gtfsBytes);
function rows(name: string): Row[] {
  if (!files[name]) throw new Error(`Missing GTFS ${name}`);
  return parse(strFromU8(files[name]), { columns: true, bom: true, skip_empty_lines: true }) as Row[];
}
const osm = JSON.parse(osmBytes.toString()) as OsmInput;
if (!osm.elements.length || !osm.context_ways.length || !osm.fetched_at || !osm.license_url) throw new Error('Incomplete OSM snapshot');
const routeRows = new Map(rows('routes.txt').map(row => [row.route_id, row]));
const routeCorridors = new Map(CORRIDORS.flatMap(c => c.routes.map(route => [route, c.id] as const)));
for (const route of routeCorridors.keys()) if (routeRows.get(route)?.route_type !== '0') throw new Error(`Route ${route} is missing or not a streetcar`);
const allStreetcarTrips = rows('trips.txt').filter(row => routeCorridors.has(row.route_id));
const trips = allStreetcarTrips.filter(row => row.trip_headsign.trim().toLowerCase() !== 'not in service');
const excludedNonPassengerTrips = allStreetcarTrips.length - trips.length;
const tripIds = new Set(trips.map(row => row.trip_id));
const stops = new Map(rows('stops.txt').map(row => [row.stop_id, row]));
const tripStops = new Map<string, Row[]>();
for (const row of rows('stop_times.txt')) if (tripIds.has(row.trip_id)) {
  const group = tripStops.get(row.trip_id) ?? []; group.push(row); tripStops.set(row.trip_id, group);
}
for (const group of tripStops.values()) group.sort((a, b) => Number(a.stop_sequence) - Number(b.stop_sequence));
const shapeIds = new Set(trips.map(row => row.shape_id));
const shapeRows = new Map<string, Row[]>();
for (const row of rows('shapes.txt')) if (shapeIds.has(row.shape_id)) {
  const group = shapeRows.get(row.shape_id) ?? []; group.push(row); shapeRows.set(row.shape_id, group);
}
const shapePoints = new Map<string, GeoPoint[]>();
for (const [id, group] of shapeRows) {
  const points = group.sort((a, b) => Number(a.shape_pt_sequence) - Number(b.shape_pt_sequence)).map(row => coordinate(row.shape_pt_lat, row.shape_pt_lon));
  shapePoints.set(id, points.filter((p, i) => i === 0 || p.lat !== points[i - 1].lat || p.lon !== points[i - 1].lon));
}
const pathTrips = new Map<string, Row[]>();
for (const trip of trips) {
  const key = `${trip.shape_id}|${trip.route_id}|${trip.direction_id}`;
  const group = pathTrips.get(key) ?? []; group.push(trip); pathTrips.set(key, group);
}
const paths: StreetcarPath[] = [], sites = new Map<string, StreetcarSite>();
for (const [id, group] of [...pathTrips].sort(([a], [b]) => a.localeCompare(b))) {
  const trip = group[0], corridor = routeCorridors.get(trip.route_id)!;
  const ordered = tripStops.get(trip.trip_id);
  const points = shapePoints.get(trip.shape_id);
  if (!ordered?.length || !points || points.length < 2) throw new Error(`Incomplete trip/shape ${id}`);
  const stopIds = ordered.map(row => row.stop_id);
  // Fail visibly if a later GTFS reuses this shape for different stopping
  // patterns; silently dropping a variant would lose passenger-stop exposure.
  if (group.some(t => JSON.stringify(tripStops.get(t.trip_id)?.map(row => row.stop_id)) !== JSON.stringify(stopIds))) {
    throw new Error(`Shape ${id} has multiple stopping patterns; represent those variants before rebuilding`);
  }
  stopIds.forEach(stopId => {
    const siteId = `${corridor}:stop:${stopId}`;
    const stop = stops.get(stopId);
    if (!stop) throw new Error(`Missing GTFS stop ${stopId}`);
    const site = sites.get(siteId) ?? { id: siteId, corridor, kind: 'stop', name: stop.stop_name,
      ...coordinate(stop.stop_lat, stop.stop_lon), routes: [], source_ids: [`gtfs:stop:${stopId}`], verification: 'gtfs' };
    if (!site.routes.includes(trip.route_id)) site.routes.push(trip.route_id);
    sites.set(siteId, site);
  });
  paths.push({ id, corridor, route: trip.route_id, direction: trip.direction_id,
    headsign: mode(group.map(t => t.trip_headsign)), points, stop_ids: stopIds });
}
const nodeWays = new Map<number, OsmWay[]>();
for (const way of osm.context_ways) for (const nodeId of way.nodes) {
  const group = nodeWays.get(nodeId) ?? []; group.push(way); nodeWays.set(nodeId, group);
}
const roadWays = (node: OsmNode): OsmWay[] => (nodeWays.get(node.id) ?? []).filter(way => {
  const highway = way.tags?.highway;
  return highway && !EXCLUDED_HIGHWAYS.has(highway);
});
const signalNodes = osm.elements.filter(node => node.tags.highway === 'traffic_signals' && roadWays(node).length > 0);
const railNodes = osm.elements.filter(node => node.tags.railway === 'signal');
let largestCluster = 0;
const clusterDiameters: number[] = [];
for (const corridor of CORRIDORS) {
  const routePaths = new Map(corridor.routes.map(route => [route, paths.filter(path => path.route === route)]));
  const matchedRoutes = (node: GeoPoint): string[] => corridor.routes.filter(route => routeDistance(node, routePaths.get(route)!) <= CORRIDOR_DISTANCE_METERS);
  const nearRoad = signalNodes.filter(node => matchedRoutes(node).length > 0);
  const nearRail = railNodes.filter(node => matchedRoutes(node).length > 0);
  for (const [kind, groups] of [['signal', clusterNodes(nearRoad)], ['rail_signal', nearRail.map(node => [node])]] as const) {
    for (const group of groups) {
      const id = `${corridor.id}:${kind}:osm-${Math.min(...group.map(node => node.id))}`;
      const names = [...new Set(group.flatMap(node => (kind === 'signal' ? roadWays(node) : nodeWays.get(node.id) ?? [])
        .map(way => way.tags?.name).filter((name): name is string => Boolean(name))))].sort();
      const point = { lat: group.reduce((sum, p) => sum + p.lat, 0) / group.length, lon: group.reduce((sum, p) => sum + p.lon, 0) / group.length };
      const nearbyStop = [...sites.values()].filter(site => site.corridor === corridor.id && site.kind === 'stop')
        .sort((a, b) => distance(a, point) - distance(b, point))[0];
      const name = names.length ? names.join(' / ') : nearbyStop && distance(nearbyStop, point) < 100 ? `Near ${nearbyStop.name}` : `${corridor.name} ${kind === 'signal' ? 'traffic signal' : 'rail signal'} · ${group[0].id}`;
      sites.set(id, { id, corridor: corridor.id, kind, name, ...point,
        routes: [...new Set(group.flatMap(matchedRoutes))].sort(),
        source_ids: group.map(node => `osm/node/${node.id}`).sort(), verification: 'osm_unverified' });
      if (kind === 'signal') {
        largestCluster = Math.max(largestCluster, group.length);
        clusterDiameters.push(Math.max(0, ...group.flatMap(a => group.map(b => distance(a, b)))));
      }
    }
  }
}
interface ReviewInput { exclusions?: Array<{ site_id: string; source_ids: string[]; osm_node_versions: Record<string, number>; reason: string }>; }
let reviews: ReviewInput = {};
try { reviews = JSON.parse(await readFile(values.reviews!, 'utf8')) as ReviewInput; }
catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
const sourceNodes = new Map(osm.elements.map(node => [node.id, node]));
const appliedExclusions: string[] = [];
for (const exclusion of reviews.exclusions ?? []) {
  const site = sites.get(exclusion.site_id);
  if (site?.kind === 'signal' && site.source_ids.length === exclusion.source_ids.length &&
      exclusion.source_ids.every(id => site.source_ids.includes(id)) &&
      Object.entries(exclusion.osm_node_versions).every(([id, version]) => sourceNodes.get(Number(id))?.version === version)) {
    sites.delete(exclusion.site_id); appliedExclusions.push(exclusion.site_id);
  }
}
const scheduleHash = createHash('sha256').update(gtfsBytes).digest('hex');
const identity = createHash('sha256').update(scheduleHash).update(osmBytes).update(`v1|passenger-only|${CORRIDOR_DISTANCE_METERS}|${CLUSTER_DIAMETER_METERS}`).digest('hex');
interface MapillaryCheck { fetched_at: string; source_url: string; attribution: string; status: string; network_version: string; license?: string; license_url?: string;
  site_checks: Array<{ site_id: string; matches: Array<{ feature_id: string; distance_meters: number }>; imagery_verified: boolean }>;
  imagery_checks?: Array<{ site_id: string; image_id: string; outcome: string }>; }
let mapillary: MapillaryCheck | undefined;
try { mapillary = JSON.parse(await readFile(values.mapillary!, 'utf8')) as MapillaryCheck; }
catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
const baseVersion = `streetcar-network-v1-${identity.slice(0, 12)}`;
const version = appliedExclusions.length ? `${baseVersion}-review-${createHash('sha256').update(appliedExclusions.sort().join('|')).digest('hex').slice(0, 8)}` : baseVersion;
const staleMapillary = Boolean(mapillary && mapillary.network_version.split('-review-')[0] !== baseVersion);
if (staleMapillary) mapillary = undefined;
const retainedChecks = mapillary?.site_checks.filter(check => sites.has(check.site_id)) ?? [];
const nearestMapillaryFeatures = new Map(retainedChecks.filter(check => check.matches.length).map(check =>
  [check.site_id, [...check.matches].sort((a, b) => a.distance_meters - b.distance_meters || a.feature_id.localeCompare(b.feature_id))[0].feature_id]));
const corroborated = nearestMapillaryFeatures.size;
const visuallyVerified = new Set(retainedChecks.filter(check => check.imagery_verified).map(check => check.site_id));
const network: StreetcarNetwork = {
  version,
  generated_at: new Date().toISOString(), schedule_hash: scheduleHash,
  corridors: CORRIDORS, paths,
  sites: [...sites.values()].map(site => ({ ...site, routes: site.routes.sort(),
    source_ids: [...site.source_ids, ...(nearestMapillaryFeatures.has(site.id) ? [`mapillary/feature/${nearestMapillaryFeatures.get(site.id)}`] : []), ...(visuallyVerified.has(site.id) ? mapillary?.imagery_checks?.filter(check => check.site_id === site.id && check.outcome === 'signal_present_at_capture').map(check => `mapillary/image/${check.image_id}`) ?? [] : [])],
    verification: nearestMapillaryFeatures.has(site.id) || visuallyVerified.has(site.id) ? 'mapillary' as const : site.verification })).sort((a, b) => a.id.localeCompare(b.id)),
  sources: [
    { name: 'OpenStreetMap signal snapshot', url: osm.source_url, fetched_at: osm.fetched_at, attribution: osm.attribution },
    { name: `OpenStreetMap license (${osm.license})`, url: osm.license_url, fetched_at: osm.fetched_at, attribution: osm.attribution },
    { name: 'NORTA GTFS schedule (local archive file timestamp)', url: 'https://www.norta.com/RTA/media/GTFS/GTFS.zip',
      fetched_at: values['gtfs-fetched-at'] ?? gtfsStat.mtime.toISOString(), attribution: 'New Orleans Regional Transit Authority; public GTFS feed' },
    ...(mapillary ? [{ name: 'Mapillary traffic-light feature crosscheck', url: mapillary.source_url,
      fetched_at: mapillary.fetched_at, attribution: mapillary.attribution },
      ...(mapillary.license_url ? [{ name: `Mapillary derived-data license (${mapillary.license ?? 'ODbL'})`, url: mapillary.license_url,
        fetched_at: mapillary.fetched_at, attribution: mapillary.attribution }] : [])] : []),
  ],
  mapillary_status: mapillary
    ? `${corroborated} of ${retainedChecks.length} retained OSM road-signal sites have nearby Mapillary vehicle-light detections; ${visuallyVerified.size} sites have reviewed dated imagery. ${appliedExclusions.length} disputed OSM candidate excluded locally after imagery review; current presence unverified. ${mapillary.status === 'complete' ? 'All requested tiles fetched.' : 'Some tile requests failed.'} Nearby detections support presence, not which streetcar movement a light controls. Missing detections do not establish absence.`
    : staleMapillary ? 'The Mapillary crosscheck refers to an older network catalog. Refresh it before applying verification to these sites.'
      : 'Not verified with Mapillary. OSM signals are mapped candidates; a Mapillary access token is required for imagery and detection crosschecks.',
};
const siteById = new Map(network.sites.map(site => [site.id, site]));
for (const path of paths) for (const stopId of path.stop_ids) {
  const siteId = `${path.corridor}:stop:${stopId}`;
  const site = siteById.get(siteId);
  if (site?.kind !== 'stop' || site.corridor !== path.corridor || !site.routes.includes(path.route)) throw new Error(`Invalid stop reference ${path.id}: ${siteId}`);
}
if (siteById.size !== network.sites.length || Math.max(...clusterDiameters) > CLUSTER_DIAMETER_METERS + 1e-6) throw new Error('Invalid catalog identity or signal cluster diameter');
await mkdir(dirname(values.output!), { recursive: true });
await writeFile(values.output!, JSON.stringify(network, null, 2) + '\n');
console.log(JSON.stringify({ output: values.output, version: network.version, paths: paths.length,
  excluded_disputed_osm_sites: appliedExclusions,
  excluded_non_passenger_trips: excludedNonPassengerTrips,
  excluded_non_passenger_shapes: new Set(allStreetcarTrips.map(t => t.shape_id)).size - shapeIds.size,
  shape_points: paths.reduce((sum, path) => sum + path.points.length, 0), sites: network.sites.length,
  max_signal_cluster_nodes: largestCluster, max_signal_cluster_diameter_m: Math.max(...clusterDiameters),
  corridors: CORRIDORS.map(c => ({ id: c.id, stops: network.sites.filter(s => s.corridor === c.id && s.kind === 'stop').length,
    signals: network.sites.filter(s => s.corridor === c.id && s.kind === 'signal').length,
    rail_signals: network.sites.filter(s => s.corridor === c.id && s.kind === 'rail_signal').length })) }, null, 2));
