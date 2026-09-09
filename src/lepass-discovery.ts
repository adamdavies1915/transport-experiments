import { createHash } from 'node:crypto';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import type { LePassQuery } from './lepass-collector.js';
import { numeric, stringId, struct, type ThriftStruct } from './lepass-thrift.js';

export interface LePassMembership { routeId: string; lineId: number; patternId: number; groupId: number }
export interface LePassCatalogInput {
  gtfsZip: Uint8Array; entities: ThriftStruct[]; lineEntities: ThriftStruct[];
  memberships: LePassMembership[]; metroRevision: string; fetchedAt: string;
}
interface Stop { id: string; code: string; lat: number; lon: number }
function metres(a: Stop, b: Stop): number {
  const radians = Math.PI / 180, lat = (a.lat + b.lat) * radians / 2;
  return Math.hypot((a.lat - b.lat) * 111_195, (a.lon - b.lon) * 111_195 * Math.cos(lat));
}
function orderedMatch(provider: Array<string | null>, schedule: string[]): number {
  let cursor = 0, matches = 0;
  for (const stop of provider) {
    if (!stop) continue;
    const at = schedule.indexOf(stop, cursor);
    if (at >= 0) { matches++; cursor = at + 1; }
  }
  return matches / Math.max(provider.length, schedule.length);
}

/** Route identity needs an RTA agency match; direction needs ordered, located stop evidence. */
export function buildLePassQueryCatalog(input: LePassCatalogInput): { queries: LePassQuery[]; metadata: Record<string, unknown> } {
  const zip = unzipSync(input.gtfsZip);
  function table(name: string): Record<string, string>[] {
    if (!zip[name]) throw new Error(`GTFS is missing ${name}`);
    return parse(strFromU8(zip[name]), { columns: true, bom: true, skip_empty_lines: true });
  }
  const routes = new Map(table('routes.txt').filter(r => ['0', '3'].includes(r.route_type)).map(r => [r.route_id, r]));
  const gtfsStops = table('stops.txt').map(s => ({ id: s.stop_id, code: s.stop_code ?? '', lat: Number(s.stop_lat), lon: Number(s.stop_lon) }));
  const codeStops = new Map<string, Stop[]>();
  for (const stop of gtfsStops) { const values = codeStops.get(stop.code) ?? []; values.push(stop); codeStops.set(stop.code, values); }
  const trips = new Map(table('trips.txt').map(t => [t.trip_id, t]));
  const stopTimes = new Map<string, Array<{ stop: string; sequence: number }>>();
  for (const row of table('stop_times.txt')) {
    const list = stopTimes.get(row.trip_id) ?? []; list.push({ stop: row.stop_id, sequence: Number(row.stop_sequence) }); stopTimes.set(row.trip_id, list);
  }
  const sequences = new Map<string, Array<{ direction: string; stops: string[] }>>(); const seen = new Set<string>();
  for (const [id, stops] of stopTimes) {
    const trip = trips.get(id); if (!trip || !routes.has(trip.route_id) || !['0', '1'].includes(trip.direction_id)) continue;
    const sequence = stops.sort((a, b) => a.sequence - b.sequence).map(s => s.stop);
    const key = `${trip.route_id}:${trip.direction_id}:${sequence.join(',')}`;
    if (seen.has(key)) continue; seen.add(key);
    const list = sequences.get(trip.route_id) ?? []; list.push({ direction: trip.direction_id, stops: sequence }); sequences.set(trip.route_id, list);
  }
  const patterns = new Map<number, ThriftStruct>(), providerStops = new Map<number, ThriftStruct>();
  for (const entity of input.entities) {
    const payload = struct(entity[1]); if (!payload) continue;
    const pattern = struct(payload[9]), stop = struct(payload[5]);
    if (pattern && numeric(pattern[1]) !== null) patterns.set(numeric(pattern[1])!, pattern);
    if (stop && numeric(stop[1]) !== null) providerStops.set(numeric(stop[1])!, stop);
  }
  const lines = new Map<number, { groupId: number; route: string; agency: number; destination: string }>();
  for (const entity of input.lineEntities) {
    const group = struct(struct(entity[1])?.[8]); if (!group || !Array.isArray(group[6])) continue;
    for (const candidate of group[6]) {
      const line = struct(candidate); if (!line || numeric(line[1]) === null) continue;
      lines.set(numeric(line[1])!, { groupId: numeric(group[1])!, route: stringId(group[2]) ?? '', agency: numeric(group[3])!, destination: stringId(line[3]) ?? '' });
    }
  }
  const matchedStops = new Map<number, Stop | null>();
  for (const [id, value] of providerStops) {
    const point = struct(value[3]), latitude = numeric(point?.[1]), longitude = numeric(point?.[2]);
    if (latitude === null || longitude === null) { matchedStops.set(id, null); continue; }
    const stop = { id: String(id), code: stringId(value[4]) ?? '', lat: latitude / 1e6, lon: longitude / 1e6 };
    const candidates = (codeStops.get(stop.code) ?? []).map(s => ({ stop: s, distance: metres(stop, s) })).filter(s => s.distance <= 80).sort((a, b) => a.distance - b.distance);
    matchedStops.set(id, candidates.length === 1 || (candidates.length > 1 && candidates[1].distance - candidates[0].distance > 30) ? candidates[0].stop : null);
  }
  const wireQueries = new Map<string, LePassQuery>(), diagnostics: Record<string, unknown>[] = [], exclusions: Record<string, unknown>[] = [];
  for (const membership of input.memberships) {
    const line = lines.get(membership.lineId), route = routes.get(membership.routeId), pattern = patterns.get(membership.patternId);
    if (!line || !route || line.groupId !== membership.groupId || line.route !== route.route_short_name || ![1185062, 1185063].includes(line.agency)) {
      exclusions.push({ ...membership, reason: 'agency_or_route_identity_not_verified' }); continue;
    }
    if (/not in service|deadhead/i.test(line.destination)) { exclusions.push({ ...membership, reason: 'nonpassenger_line' }); continue; }
    const stopIds = Array.isArray(pattern?.[2]) ? pattern![2].map(numeric).filter((n): n is number => n !== null) : [];
    if (stopIds.length < 2) { exclusions.push({ ...membership, reason: 'pattern_stops_missing' }); continue; }
    const mapped = stopIds.map(id => matchedStops.get(id)?.id ?? null);
    const scoreByDirection = new Map<string, number>();
    for (const sequence of sequences.get(membership.routeId) ?? []) scoreByDirection.set(sequence.direction, Math.max(scoreByDirection.get(sequence.direction) ?? 0, orderedMatch(mapped, sequence.stops)));
    const ranked = [...scoreByDirection].sort((a, b) => b[1] - a[1]);
    const verified = ranked.length > 0 && ranked[0][1] >= .8 && ranked[0][1] - (ranked[1]?.[1] ?? 0) >= .2;
    const direction = verified ? ranked[0][0] : null;
    // A penultimate stop sees approaching vehicles without relying on terminal boarding predictions.
    const stopId = stopIds[stopIds.length - 2], gtfsStop = matchedStops.get(stopId);
    const confidence = verified && gtfsStop ? 'verified' : 'candidate';
    const key = `${membership.lineId}:${stopId}`, existing = wireQueries.get(key);
    const mapping = { patternId: membership.patternId, directionId: direction, mappingConfidence: confidence as 'verified' | 'candidate' };
    if (existing) { if (!existing.patterns!.some(p => p.patternId === mapping.patternId)) existing.patterns!.push(mapping); }
    else wireQueries.set(key, { id: `rta-${membership.routeId}-line-${membership.lineId}-stop-${stopId}`, stopId, lineId: membership.lineId,
      routeId: membership.routeId, directionId: direction, gtfsStopId: gtfsStop?.id, mappingConfidence: confidence, patterns: [mapping], metroRevision: input.metroRevision,
      mappingEvidence: 'App RTA agency and line number match GTFS; stop code plus <=80m location; >=80% ordered stop-sequence match and >=20 percentage-point direction margin when verified.' });
    diagnostics.push({ ...membership, queryStopId: stopId, gtfsStopId: gtfsStop?.id ?? null, matchedStops: mapped.filter(Boolean).length, totalStops: stopIds.length,
      directionId: direction, scores: Object.fromEntries(scoreByDirection), confidence });
  }
  const queries = [...wireQueries.values()].sort((a, b) => a.routeId!.localeCompare(b.routeId!, undefined, { numeric: true }) || a.lineId! - b.lineId! || a.stopId - b.stopId);
  const covered = new Set(queries.map(q => q.routeId));
  return { queries, metadata: { schema_version: 1, fetched_at: input.fetchedAt, metro_id: 1504, metro_revision: input.metroRevision,
    gtfs_sha256: createHash('sha256').update(input.gtfsZip).digest('hex'), query_count: queries.length, routes: [...covered],
    missing_gtfs_routes: [...routes.values()].filter(r => !covered.has(r.route_id)).map(r => ({ route_id: r.route_id, route_name: r.route_long_name })),
    verified_patterns: diagnostics.filter(d => d.confidence === 'verified').length, candidate_patterns: diagnostics.filter(d => d.confidence !== 'verified').length,
    exclusions, patterns: diagnostics, polling: { max_queries_per_request: 8, max_requests_per_second: 1, minimum_query_interval_seconds: 20 },
    coverage_note: 'Queries request arrivals at selected downstream stops. Optional vehicle locations are not a complete fleet feed. Unsupported directions and changed metro revisions remain candidate mappings.' } };
}
