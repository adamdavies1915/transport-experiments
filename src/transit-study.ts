import { createHash } from 'node:crypto';
import { analyzeStreetcarIntervals, type StreetcarObservation } from './streetcar-analysis';
import type { StreetcarNetwork } from '../dashboard/src/streetcar-data';
import type { StudyObservation } from './observation-types';
import type { StudyCatalog, StudyContext, StudyEncounter, StudyPassage, StudyPath, StudyQuality, StudySite, TransitStudyEvents } from './transit-study-types';
import { distance, projectPath, pathLength, featureRanges, METERS_PER_DEGREE, LONGITUDE_SCALE } from './transit-study-geometry';

export const TRANSIT_STUDY_METHOD = 'transit-two-source-study-v1';
export const STUDY_LIMITATIONS = [
  'Feeds are analyzed separately and may share an upstream vehicle source. Agreement is not independent ground truth; ETA predictions and estimated positions never become observed travel times.',
  'Travel times describe complete, non-overlapping 200 m passages. Terminal, off-route, rail-control and ambiguous geometry intervals are excluded. Incomplete trips and gaps are not filled.',
  'SSE passage matching retains the first received position in each reported provider minute; changing positions under that timestamp remain in the receipt ledger for wait diagnostics. Source timestamp uncertainty widens passage bounds.',
  'Timing bounds describe bracketing and reported clock precision, assuming positions belong to those clock intervals. They do not cover GPS error, stale or smoothed coordinates, or unknown measurement-to-provider latency.',
  'ROW contrasts match route, direction, date, weekday/weekend, four-hour band and mapped stop/signal counts. Geometry, boarding demand, operating rules and other unmeasured differences remain confounders. Contrasts are associations, not causal treatment effects.',
  'Waits are candidate stationary episodes bracketed by movement. SSE requires explicit zero speeds and uses receipt time; Le Pass requires distinct provider-GPS sample times and permits absent speed. Cached data and sparse sampling can still hide or imitate stops.',
  'Every complete signal-site encounter remains in the denominator, including no detected wait and insufficient sampling. Detected waiting per encounter is not total true signal delay. Short waits may be missed.',
  'Stops overlapping signal windows remain mixed. Nearby hardware does not prove the controlled approach or a red phase. Rail controls of unknown function are excluded.',
  'Signal sites may overlap one 200 m passage; passage travel time must not be added across sites. Each detected wait is assigned at most once to an unambiguous nearest site.',
  'Recovery of 25%, 50% or 75% of isolated detected waiting is hypothetical. These scenarios do not estimate deployed signal-priority effectiveness; mixed boarding waits are excluded.',
  'Only within-calendar-day observations are joined, in America/Chicago. Historical detail and wait coverage begin when each source was actually retained. Independent bus congestion data is deferred to version 2.',
];
const WINDOW_METERS = 200, MAX_PROVIDER_AGE = 120;
const dateFormat = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit' });
const hourFormat = new Intl.DateTimeFormat('en-US', { timeZone: 'America/Chicago', hour: 'numeric', hourCycle: 'h23' });
const hash = (parts: unknown[]) => createHash('sha256').update(JSON.stringify(parts)).digest('hex');
export function studyDate(at: number): string {
  const parts = dateFormat.formatToParts(new Date(at * 1000));
  const get = (type: string) => parts.find(p => p.type === type)!.value;
  return `${get('year')}-${get('month')}-${get('day')}`;
}
type MatchedPair = Parameters<NonNullable<Parameters<typeof analyzeStreetcarIntervals>[3]>>[0];
interface Sample extends StreetcarObservation { original: StudyObservation }
interface Chain { samples: Sample[]; receipts: StudyObservation[]; q: StudyQuality }
interface TrackPoint { at: number; position: number; original: StudyObservation }
interface Crossing { estimated: number; lower: number; upper: number }
interface WaitSample { original: StudyObservation; at: number; position: number; point: { lat: number; lon: number } }
interface CandidateWait { site_id: string; position: number; seconds: number; lower: number; upper: number }
const adapters = new WeakMap<StudyCatalog, Map<string, StreetcarNetwork>>();
type PathSite = { site: StudySite; position: number; ranges: Array<[number, number]> };
const pathContexts = new WeakMap<StudyCatalog, Map<string, { length: number; sites: PathSite[] }>>();

function adapter(catalog: StudyCatalog, route: string, direction: string | null): StreetcarNetwork {
  let cache = adapters.get(catalog); if (!cache) { cache = new Map(); adapters.set(catalog, cache); }
  const key = JSON.stringify([route, direction]), existing = cache.get(key); if (existing) return existing;
  const paths = catalog.paths.filter(p => p.route_id === route && (direction == null || p.direction_id === direction));
  // The old matcher treats corridor as an opaque grouping key at runtime. This
  // adapter generalizes that key without changing its published streetcar API.
  const network = {
    version: catalog.version, generated_at: catalog.generated_at, schedule_hash: catalog.schedule_hash,
    corridors: [{ id: route, name: route, routes: [route] }],
    paths: paths.map(p => ({ id: p.id, corridor: route, route, direction: p.direction_id, headsign: p.name, points: p.points, stop_ids: p.stop_ids })),
    sites: catalog.sites.filter(s => s.route_ids.includes(route) && (!s.path_ids || s.path_ids.some(id => paths.some(p => p.id === id))))
      .map(s => ({ ...s, corridor: route, routes: [route] })),
    sources: catalog.sources, mapillary_status: 'See the transit study catalog for dated evidence.',
  } as unknown as StreetcarNetwork;
  // Detailed applicability is checked again on the resolved path below.
  cache.set(key, network); return network;
}
function count(q: StudyQuality, reason: string, n = 1): void { q.excluded[reason] = (q.excluded[reason] ?? 0) + n; }
function applicable(site: StudySite, path: StudyPath): boolean {
  if (!site.route_ids.includes(path.route_id) || site.path_ids && !site.path_ids.includes(path.id)) return false;
  return site.kind !== 'stop' || site.source_ids.some(id => path.stop_ids.includes(id.replace(/^gtfs:(?:stop:)?/, '')));
}
function pathContext(catalog: StudyCatalog, path: StudyPath): { length: number; sites: PathSite[] } {
  let cache = pathContexts.get(catalog); if (!cache) { cache = new Map(); pathContexts.set(catalog, cache); }
  const prior = cache.get(path.id); if (prior) return prior;
  const sites = catalog.sites.filter(s => applicable(s, path)).map(site => ({ site, projection: projectPath(path, site, 40) }))
    .filter((s): s is { site: StudySite; projection: { position: number; distance: number } } => s.projection != null)
    .map(s => ({ site: s.site, position: s.projection.position, ranges: featureRanges(path, s.site) }));
  const context = { length: pathLength(path.points), sites }; cache.set(path.id, context); return context;
}
function crossing(points: TrackPoint[], boundary: number): Crossing | null {
  const epsilon = 1e-6;
  let low = 0, high = points.length;
  while (low < high) { const mid = (low + high) >>> 1; if (points[mid].position < boundary - epsilon) low = mid + 1; else high = mid; }
  const after = points[low], before = points[low - 1];
  if (!after || !before || before.position >= boundary - epsilon) return null;
  if (Math.abs(after.position - boundary) <= epsilon) return { estimated: after.at, lower: before.at, upper: after.at };
  if (after.position <= before.position) return null;
  return { estimated: before.at + (after.at - before.at) * (boundary - before.position) / (after.position - before.position), lower: before.at, upper: after.at };
}
export function classifyStudyRow(catalog: StudyCatalog, pathId: string, from: number, to: number, date: string): { row_class: StudyPassage['row_class']; row_section_ids: string[] } {
  const sections = catalog.row_sections.filter(s => s.path_id === pathId && s.row_class !== 'unknown' &&
    s.reviewed_at && s.evidence_urls.length && s.valid_from && s.valid_from <= date && (!s.valid_to || s.valid_to >= date) &&
    s.to_meters > from && s.from_meters < to).sort((a, b) => a.from_meters - b.from_meters);
  const ids = sections.map(s => s.id).sort();
  if (!sections.length || new Set(sections.map(s => s.row_class)).size !== 1) return { row_class: 'unknown', row_section_ids: ids };
  let covered = from;
  for (const section of sections) {
    if (section.from_meters > covered + 1e-6) return { row_class: 'unknown', row_section_ids: ids };
    covered = Math.max(covered, section.to_meters);
  }
  return { row_class: covered >= to - 1e-6 ? sections[0].row_class : 'unknown', row_section_ids: ids };
}
function context(sites: StudySite[]): StudyContext {
  const signal = sites.some(s => s.kind === 'signal'), stop = sites.some(s => s.kind === 'stop');
  return signal ? stop ? 'both' : 'signal_only' : stop ? 'stop_only' : 'neither';
}
function validObservation(o: StudyObservation, catalog: StudyCatalog): string | null {
  if (o.in_service === false) return 'not_in_service';
  if (!o.vehicle_id || !o.route_id || o.lat == null || o.lon == null || o.observed_at == null) return 'missing_identity_location_or_clock';
  if (![o.lat, o.lon, o.observed_at, o.received_at, o.timestamp_precision_seconds].every(Number.isFinite) ||
      Math.abs(o.lat) > 90 || Math.abs(o.lon) > 180 || o.timestamp_precision_seconds < 0 || o.timestamp_precision_seconds > 60)
    return 'invalid_coordinates_or_clock';
  if (o.mapping_confidence !== 'verified') return 'unverified_route_mapping';
  if (!catalog.paths.some(p => p.route_id === o.route_id && (o.direction_id == null || p.direction_id === o.direction_id))) return 'unmapped_route_or_direction';
  if (o.off_route) return 'off_route';
  if (o.location_source === 'estimated' || o.source === 'lepass' && o.location_source !== 'provider_gps') return 'non_gps_location';
  const age = o.received_at - o.observed_at;
  if (age < 0 || age > MAX_PROVIDER_AGE) return 'stale_or_future_observation';
  return null;
}
function waitSamples(receipts: StudyObservation[], path: StudyPath, start: number, end: number): WaitSample[] {
  const samples: WaitSample[] = [], seen = new Set<number>();
  for (const original of receipts) {
    const clock = original.source === 'lepass' ? original.observed_at : original.received_at;
    if (clock == null || clock < start || clock > end || original.lat == null || original.lon == null) continue;
    if (original.source === 'lepass') {
      if (original.observed_at == null || seen.has(original.observed_at)) continue;
      seen.add(original.observed_at);
    }
    const point = { lat: original.lat, lon: original.lon }, projection = projectPath(path, point, 35);
    if (projection) samples.push({ original, point, position: projection.position, at: clock });
  }
  return samples;
}
function detectWaits(samples: WaitSample[], sites: Array<{ site: StudySite; position: number }>, q: StudyQuality): CandidateWait[] {
  const waits: CandidateWait[] = [];
  let start = 0;
  while (start < samples.length) {
    const first = samples[start], gap = first.original.source === 'sse' ? 25 : 45;
    let end = start + 1, minLat = first.point.lat, maxLat = minLat, minLon = first.point.lon, maxLon = minLon;
    while (end < samples.length) {
      const next = samples[end], prev = samples[end - 1];
      if (next.at <= prev.at || next.at - prev.at > gap) break;
      const a = Math.min(minLat, next.point.lat), b = Math.max(maxLat, next.point.lat);
      const c = Math.min(minLon, next.point.lon), d = Math.max(maxLon, next.point.lon);
      if (Math.hypot((b - a) * METERS_PER_DEGREE, (d - c) * LONGITUDE_SCALE) > 12) break;
      minLat = a; maxLat = b; minLon = c; maxLon = d; end++;
    }
    const cluster = samples.slice(start, end), last = cluster.at(-1)!;
    const speeds = cluster.every(s => s.original.source === 'sse' ? s.original.speed_mph === 0 : s.original.speed_mph == null || s.original.speed_mph === 0);
    if (cluster.length >= 3 && last.at - first.at >= 20 && speeds) {
      const center = { lat: (minLat + maxLat) / 2, lon: (minLon + maxLon) / 2 };
      const bracket = (n: number, step: number, at: number) => {
        for (; n >= 0 && n < samples.length; n += step) {
          if (Math.abs(samples[n].at - at) > (first.original.source === 'sse' ? 45 : 90)) return null;
          if (distance(samples[n].point, center) >= 25) return samples[n];
        }
        return null;
      };
      const before = bracket(start - 1, -1, first.at), after = bracket(end, 1, last.at);
      const nearby = sites.filter(s => s.site.kind === 'signal').map(s => ({ ...s, distance: distance(s.site, center) }))
        .filter(s => s.distance <= 40).sort((a, b) => a.distance - b.distance || a.site.id.localeCompare(b.site.id));
      if (before && after && nearby.length) {
        if (nearby[1] && nearby[1].distance - nearby[0].distance < 8) count(q, 'ambiguous_wait_site');
        else {
          const precision = first.original.source === 'sse' ? 0 : Math.max(...cluster.map(s => s.original.timestamp_precision_seconds));
          waits.push({ site_id: nearby[0].site.id, position: cluster.reduce((sum, s) => sum + s.position, 0) / cluster.length,
            seconds: last.at - first.at, lower: Math.max(0, last.at - first.at - precision), upper: after.at - before.at + precision });
        }
      }
    } else if (cluster.length >= 3 && last.at - first.at >= 20 && !speeds) count(q, 'stationary_speed_contradiction');
    start = end;
  }
  return waits;
}

/** Source-specific events. No persistence, network calls, predictions or pooled trajectories. */
export function analyzeTransitStudy(catalog: StudyCatalog, observations: StudyObservation[]): TransitStudyEvents {
  const result: TransitStudyEvents = { passages: [], encounters: [], quality: [] };
  const quality = new Map<string, StudyQuality>(), vehicles = new Map<string, StudyObservation[]>();
  for (const o of observations) {
    const clock = Number.isFinite(o.observed_at) && o.observed_at != null ? o.observed_at : o.received_at;
    if (!Number.isFinite(clock)) continue;
    const date = studyDate(clock), key = `${o.source}:${date}`;
    const q = quality.get(key) ?? { date, source: o.source, raw_observations: 0, usable_observations: 0,
      completed_passages: 0, complete_encounters: 0, excluded: {} };
    q.raw_observations++; quality.set(key, q);
    const vehicleKey = JSON.stringify([key, o.vehicle_id]), group = vehicles.get(vehicleKey) ?? [];
    group.push(o); vehicles.set(vehicleKey, group);
  }
  const analyzeChain = ({ samples, receipts, q }: Chain) => {
    if (samples.length < 2) return;
    const n = adapter(catalog, samples[0].route, samples[0].original.direction_id);
    const matched: MatchedPair[] = [];
    const diagnostic = analyzeStreetcarIntervals(n, q.date, samples, m => matched.push(m));
    for (const d of diagnostic.quality) for (const [reason, amount] of Object.entries(d.excluded)) count(q, `matching_${reason}`, amount);
    const analyzeTrack = (track: MatchedPair[]) => {
      if (!track.length) return;
      const first = track[0], path = catalog.paths.find(p => p.id === n.paths[first.option.path].id)!;
      const a = first.pair.a.observation as Sample, b = track.at(-1)!.pair.b.observation as Sample;
      const runId = hash([TRANSIT_STUDY_METHOD, catalog.version, q.source, q.date, a.vid, a.trip_id, path.id, a.at, a.original.received_at]);
      const points: TrackPoint[] = [{ at: a.at, position: first.option.from, original: a.original }];
      for (const pair of track) points.push({ at: pair.pair.b.observation.at,
        position: Math.max(points.at(-1)!.position, pair.option.to), original: (pair.pair.b.observation as Sample).original });
      const { sites, length } = pathContext(catalog, path);
      const ws = waitSamples(receipts, path, q.source === 'lepass' ? a.at : a.original.received_at,
        q.source === 'lepass' ? b.at : b.original.received_at);
      const waits = detectWaits(ws, sites, q);
      const precision = Math.max(...points.map(p => p.original.timestamp_precision_seconds));
      const last = points.at(-1)!.position;
      for (let from = Math.ceil((points[0].position - 1e-6) / WINDOW_METERS) * WINDOW_METERS; from + WINDOW_METERS <= last + 1e-6; from += WINDOW_METERS) {
        const to = from + WINDOW_METERS;
        if (from < 60 || to > length - 60) continue;
        const entry = crossing(points, from), exit = crossing(points, to);
        if (!entry || !exit || exit.estimated <= entry.estimated) continue;
        const exposed = sites.filter(s => s.ranges.some(([a, b]) => a <= to && b >= from));
        if (exposed.some(s => s.site.kind === 'rail_signal')) continue;
        const hour = Number(hourFormat.format(new Date(entry.estimated * 1000)));
        const day = new Date(`${q.date}T12:00:00Z`).getUTCDay();
        const dimensions = { source: q.source, date: q.date, mode: path.mode, route_id: path.route_id,
          direction_id: path.direction_id, path_id: path.id, hour, day_type: day === 0 || day === 6 ? 'weekend' as const : 'weekday' as const,
          time_band: Math.floor(hour / 4) };
        const windowId = `${path.id}:${from}`;
        const p: StudyPassage = { ...dimensions, id: hash([runId, windowId]), run_id: runId, vehicle_id: a.vid, trip_id: a.trip_id,
          window_id: windowId, from_meters: from, to_meters: to, distance_meters: WINDOW_METERS,
          entry_at: entry.estimated, exit_at: exit.estimated, duration_seconds: exit.estimated - entry.estimated,
          duration_lower_seconds: Math.max(0, exit.lower - entry.upper - precision), duration_upper_seconds: exit.upper - entry.lower + precision,
          timestamp_precision_seconds: precision, ...classifyStudyRow(catalog, path.id, from, to, q.date),
          context: context(exposed.map(s => s.site)), signal_ids: exposed.filter(s => s.site.kind === 'signal').map(s => s.site.id).sort(),
          stop_ids: exposed.filter(s => s.site.kind === 'stop').map(s => s.site.id).sort(), network_version: catalog.version, method: TRANSIT_STUDY_METHOD };
        result.passages.push(p); q.completed_passages++;
        // A site belongs to the window containing its projected anchor, not to
        // every neighboring buffer that happens to include that site.
        for (const { site } of sites.filter(s => s.site.kind === 'signal' && s.position >= from - 1e-6 && s.position < to - 1e-6)) {
          const selected = waits.filter(w => w.site_id === site.id && w.position >= from - 40 && w.position < to + 40);
          const sampling = ws.filter(s => s.position >= from - 25 && s.position <= to + 25);
          const gap = q.source === 'sse' ? 25 : 45;
          const evaluable = sampling.length >= 3 && sampling.every((s, i) => !i || s.at - sampling[i - 1].at <= gap);
          const encounter: StudyEncounter = { ...dimensions, id: hash([p.id, site.id]), passage_id: p.id, run_id: runId, vehicle_id: a.vid,
            site_id: site.id, approach_id: `${path.id}:${site.id}`, context: p.stop_ids.length ? 'both' : 'signal_only',
            control_verified: site.verification === 'reviewed_control', entry_at: p.entry_at, exit_at: p.exit_at,
            wait_status: selected.length ? 'detected' : evaluable ? 'not_detected' : 'insufficient_sampling', wait_events: selected.length,
            wait_seconds: selected.reduce((sum, w) => sum + w.seconds, 0), wait_lower_seconds: selected.reduce((sum, w) => sum + w.lower, 0),
            wait_upper_seconds: selected.reduce((sum, w) => sum + w.upper, 0), wait_clock: q.source === 'sse' ? 'collector_receipt' : 'provider_sample',
            network_version: catalog.version, method: TRANSIT_STUDY_METHOD };
          result.encounters.push(encounter); q.complete_encounters++;
        }
      }
    };
    let track: MatchedPair[] = [];
    for (const pair of matched) {
      const prev = track.at(-1);
      if (prev && (prev.pair.b !== pair.pair.a || prev.option.path !== pair.option.path)) { analyzeTrack(track); track = []; }
      track.push(pair);
    }
    analyzeTrack(track);
  };
  for (const group of vehicles.values()) {
    const lepass = group[0].source === 'lepass';
    const invalid = new Map<StudyObservation, string | null>();
    if (lepass) for (const o of group) invalid.set(o, validObservation(o, catalog));
    // Independent arrival queries can deliver old GPS samples after newer ones.
    // Match Le Pass on its provider clock; SSE keeps its receipt/minute semantics.
    // An invalid provider clock still interrupts evidence at its receipt time.
    const orderClock = (o: StudyObservation) => lepass && invalid.get(o) === null ? o.observed_at! : o.received_at;
    group.sort((a, b) => orderClock(a) - orderClock(b) || a.received_at - b.received_at || a.observation_id.localeCompare(b.observation_id));
    const first = group[0], clock = first.observed_at != null && Number.isFinite(first.observed_at) ? first.observed_at : first.received_at;
    const q = quality.get(`${first.source}:${studyDate(clock)}`)!;
    const collisions = new Set<number>(), seen = new Map<number, StudyObservation>();
    if (lepass) for (const o of group) if (o.observed_at != null) {
      const before = seen.get(o.observed_at);
      if (before && (before.lat !== o.lat || before.lon !== o.lon || before.route_id !== o.route_id || before.trip_id !== o.trip_id ||
          before.direction_id !== o.direction_id || before.pattern_id !== o.pattern_id)) collisions.add(o.observed_at);
      seen.set(o.observed_at, o);
    }
    const acceptedSamples = new Set<number>();
    let chain: Chain = { samples: [], receipts: [], q };
    const flush = () => { analyzeChain(chain); chain = { samples: [], receipts: [], q }; };
    for (const o of group) {
      const reason = (lepass ? invalid.get(o)! : validObservation(o, catalog)) ??
        (o.observed_at != null && collisions.has(o.observed_at) ? 'conflicting_sample_timestamp' : null);
      if (reason) { count(q, reason); flush(); continue; }
      // Deduplicate across the complete vehicle/day, before a replay can split a
      // trajectory and rebuild a passage. Every receipt remains in raw storage.
      if (lepass && acceptedSamples.has(o.observed_at!)) { count(q, 'repeated_sample'); continue; }
      if (lepass) acceptedSamples.add(o.observed_at!);
      const previous = chain.samples.at(-1);
      if (previous && (previous.route !== o.route_id || previous.trip_id !== o.trip_id || previous.original.direction_id !== o.direction_id ||
          lepass && previous.original.pattern_id !== o.pattern_id || o.observed_at! < previous.at ||
          (lepass ? o.observed_at! - previous.at : o.received_at - chain.receipts.at(-1)!.received_at) > 120)) {
        count(q, 'trajectory_break'); flush();
      }
      chain.receipts.push(o);
      const last = chain.samples.at(-1);
      if (last?.at === o.observed_at) {
        count(q, last.lat === o.lat && last.lon === o.lon ? 'repeated_sample' : 'positions_within_reported_minute');
        continue;
      }
      q.usable_observations++;
      chain.samples.push({ vid: o.vehicle_id!, route: o.route_id!, trip_id: o.trip_id, at: o.observed_at!, lat: o.lat!, lon: o.lon!, off_route: false, original: o });
    }
    flush();
  }
  result.quality = [...quality.values()].sort((a, b) => a.date.localeCompare(b.date) || a.source.localeCompare(b.source));
  result.passages.sort((a, b) => a.entry_at - b.entry_at || a.id.localeCompare(b.id));
  result.encounters.sort((a, b) => a.entry_at - b.entry_at || a.id.localeCompare(b.id));
  return result;
}
