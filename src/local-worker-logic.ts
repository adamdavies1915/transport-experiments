/** Pure study partitioning: physical archives retain receipt dates; analysis uses provider dates. */
import { createHash } from 'node:crypto';
import type { StudyObservation } from './observation-types';
import type { StudyCatalog, TransitStudyEvents } from './transit-study-types';
import { analyzeTransitStudy, studyDate } from './transit-study';
import { observationEpoch } from './otp';

export const LEGACY_STUDY_DATE_SQL = "COALESCE(timezone('America/Chicago',observed_at)::DATE,timestamp::DATE)";
export const SNAPSHOT_STUDY_DATE_SQL = "timezone('America/Chicago',COALESCE(provider_observed_at,received_at))::DATE";
const identity = (o: StudyObservation) => JSON.stringify([o.source, o.route_id,
  o.provider_vehicle_id ?? o.vehicle_id?.replace(new RegExp(`^${o.source}:`), '')]);
const sourceDate = (o: StudyObservation) => {
  const at = o.observed_at != null && Number.isFinite(o.observed_at) ? o.observed_at : o.received_at;
  return Number.isFinite(at) ? studyDate(at) : null;
};
const canonicalVehicle = (o: StudyObservation): StudyObservation => ({ ...o,
  vehicle_id: o.provider_vehicle_id ? `${o.source}:${o.provider_vehicle_id}` : o.vehicle_id });
const RECORDER_PRECEDENCE_REVISION = 'historical-local-precedence-v1';
/** A complete GPS interval cannot span more than 90 seconds. The same margin
 * around recorder transitions avoids reconstructing an overlapping trajectory
 * from delayed copies at either edge. Long gaps remain available to history. */
const COVERAGE_GAP_SECONDS = 90;
function providerCoverage(observations: StudyObservation[]) {
  const times = new Map<string, number[]>();
  for (const o of observations) if (o.source === 'sse' && o.vehicle_id && o.observed_at != null && Number.isFinite(o.observed_at)) {
    const key = identity(o), values = times.get(key) ?? []; values.push(o.observed_at); times.set(key, values);
  }
  const ranges = new Map<string, Array<[number, number]>>();
  for (const [key, values] of times) {
    const merged: Array<[number, number]> = [];
    for (const at of values.sort((a, b) => a - b)) {
      const last = merged.at(-1);
      if (last && at - last[1] <= COVERAGE_GAP_SECONDS) last[1] = at; else merged.push([at, at]);
    }
    ranges.set(key, merged);
  }
  return ranges;
}
const covered = (o: StudyObservation, ranges: ReturnType<typeof providerCoverage>) => o.observed_at != null &&
  (ranges.get(identity(o)) ?? []).some(([a, b]) => o.observed_at! >= a - COVERAGE_GAP_SECONDS && o.observed_at! <= b + COVERAGE_GAP_SECONDS);

export interface LegacyStudyRow {
  vid: unknown; route: unknown; trip: unknown; instant: unknown; wall_time: unknown;
  lat: unknown; lon: unknown; is_off_route: unknown; pid: unknown;
}
/** Provider time is used as a placeholder only for passage matching; never a receipt measurement. */
export function legacyStudyObservation(row: LegacyStudyRow): StudyObservation | null {
  const at = row.instant == null ? observationEpoch(String(row.wall_time), 'America/Chicago') : Number(row.instant);
  if (at == null || !Number.isFinite(at)) return null;
  const observation: StudyObservation = { source: 'sse', observation_id: '',
    vehicle_id: row.vid == null ? null : `sse:${row.vid}`, provider_vehicle_id: row.vid == null ? null : String(row.vid),
    route_id: row.route == null ? null : String(row.route), trip_id: row.trip == null ? null : String(row.trip),
    observed_at: at, received_at: at, lat: row.lat == null ? null : Number(row.lat), lon: row.lon == null ? null : Number(row.lon),
    speed_mph: null, off_route: row.is_off_route === true, location_source: 'provider_gps', timestamp_precision_seconds: 60,
    direction_id: null, pattern_id: row.pid == null ? null : String(row.pid), mapping_confidence: 'verified' };
  // SQL row order and repeated migration cannot change legacy event identity.
  observation.observation_id = 'legacy:' + createHash('sha256').update(JSON.stringify(observation)).digest('hex');
  return observation;
}
export interface StudyDayInputs { historical: StudyObservation[]; dense: StudyObservation[]; legacy: StudyObservation[] }
export function prepareStudyDay(date: string, input: StudyDayInputs) {
  const observed = new Map<string, StudyObservation>();
  const duplicateReceipts = { sse: 0, lepass: 0 };
  const dense = input.dense.map(canonicalVehicle).filter(o => sourceDate(o) === date);
  const denseKeys = new Set(dense.map(o => JSON.stringify([o.source, o.observation_id])));
  // Independent server/local collectors give the same physical SSE evidence
  // different receipt IDs. Prefer the normalized local recording over its
  // continuous provider-time coverage; retain server snapshots in true gaps.
  // Invalid local provider clocks must not suppress valid historical evidence.
  const localCoverage = providerCoverage(dense.filter(o => o.observed_at != null &&
    Number.isFinite(o.received_at) && o.received_at >= o.observed_at && o.received_at - o.observed_at <= 120));
  let overlappingHistorical = 0;
  for (const [historical, rows] of [[true, input.historical], [false, dense]] as const) {
    for (const original of rows) {
      const o = canonicalVehicle(original);
      if (sourceDate(o) !== date) continue;
      const key = JSON.stringify([o.source, o.observation_id]);
      // Preserve exact-receipt diagnostics before the local copy replaces it.
      if (historical && !denseKeys.has(key) && covered(o, localCoverage)) { overlappingHistorical++; continue; }
      if (observed.has(key)) duplicateReceipts[o.source]++;
      observed.set(key, o);
    }
  }
  const receipts = [...observed.values()].sort((a, b) => a.received_at - b.received_at || a.observation_id.localeCompare(b.observation_id));
  const ranges = providerCoverage(receipts);
  let overlappingLegacy = 0, duplicateLegacy = 0, ambiguousLegacy = 0;
  const samples = new Map<string, StudyObservation[]>();
  for (const original of input.legacy) {
    const o = canonicalVehicle(original);
    if (sourceDate(o) !== date || o.observed_at == null) continue;
    // Trip IDs may gain a GTFS mapping in the newer ledger. Vehicle/time overlap
    // still identifies duplicate physical evidence, so trip is deliberately absent.
    if (covered(o, ranges)) {
      overlappingLegacy++; continue;
    }
    const key = JSON.stringify([identity(o), o.observed_at]), list = samples.get(key) ?? [];
    list.push(o); samples.set(key, list);
  }
  const providerOnly: StudyObservation[] = [];
  for (const rows of samples.values()) {
    const states = new Set(rows.map(o => JSON.stringify([o.lat, o.lon, o.trip_id, o.off_route])));
    // Without receipt order, changing positions in the same reported minute have
    // no defensible first sample. Reject the ambiguity instead of choosing SQL order.
    if (states.size > 1) {
      ambiguousLegacy += rows.length;
      // Preserve an invalid separator at that time so nearby valid points cannot
      // silently bridge a rejected sample, even in unusual sub-minute history.
      providerOnly.push({ ...rows.sort((a, b) => a.observation_id.localeCompare(b.observation_id))[0], lat: null, lon: null, speed_mph: null });
      continue;
    }
    duplicateLegacy += rows.length - 1;
    providerOnly.push({ ...rows.sort((a, b) => a.observation_id.localeCompare(b.observation_id))[0], speed_mph: null });
  }
  return { receipts, provider_only: providerOnly, duplicate_receipts: duplicateReceipts, excluded: {
    historical_dense_overlap: overlappingHistorical,
    legacy_receipt_overlap: overlappingLegacy,
    duplicate_legacy_sample: duplicateLegacy, ambiguous_legacy_provider_minute: ambiguousLegacy,
  } };
}
export function analyzeStudyDay(catalog: StudyCatalog, date: string, input: StudyDayInputs): TransitStudyEvents {
  const prepared = prepareStudyDay(date, input), actual = analyzeTransitStudy(catalog, prepared.receipts);
  const legacy = analyzeTransitStudy(catalog, prepared.provider_only);
  actual.passages.push(...legacy.passages);
  actual.quality.push(...legacy.quality.map(q => ({ ...q, complete_encounters: 0,
    excluded: { ...q.excluded, legacy_missing_receipt_history: prepared.provider_only.length } })));
  // Never publish provider-only encounter rows, even "not detected" ones: no
  // historical receipt evidence means there is no honest wait denominator.
  const exclusions = Object.fromEntries(Object.entries(prepared.excluded).filter(([, n]) => n > 0));
  for (const source of ['sse', 'lepass'] as const) {
    const reasons = { ...(source === 'sse' ? exclusions : {}),
      ...(prepared.duplicate_receipts[source] ? { duplicate_receipt_copy: prepared.duplicate_receipts[source] } : {}) };
    if (Object.keys(reasons).length) actual.quality.push({ date, source, raw_observations: 0,
      usable_observations: 0, completed_passages: 0, complete_encounters: 0, excluded: reasons });
  }
  if (actual.passages.some(p => p.date !== date) || actual.encounters.some(e => e.date !== date) || actual.quality.some(q => q.date !== date))
    throw new Error('Study date partition mismatch');
  return actual;
}

/** A receipt in the first two minutes can contain a fresh preceding-day sample.
 * Later arrivals cannot change that preceding study day under the 120-second freshness rule. */
export function pendingStudyDaysSql(method: string): string {
  const literal = `'${method.replaceAll("'", "''")}'`;
  return `WITH days AS (
    SELECT ${LEGACY_STUDY_DATE_SQL} AS service_date,'legacy' AS origin,COUNT(*) AS n,
      MAX(COALESCE(epoch(observed_at),epoch(timestamp))) AS last_at FROM transit_data GROUP BY 1
    UNION ALL SELECT timezone('America/Chicago',received_at)::DATE,'dense',SUM(observations),MAX(epoch(received_at))
      FROM collection_batches WHERE observations>0 GROUP BY 1
    UNION ALL SELECT timezone('America/Chicago',received_at)::DATE-1,'dense_next_midnight',SUM(observations),MAX(epoch(received_at))
      FROM collection_batches WHERE observations>0 AND received_at < timezone('America/Chicago',timezone('America/Chicago',received_at)::DATE::TIMESTAMP)+INTERVAL '120 seconds' GROUP BY 1
    UNION ALL SELECT ${SNAPSHOT_STUDY_DATE_SQL},'snapshots',COUNT(*),MAX(epoch(received_at)) FROM streetcar_snapshots GROUP BY 1
  ), revisions AS (SELECT service_date,
    CASE WHEN COUNT(*) FILTER (WHERE origin='snapshots')>0 AND COUNT(*) FILTER (WHERE origin IN ('dense','dense_next_midnight'))>0
      THEN '${RECORDER_PRECEDENCE_REVISION}:' ELSE '' END || string_agg(origin||':'||n||':'||last_at,',' ORDER BY origin) AS revision FROM days GROUP BY 1)
  SELECT r.service_date::VARCHAR AS date,r.revision FROM revisions r LEFT JOIN study_dates s ON s.date=r.service_date
  WHERE r.service_date IS NOT NULL AND (s.date IS NULL OR s.source_revision<>r.revision OR s.method_revision<>${literal})
  ORDER BY CASE WHEN r.service_date>=timezone('America/Chicago',now())::DATE-2 THEN 0 ELSE 1 END,r.service_date DESC LIMIT 2`;
}
