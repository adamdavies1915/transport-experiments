import type { DuckDBConnection } from '@duckdb/node-api';
import { activeServices, addDays, type Schedule, type ScheduledTrip } from './otp';
const quote = (s: string) => `'${s.replace(/'/g, "''")}'`;

export interface HistoricalRun {
  day: string; route: string; legacy_id: string; block: string; destination: string;
  first_at: number; last_at: number; readings: number; vehicles: number;
}
export interface SequenceMapping {
  route: string; legacy_id: string; gtfs_id: string; service_id: string; evidence_days: string[];
}
const normalize = (v: string) => v.trim().toLowerCase().replace(/\s+/g, ' ');

// Reconstruct complete daily block sequences, never selecting a trip by its
// distance from a scheduled time. Retain contradictory evidence for rejection.
export function inferSequenceMappings(schedule: Schedule, runs: HistoricalRun[]): SequenceMapping[] {
  const groups = new Map<string, HistoricalRun[]>();
  for (const r of runs) {
    if (r.day < schedule.start || r.day > schedule.end || !r.block || !r.legacy_id ||
        ['0', 'N/A', 'null'].includes(r.legacy_id)) continue;
    const key = `${r.day}|${r.route}|${r.block}`;
    const group = groups.get(key) ?? []; group.push(r); groups.set(key, group);
  }
  const scheduled = new Map<string, Schedule['trips']>();
  for (const t of schedule.trips) {
    const key = `${t.route}|${t.block}`;
    const group = scheduled.get(key) ?? []; group.push(t); scheduled.set(key, group);
  }
  const serviceDays = new Map<string, Set<string>>();
  const evidence = new Map<string, SequenceMapping>();
  for (const group of groups.values()) {
    const first = group[0];
    const active = serviceDays.get(first.day) ?? activeServices(schedule, first.day);
    serviceDays.set(first.day, active);
    const trips = (scheduled.get(`${first.route}|${first.block}`) ?? [])
      .filter(t => active.has(t.service)).sort((a, b) => a.stops[0].departure - b.stops[0].departure);
    if (!trips.length || group.length !== trips.length || new Set(group.map(r => r.legacy_id)).size !== group.length) continue;
    // Equal scheduled starts have no unique ordering. Observed runs must also
    // have a unique order and cannot overlap or contain vehicle conflicts.
    if (trips.some((t, i) => i > 0 && t.stops[0].departure === trips[i - 1].stops[0].departure)) continue;
    const ordered = [...group].sort((a, b) => a.first_at - b.first_at);
    if (ordered.some((r, i) => r.vehicles !== 1 || r.readings < 2 ||
        !Number.isFinite(r.first_at) || !Number.isFinite(r.last_at) || r.last_at <= r.first_at ||
        (i > 0 && r.first_at <= ordered[i - 1].last_at) ||
        !r.destination || normalize(r.destination) !== normalize(trips[i].headsign))) continue;
    ordered.forEach((r, i) => {
      const t = trips[i], key = `${r.route}|${r.legacy_id}|${t.service}|${t.id}`;
      const mapping = evidence.get(key) ?? { route: r.route, legacy_id: r.legacy_id,
        gtfs_id: t.id, service_id: t.service, evidence_days: [] };
      mapping.evidence_days.push(r.day); evidence.set(key, mapping);
    });
  }
  return [...evidence.values()].map(m => ({ ...m, evidence_days: [...new Set(m.evidence_days)].sort() }));
}

export function eligibleSequenceMappings(candidates: SequenceMapping[]): SequenceMapping[] {
  const targets = new Map<string, Set<string>>();
  for (const m of candidates) {
    const key = `${m.route}|${m.legacy_id}|${m.service_id}`;
    const ids = targets.get(key) ?? new Set<string>(); ids.add(m.gtfs_id); targets.set(key, ids);
  }
  return candidates.filter(m => new Set(m.evidence_days).size >= 2 &&
    targets.get(`${m.route}|${m.legacy_id}|${m.service_id}`)!.size === 1);
}

export async function initializeSequences(connection: DuckDBConnection): Promise<void> {
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_sequence_mappings (
    schedule_hash VARCHAR, route VARCHAR, legacy_id VARCHAR, gtfs_id VARCHAR, service_id VARCHAR,
    evidence_days VARCHAR, evidence_count INTEGER,
    PRIMARY KEY(schedule_hash, route, legacy_id, gtfs_id)
  )`);
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_sequence_validation (
    schedule_hash VARCHAR, service_date DATE, service_id VARCHAR, known_pairs INTEGER,
    compared INTEGER, correct INTEGER, wrong INTEGER, updated_at TIMESTAMPTZ,
    PRIMARY KEY(schedule_hash, service_date, service_id)
  )`);
}

export function validateSequencePairs(schedule: Schedule, candidates: SequenceMapping[],
  pairs: Array<{ route: string; legacy_id: string; gtfs_id: string }>, day: string) {
  // Historical training must precede the evaluation date. A same-day sequence
  // cannot supply its own validation evidence.
  const historical = candidates.map(m => ({ ...m, evidence_days: m.evidence_days.filter(d => d < day) }))
    .filter(m => m.evidence_days.length);
  const eligible = eligibleSequenceMappings(historical);
  const lookup = new Map(eligible.map(m => [`${m.route}|${m.legacy_id}|${m.service_id}`, m]));
  const active = activeServices(schedule, day);
  const trips = new Map(schedule.trips.filter(t => active.has(t.service)).map(t => [t.id, t]));
  const unique = [...new Map(pairs.map(p => [`${p.route}|${p.legacy_id}|${p.gtfs_id}`, p])).values()];
  const reports = new Map<string, { service_id: string; known_pairs: number; compared: number; correct: number; wrong: number }>();
  for (const p of unique) {
    const trip = trips.get(p.gtfs_id);
    if (trip?.route !== p.route) continue;
    const r = reports.get(trip.service) ?? { service_id: trip.service, known_pairs: 0, compared: 0, correct: 0, wrong: 0 };
    r.known_pairs++;
    const predicted = lookup.get(`${p.route}|${p.legacy_id}|${trip.service}`);
    if (predicted) { r.compared++; if (predicted.gtfs_id === p.gtfs_id) r.correct++; else r.wrong++; }
    reports.set(trip.service, r);
  }
  return [...reports.values()];
}

export async function validateSequences(connection: DuckDBConnection, schedule: Schedule, day: string) {
  const candidates = (await connection.runAndReadAll(`SELECT route, legacy_id, gtfs_id, service_id, evidence_days
    FROM otp_sequence_mappings WHERE schedule_hash = ${quote(schedule.hash)}`)).getRowObjectsJson().map(r => ({
      route: String(r.route), legacy_id: String(r.legacy_id), gtfs_id: String(r.gtfs_id),
      service_id: String(r.service_id), evidence_days: JSON.parse(String(r.evidence_days)) as string[],
    }));
  const pairs = (await connection.runAndReadAll(`SELECT DISTINCT route, trip_id AS legacy_id, gtfs_trip_id AS gtfs_id
    FROM transit_data WHERE timestamp >= ${quote(day)}::DATE AND timestamp < ${quote(addDays(day, 1))}::DATE
      AND gtfs_trip_id IS NOT NULL AND trip_id IS NOT NULL`)).getRowObjectsJson() as unknown as Array<{ route: string; legacy_id: string; gtfs_id: string }>;
  const reports = validateSequencePairs(schedule, candidates, pairs, day);
  for (const r of reports) {
    await connection.run(`INSERT INTO otp_sequence_validation VALUES (${quote(schedule.hash)},${quote(day)},
      ${quote(r.service_id)},${r.known_pairs},${r.compared},${r.correct},${r.wrong},now())
      ON CONFLICT(schedule_hash,service_date,service_id) DO UPDATE SET known_pairs=excluded.known_pairs,
        compared=excluded.compared,correct=excluded.correct,wrong=excluded.wrong,updated_at=excluded.updated_at`);
    console.log(`[OTP] Sequence validation ${day} service ${r.service_id}: ${r.correct}/${r.compared} correct against ${r.known_pairs} observed ID pairs`);
  }
  return reports;
}

export async function rebuildSequenceMappings(connection: DuckDBConnection, schedule: Schedule, from: string, to: string): Promise<number> {
  if (addDays(from, 0) !== from || addDays(to, 0) !== to || from > to || from < schedule.start || to > schedule.end) throw new Error('Invalid sequence evidence range');
  const runs = (await connection.runAndReadAll(`SELECT timestamp::DATE::VARCHAR AS day,
    route, trip_id AS legacy_id, tablockid AS block, destination,
    MIN(epoch(timestamp)) AS first_at, MAX(epoch(timestamp)) AS last_at,
    COUNT(*)::INTEGER AS readings, COUNT(DISTINCT vid)::INTEGER AS vehicles
    FROM transit_data WHERE timestamp >= ${quote(from)}::DATE AND timestamp < ${quote(addDays(to, 1))}::DATE
      AND trip_id IS NOT NULL AND tablockid IS NOT NULL AND NOT COALESCE(is_off_route, false)
    GROUP BY 1,2,3,4,5`)).getRowObjectsJson() as unknown as HistoricalRun[];
  const candidates = inferSequenceMappings(schedule, runs);
  await connection.run('BEGIN TRANSACTION');
  try {
    await connection.run(`DELETE FROM otp_sequence_mappings WHERE schedule_hash = ${quote(schedule.hash)}`);
    for (let i = 0; i < candidates.length; i += 500) {
      await connection.run(`INSERT INTO otp_sequence_mappings VALUES ${candidates.slice(i, i + 500).map(m =>
        `(${[schedule.hash, m.route, m.legacy_id, m.gtfs_id, m.service_id, JSON.stringify(m.evidence_days)].map(quote).join(',')},${m.evidence_days.length})`).join(',')}`);
    }
    await connection.run('COMMIT');
  } catch (err) { await connection.run('ROLLBACK'); throw err; }
  return eligibleSequenceMappings(candidates).length;
}

export async function loadSequenceMappings(connection: DuckDBConnection, schedule: Schedule, day: string): Promise<Map<string, ScheduledTrip>> {
  const active = [...activeServices(schedule, day)];
  if (!active.length) return new Map();
  const rows = (await connection.runAndReadAll(`SELECT m.route, m.legacy_id, MIN(m.gtfs_id) AS gtfs_id
    FROM otp_sequence_mappings m WHERE m.schedule_hash = ${quote(schedule.hash)}
      AND m.service_id IN (${active.map(quote).join(',')})
    GROUP BY m.route, m.legacy_id HAVING COUNT(DISTINCT m.gtfs_id) = 1 AND MAX(m.evidence_count) >= 2
      AND NOT EXISTS (SELECT 1 FROM otp_trip_mappings p
        WHERE p.schedule_hash = ${quote(schedule.hash)} AND p.route = m.route AND p.legacy_id = m.legacy_id
          AND p.service_id IN (${active.map(quote).join(',')}) AND p.gtfs_id <> MIN(m.gtfs_id))`)).getRowObjectsJson();
  const trips = new Map(schedule.trips.map(t => [t.id, t]));
  return new Map(rows.flatMap(r => {
    const trip = trips.get(String(r.gtfs_id));
    return trip ? [[`${r.route}|${r.legacy_id}`, trip] as const] : [];
  }));
}
