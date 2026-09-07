import type { DuckDBConnection } from '@duckdb/node-api';
import { activeServices, addDays, type Schedule, type ScheduledTrip } from './otp';

const quote = (v: string) => `'${v.replace(/'/g, "''")}'`;
export interface TripMapping { route: string; tatripid: string; tripid: string }
export interface CrosswalkFile { schedule_sha256: string; evidence_date: string; mappings: TripMapping[] }

export async function initializeCrosswalk(connection: DuckDBConnection): Promise<void> {
  // Keep competing pairs. Never overwrite contradictory evidence with the latest
  // value: a conflicted legacy ID must become ineligible for reconstruction.
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_trip_mappings (
    schedule_hash VARCHAR, route VARCHAR, legacy_id VARCHAR, gtfs_id VARCHAR,
    evidence_from DATE, evidence_to DATE,
    PRIMARY KEY(schedule_hash, route, legacy_id, gtfs_id)
  )`);
  await connection.run('ALTER TABLE otp_trip_mappings ADD COLUMN IF NOT EXISTS service_id VARCHAR');
}

export async function importCrosswalk(connection: DuckDBConnection, schedule: Schedule, input: unknown): Promise<number> {
  const file = input as CrosswalkFile;
  if (!file || file.schedule_sha256 !== schedule.hash || !Array.isArray(file.mappings) ||
      typeof file.evidence_date !== 'string' || addDays(file.evidence_date, 0) !== file.evidence_date ||
      file.evidence_date < schedule.start || file.evidence_date > schedule.end) throw new Error('Crosswalk archive/date mismatch');
  const active = activeServices(schedule, file.evidence_date);
  const trips = new Map(schedule.trips.map(t => [t.id, t]));
  for (const m of file.mappings) {
    const t = trips.get(m.tripid);
    if (typeof m.tatripid !== 'string' || !m.tatripid || ['0', 'N/A', 'null'].includes(m.tatripid) ||
        !t || t.route !== m.route || !active.has(t.service)) throw new Error('Crosswalk contains an invalid trip/route/service pair');
  }
  await initializeCrosswalk(connection);
  if (!file.mappings.length) return 0;
  await connection.run(`INSERT INTO otp_trip_mappings (schedule_hash, route, legacy_id, gtfs_id, evidence_from, evidence_to, service_id) VALUES ${file.mappings.map(m =>
    `(${quote(schedule.hash)},${quote(m.route)},${quote(m.tatripid)},${quote(m.tripid)},${quote(file.evidence_date)},${quote(file.evidence_date)},${quote(trips.get(m.tripid)!.service)})`).join(',')}
    ON CONFLICT(schedule_hash, route, legacy_id, gtfs_id) DO UPDATE SET
      service_id = excluded.service_id,
      evidence_from = LEAST(otp_trip_mappings.evidence_from, excluded.evidence_from),
      evidence_to = GREATEST(otp_trip_mappings.evidence_to, excluded.evidence_to)`);
  return file.mappings.length;
}

export async function learnCrosswalk(connection: DuckDBConnection, schedule: Schedule, day: string): Promise<void> {
  const result = (await connection.runAndReadAll(`SELECT DISTINCT route, trip_id AS tatripid, gtfs_trip_id AS tripid
    FROM transit_data WHERE timestamp >= ${quote(day)}::DATE AND timestamp < ${quote(addDays(day, 1))}::DATE
    AND gtfs_trip_id IS NOT NULL AND trip_id IS NOT NULL AND trip_id NOT IN ('', '0', 'N/A', 'null')`)).getRowObjectsJson() as unknown as TripMapping[];
  const active = activeServices(schedule, day);
  const trips = new Map(schedule.trips.filter(t => active.has(t.service)).map(t => [t.id, t]));
  await importCrosswalk(connection, schedule, { schedule_sha256: schedule.hash, evidence_date: day,
    mappings: result.filter(m => trips.get(m.tripid)?.route === m.route) });
}

export async function loadCrosswalk(connection: DuckDBConnection, schedule: Schedule, day?: string): Promise<Map<string, ScheduledTrip>> {
  // Older observed pairs predate service scoping. Resolve them from their exact
  // archived GTFS IDs; one legacy ID can legitimately vary by service calendar.
  await connection.run(`UPDATE otp_trip_mappings m SET service_id = t.service_id
    FROM (VALUES ${schedule.trips.map(t => `(${quote(t.id)},${quote(t.service)})`).join(',')}) t(gtfs_id,service_id)
    WHERE m.schedule_hash = ${quote(schedule.hash)} AND m.gtfs_id = t.gtfs_id AND m.service_id IS NULL`);
  const active = day ? [...activeServices(schedule, day)] : undefined;
  const records = (await connection.runAndReadAll(`SELECT route, legacy_id, MIN(gtfs_id) AS gtfs_id
    FROM otp_trip_mappings WHERE schedule_hash = ${quote(schedule.hash)}
    ${active ? `AND service_id IN (${active.length ? active.map(quote).join(',') : "''"})` : ''}
    GROUP BY route, legacy_id HAVING COUNT(DISTINCT gtfs_id) = 1`)).getRowObjectsJson();
  const trips = new Map(schedule.trips.map(t => [t.id, t]));
  const mapping = new Map<string, ScheduledTrip>();
  for (const r of records) {
    const trip = trips.get(String(r.gtfs_id));
    if (trip?.route === r.route) mapping.set(`${r.route}|${r.legacy_id}`, trip);
  }
  return mapping;
}

export function mappedTrip(mapping: Map<string, ScheduledTrip>, route: string, legacyId: string,
  block: string | null, destination: string | null): ScheduledTrip | undefined {
  const trip = mapping.get(`${route}|${legacyId}`);
  if (!trip) return;
  if (block && !['N/A', 'null'].includes(block) && trip.block && block !== trip.block) return;
  const normalize = (v: string) => v.trim().toLowerCase().replace(/\s+/g, ' ');
  if (destination && trip.headsign && normalize(destination) !== normalize(trip.headsign)) return;
  return trip;
}
