import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { readFile } from 'node:fs/promises';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { fork, type ChildProcess } from 'node:child_process';
import 'dotenv/config';
import { activeServices, addDays, estimateOtp, localDay, observationEpoch, OTP_METHOD, readSchedule,
  type Observation, type Schedule } from './otp';
import { initializeCrosswalk, importCrosswalk, learnCrosswalk, loadCrosswalk, mappedTrip } from './trip-crosswalk';
import { initializeSequences, loadSequenceMappings, rebuildSequenceMappings, validateSequences } from './trip-sequence';

export const GTFS_URL = 'https://www.norta.com/RTA/media/GTFS/GTFS.zip';
export const quote = (v: string): string => `'${v.replace(/'/g, "''")}'`;
export async function initializeOtp(connection: DuckDBConnection): Promise<void> {
  await initializeCrosswalk(connection);
  await initializeSequences(connection);
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_schedules (
    hash VARCHAR PRIMARY KEY, source VARCHAR, fetched_at TIMESTAMPTZ,
    usable_from DATE, valid_to DATE, zip_base64 VARCHAR
  )`);
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_events (
    service_date DATE, route VARCHAR, trip_id VARCHAR, stop_sequence INTEGER,
    stop_id VARCHAR, scheduled_at DOUBLE, observed_from DOUBLE, observed_to DOUBLE,
    deviation_seconds DOUBLE, status VARCHAR, match_method VARCHAR, vid VARCHAR,
    schedule_hash VARCHAR, method VARCHAR, mapping_legacy_id VARCHAR,
    PRIMARY KEY(service_date, route, trip_id, stop_sequence)
  )`);
  await connection.run('ALTER TABLE otp_events ADD COLUMN IF NOT EXISTS mapping_legacy_id VARCHAR');
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_backfill_days (
    service_date DATE PRIMARY KEY, schedule_hash VARCHAR, applied_revision VARCHAR,
    last_completed_at TIMESTAMPTZ
  )`);
  await connection.run(`CREATE TABLE IF NOT EXISTS otp_coverage (
    service_date DATE, route VARCHAR, scheduled_timepoints INTEGER,
    observed_timepoints INTEGER, classified_timepoints INTEGER,
    observed_trips INTEGER, matched_trips INTEGER, block_matched_trips INTEGER,
    schedule_hash VARCHAR, method VARCHAR, updated_at TIMESTAMPTZ,
    PRIMARY KEY(service_date, route)
  )`);
}

async function rows<T>(connection: DuckDBConnection, sql: string): Promise<T[]> {
  return (await connection.runAndReadAll(sql)).getRowObjectsJson() as unknown as T[];
}
export async function saveSchedule(connection: DuckDBConnection, bytes: Uint8Array, source: string, usableFrom?: string): Promise<Schedule> {
  const schedule = readSchedule(bytes);
  // Automatic collection never assumes that today's download was in force in
  // the past. Historical backfills require an explicitly supplied archive/date.
  const today = localDay(Date.now() / 1000, schedule.timezone);
  const start = usableFrom ?? (today > schedule.start ? today : schedule.start);
  if (start < schedule.start || start > schedule.end) throw new Error('Schedule is not valid for the requested start date');
  await connection.run(`INSERT INTO otp_schedules VALUES (
    ${quote(schedule.hash)}, ${quote(source)}, now(), ${quote(start)}, ${quote(schedule.end)},
    ${quote(Buffer.from(bytes).toString('base64'))}
  ) ON CONFLICT(hash) DO UPDATE SET usable_from = LEAST(otp_schedules.usable_from, excluded.usable_from)`);
  return schedule;
}

export async function refreshSchedule(connection: DuckDBConnection): Promise<void> {
  const recent = await rows<{ fresh: boolean }>(connection,
    `SELECT count(*) > 0 AS fresh FROM otp_schedules WHERE fetched_at > now() - INTERVAL '24 hours'`);
  if (recent[0].fresh) return;
  const response = await fetch(process.env.GTFS_URL || GTFS_URL, { signal: AbortSignal.timeout(60000) });
  if (!response.ok) throw new Error(`GTFS download failed: HTTP ${response.status}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  const schedule = await saveSchedule(connection, bytes, process.env.GTFS_URL || GTFS_URL);
  // A successful re-fetch validates freshness, but does not change usable_from.
  await connection.run(`UPDATE otp_schedules SET fetched_at = now() WHERE hash = ${quote(schedule.hash)}`);
}

export async function calculateDay(connection: DuckDBConnection, day: string, schedule: Schedule, asOf = Date.now() / 1000): Promise<void> {
  if (day < schedule.start || day > schedule.end) throw new Error(`Schedule does not cover ${day}`);
  const mapping = await loadCrosswalk(connection, schedule, day);
  const sequences = await loadSequenceMappings(connection, schedule, day);
  const raw = await rows<Record<string, unknown>>(connection, `SELECT DISTINCT
    vid, trip_id AS legacy_id, gtfs_trip_id,
    route, tablockid AS block, destination, lat, lon, is_off_route,
    CAST(timestamp AS VARCHAR) AS wall_time, CAST(observed_at AS VARCHAR) AS observed_at
    FROM transit_data
    WHERE timestamp >= ${quote(addDays(day, -1))}::DATE
      AND timestamp < ${quote(addDays(day, 2))}::DATE
      AND route NOT IN ('U', 'PO', 'PI') AND COALESCE(gtfs_trip_id, trip_id) IS NOT NULL
    `);
  const observations: Observation[] = [];
  for (const r of raw) {
    // DuckDB formats TIMESTAMPTZ UTC offsets as +00; normalize to ISO +00:00.
    const value = r.observed_at ? String(r.observed_at).replace(/([+-]\d\d)$/, '$1:00') : String(r.wall_time);
    const at = observationEpoch(value, schedule.timezone);
    if (at === null || r.lat == null || r.lon == null) continue;
    const block = r.block == null ? null : String(r.block);
    const destination = r.destination == null ? null : String(r.destination);
    const mapped = r.gtfs_trip_id == null ? mappedTrip(mapping, String(r.route), String(r.legacy_id), block, destination) : undefined;
    const inferred = r.gtfs_trip_id == null && !mapped ? mappedTrip(sequences, String(r.route), String(r.legacy_id), block, destination) : undefined;
    observations.push({ vid: String(r.vid), trip_id: r.gtfs_trip_id != null ? String(r.gtfs_trip_id) : mapped?.id ?? inferred?.id ?? String(r.legacy_id), route: String(r.route),
      exact_id: r.gtfs_trip_id != null || mapped !== undefined || inferred !== undefined,
      id_source: inferred ? 'sequence' : mapped ? 'crosswalk' : 'trip_id', legacy_trip_id: String(r.legacy_id), block, destination,
      lat: Number(r.lat), lon: Number(r.lon), off_route: r.is_off_route === true, at });
  }
  const { events, coverage } = estimateOtp(schedule, day, observations, asOf);
  // Atomic replacement is idempotent, including corrected schedules and newly
  // arrived pings. A partial/failed run must never erase the previous result.
  await connection.run('BEGIN TRANSACTION');
  try {
    await connection.run(`DELETE FROM otp_events WHERE service_date = ${quote(day)}::DATE`);
    await connection.run(`DELETE FROM otp_coverage WHERE service_date = ${quote(day)}::DATE`);
    for (let i = 0; i < events.length; i += 1000) {
      const values = events.slice(i, i + 1000).map(e => `(${[
        quote(e.service_date), quote(e.route), quote(e.trip_id), e.stop_sequence,
        quote(e.stop_id), e.scheduled_at, e.observed_from, e.observed_to,
        e.deviation_seconds, quote(e.status), quote(e.match_method), quote(e.vid),
        quote(schedule.hash), quote(OTP_METHOD),
        e.mapping_legacy_id == null ? 'NULL' : quote(e.mapping_legacy_id),
      ].join(',')})`).join(',');
      await connection.run(`INSERT INTO otp_events VALUES ${values}`);
    }
    if (coverage.length) {
      await connection.run(`INSERT INTO otp_coverage VALUES ${coverage.map(c => `(${[
        quote(c.service_date), quote(c.route), c.scheduled_timepoints, c.observed_timepoints,
        c.classified_timepoints, c.observed_trips, c.matched_trips, c.block_matched_trips,
        quote(schedule.hash), quote(OTP_METHOD), 'now()',
      ].join(',')})`).join(',')}`);
    }
    await connection.run('COMMIT');
  } catch (err) { await connection.run('ROLLBACK'); throw err; }
  const reported = events.filter(e => e.status !== 'uncertain' && e.match_method !== 'block').length;
  console.log(`[OTP] ${day}: ${events.length} observed timepoints; ${reported} events classified for dashboard OTP (${events.filter(e => e.status !== 'uncertain' && e.match_method === 'sequence').length} use historical sequence inference)`);
}

export async function processRecentDays(connection: DuckDBConnection): Promise<void> {
  const snapshots = await rows<{ zip_base64: string; usable_from: string; valid_to: string; hash: string }>(connection,
    `SELECT zip_base64, CAST(usable_from AS VARCHAR) AS usable_from,
      CAST(valid_to AS VARCHAR) AS valid_to, hash FROM otp_schedules ORDER BY usable_from DESC, fetched_at DESC`);
  const parsed = new Map<string, Schedule>();
  const today = localDay(Date.now() / 1000, 'America/Chicago');
  for (let offset = -2; offset <= 0; offset++) {
    const day = addDays(today, offset);
    const snapshot = snapshots.find(s => s.usable_from <= day && s.valid_to >= day);
    if (!snapshot) continue;
    const schedule = parsed.get(snapshot.hash) ?? readSchedule(Buffer.from(snapshot.zip_base64, 'base64'));
    parsed.set(snapshot.hash, schedule);
    await learnCrosswalk(connection, schedule, day);
    await validateSequences(connection, schedule, day);
    await calculateDay(connection, day, schedule);
  }
  // Only explicitly requested historical dates are revisited. Bound each hourly
  // run to two dates and rotate oldest results first as the mapping grows.
  for (const snapshot of snapshots) {
    const revision = await crosswalkRevision(connection, snapshot.hash);
    const pending = await rows<{ day: string }>(connection, `SELECT CAST(service_date AS VARCHAR) AS day
      FROM otp_backfill_days WHERE schedule_hash = ${quote(snapshot.hash)}
        AND (applied_revision IS NULL OR applied_revision <> ${quote(revision)})
      ORDER BY last_completed_at ASC NULLS FIRST, service_date LIMIT 2`);
    if (!pending.length) continue;
    const schedule = parsed.get(snapshot.hash) ?? readSchedule(Buffer.from(snapshot.zip_base64, 'base64'));
    for (const { day } of pending) {
      await calculateDay(connection, day, schedule);
      await finishBackfillDay(connection, day, revision);
    }
    break;
  }
}

async function crosswalkRevision(connection: DuckDBConnection, hash: string): Promise<string> {
  const result = await rows<{ revision: string }>(connection, `SELECT md5(COALESCE(string_agg(
    route || '|' || legacy_id || '|' || gtfs_id, ',' ORDER BY route, legacy_id, gtfs_id), '')) AS revision
    FROM (SELECT route, legacy_id, gtfs_id FROM otp_trip_mappings WHERE schedule_hash = ${quote(hash)}
      UNION ALL SELECT route, legacy_id, gtfs_id || ':' || evidence_count AS gtfs_id
      FROM otp_sequence_mappings WHERE schedule_hash = ${quote(hash)}) mappings`);
  return `${OTP_METHOD}:${result[0].revision}`;
}
async function finishBackfillDay(connection: DuckDBConnection, day: string, revision: string): Promise<void> {
  await connection.run(`UPDATE otp_backfill_days SET applied_revision = ${quote(revision)},
    last_completed_at = now() WHERE service_date = ${quote(day)}::DATE`);
}

// Expand an existing backfill using all paired observations on an evidence day.
// Restrict work to requested dates containing an eligible mapped legacy ID;
// unmapped service patterns stay queued for the normal hourly worker.
export async function refreshMappedBackfill(connection: DuckDBConnection, schedule: Schedule, evidenceDay: string): Promise<string[]> {
  await learnCrosswalk(connection, schedule, evidenceDay);
  const revision = await crosswalkRevision(connection, schedule.hash);
  const candidates = await rows<{ day: string; gtfs_id: string }>(connection, `WITH mappings AS (
    SELECT route, legacy_id, MIN(gtfs_id) AS gtfs_id FROM otp_trip_mappings
    WHERE schedule_hash = ${quote(schedule.hash)} GROUP BY route, legacy_id, service_id
    HAVING COUNT(DISTINCT gtfs_id) = 1
  ) SELECT DISTINCT CAST(b.service_date AS VARCHAR) AS day, m.gtfs_id
    FROM otp_backfill_days b JOIN transit_data t
      ON t.timestamp >= b.service_date AND t.timestamp < b.service_date + INTERVAL '1 day'
    JOIN mappings m ON t.route = m.route AND t.trip_id = m.legacy_id
    WHERE b.schedule_hash = ${quote(schedule.hash)}
      AND (b.applied_revision IS NULL OR b.applied_revision <> ${quote(revision)})`);
  const trips = new Map(schedule.trips.map(t => [t.id, t]));
  const services = new Map<string, Set<string>>();
  const days = [...new Set(candidates.filter(r => {
    const active = services.get(r.day) ?? activeServices(schedule, r.day);
    services.set(r.day, active);
    const trip = trips.get(r.gtfs_id);
    return trip && active.has(trip.service);
  }).map(r => r.day))].sort();
  console.log(`[OTP] Refreshing ${days.length} requested dates with observed trip mappings`);
  for (const day of days) {
    await calculateDay(connection, day, schedule);
    await finishBackfillDay(connection, day, revision);
  }
  return days;
}

export async function openOtpDatabase(): Promise<{ instance: DuckDBInstance; connection: DuckDBConnection }> {
  const token = process.env.MOTHER_DUCK_API_KEY;
  if (!token) throw new Error('Missing MOTHER_DUCK_API_KEY');
  const database = process.env.MOTHERDUCK_DATABASE || 'my_db';
  const instance = await DuckDBInstance.create(`md:${database}?motherduck_token=${token}`);
  const connection = await instance.connect();
  try {
    await initializeOtp(connection);
    // Backfills also work before restarting the scraper with the new schema.
    await connection.run('ALTER TABLE transit_data ADD COLUMN IF NOT EXISTS observed_at TIMESTAMPTZ');
    await connection.run('ALTER TABLE transit_data ADD COLUMN IF NOT EXISTS gtfs_trip_id VARCHAR');
  } catch (err) { connection.closeSync(); instance.closeSync(); throw err; }
  return { instance, connection };
}

// Called by the collector. Dedicated connection; serialized runs; retries each
// hour. A GTFS/network error never stops raw observation collection.
export function startOtpWorker(): () => void {
  let child: ChildProcess | undefined, stopped = false;
  const run = () => {
    if (child || stopped) return;
    // Parsing GTFS and matching GPS are CPU-intensive: keep them off the SSE
    // collector's event loop so calculations cannot interrupt raw collection.
    child = fork(fileURLToPath(import.meta.url), ['--recent'], { execArgv: ['--import', 'tsx'], stdio: 'inherit' });
    child.on('error', err => { console.error('[OTP] Worker failed:', String(err)); });
    child.on('exit', code => {
      if (code) console.error(`[OTP] Worker exited with code ${code}; retrying next hour`);
      child = undefined;
    });
  };
  run();
  const timer = setInterval(run, 60 * 60 * 1000);
  return () => { stopped = true; clearInterval(timer); child?.kill('SIGTERM'); };
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.includes('--recent')) {
    const db = await openOtpDatabase();
    try {
      try { await refreshSchedule(db.connection); }
      catch (err) { console.error('[OTP] Schedule refresh failed; using valid archived schedules:', String(err)); }
      await processRecentDays(db.connection);
    } finally { db.connection.closeSync(); db.instance.closeSync(); }
    return;
  }
  const get = (flag: string) => { const i = args.indexOf(flag); return i < 0 ? undefined : args[i + 1]; };
  const file = get('--gtfs'), from = get('--from'), to = get('--to'), crosswalk = get('--crosswalk');
  const evidenceDay = get('--refresh-mappings');
  if (file && evidenceDay) {
    if (addDays(evidenceDay, 0) !== evidenceDay) throw new Error('Invalid evidence date');
    const schedule = readSchedule(new Uint8Array(await readFile(file)));
    if (evidenceDay < schedule.start || evidenceDay > schedule.end) throw new Error('Evidence date outside archive validity');
    const db = await openOtpDatabase();
    try { await refreshMappedBackfill(db.connection, schedule, evidenceDay); }
    finally { db.connection.closeSync(); db.instance.closeSync(); }
    return;
  }
  if (!file || !from || !to) throw new Error('Usage: npm run otp:backfill -- --gtfs archive.zip --from YYYY-MM-DD --to YYYY-MM-DD');
  if (addDays(from, 0) !== from || addDays(to, 0) !== to || from > to) throw new Error('Invalid date range');
  const bytes = new Uint8Array(await readFile(file));
  const schedule = readSchedule(bytes);
  if (from < schedule.start || to > schedule.end) throw new Error('Archive validity does not cover requested dates');
  const db = await openOtpDatabase();
  try {
    await saveSchedule(db.connection, bytes, file, from);
    if (crosswalk) await importCrosswalk(db.connection, schedule, JSON.parse(await readFile(crosswalk, 'utf8')));
    if (get('--sequence-from') || get('--sequence-to')) {
      if (!get('--sequence-from') || !get('--sequence-to')) throw new Error('Both sequence evidence dates are required');
      const count = await rebuildSequenceMappings(db.connection, schedule, get('--sequence-from')!, get('--sequence-to')!);
      console.log(`[OTP] ${count} recurring sequence mappings inferred; provenance retained separately`);
    }
    const revision = await crosswalkRevision(db.connection, schedule.hash);
    const days: string[] = [];
    for (let day = from; day <= to; day = addDays(day, 1)) days.push(day);
    // Persist the entire request before starting, so an interrupted run can be
    // completed by the existing worker without losing the unvisited dates.
    await db.connection.run(`INSERT INTO otp_backfill_days VALUES ${days.map(day =>
      `(${quote(day)}, ${quote(schedule.hash)}, NULL, NULL)`).join(',')}
      ON CONFLICT(service_date) DO UPDATE SET schedule_hash = excluded.schedule_hash, applied_revision = NULL`);
    for (const day of days) {
      await calculateDay(db.connection, day, schedule);
      await finishBackfillDay(db.connection, day, revision);
    }
  } finally { db.connection.closeSync(); db.instance.closeSync(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(err => { console.error(String(err)); process.exitCode = 1; });
}
