import { randomUUID } from 'node:crypto';
import { mkdir, realpath, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { atomicFile } from '../src/local-journal';
import { query, sql } from '../src/local-store';
import { safeError } from '../src/log-safety';
import { importServerHistory } from '../src/server-history-import';

export interface ServerHistoryOtpQueue {
  queued: { service_date: string; schedule_hash: string }[];
  no_covering_schedule: string[];
}

/** Revisit only explicitly imported dates against retained, historically usable
 * schedules. Caller holds the only database writer; no schedule is downloaded. */
export async function queueServerHistoryOtp(connection: DuckDBConnection, studyDates: readonly string[]): Promise<ServerHistoryOtpQueue> {
  const dates = [...new Set(studyDates)].sort();
  for (const date of dates) {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(date)) || new Date(date).toISOString().slice(0, 10) !== date)
      throw new Error('Imported OTP dates must be valid YYYY-MM-DD dates');
  }
  const schedules = await query<{ hash: string; usable_from: string; valid_to: string }>(connection,
    `SELECT hash,usable_from::VARCHAR AS usable_from,valid_to::VARCHAR AS valid_to FROM otp_schedules
      WHERE usable_from IS NOT NULL AND valid_to IS NOT NULL
      ORDER BY usable_from DESC,fetched_at DESC NULLS LAST,hash`);
  const result: ServerHistoryOtpQueue = { queued: [], no_covering_schedule: [] };
  for (const date of dates) {
    const schedule = schedules.find(s => s.usable_from <= date && s.valid_to >= date);
    if (schedule) result.queued.push({ service_date: date, schedule_hash: schedule.hash });
    else result.no_covering_schedule.push(date);
  }
  if (result.queued.length) {
    await connection.run('BEGIN TRANSACTION');
    try {
      await connection.run(`INSERT INTO otp_backfill_days (service_date,schedule_hash,applied_revision,last_completed_at)
        VALUES ${result.queued.map(r => `(${sql(r.service_date)},${sql(r.schedule_hash)},NULL,NULL)`).join(',')}
        ON CONFLICT(service_date) DO UPDATE SET schedule_hash=excluded.schedule_hash,applied_revision=NULL`);
      await connection.run('COMMIT');
    } catch (error) { await connection.run('ROLLBACK'); throw error; }
  }
  return result;
}

const usage = `Usage: npm run history:import -- --data-dir DATA_DIR --transit-data RAW.parquet --streetcar-snapshots SNAPSHOTS.parquet

Imports immutable local exports into an existing DATA_DIR/transit.duckdb.
Pause the database worker first; the collector may continue journaling.
Writes a unique atomic audit to DATA_DIR/imports. Performs no network access,
database bootstrap, or process management. LOCAL_DB_MEMORY defaults to 1GB.`;

async function main(): Promise<void> {
  const { values } = parseArgs({ options: {
    'data-dir': { type: 'string' }, 'transit-data': { type: 'string' },
    'streetcar-snapshots': { type: 'string' }, help: { type: 'boolean' },
  }, strict: true, allowPositionals: false });
  if (values.help) { console.log(usage); return; }
  if (!values['data-dir'] || !values['transit-data'] || !values['streetcar-snapshots']) throw new Error(usage);
  const directory = await realpath(values['data-dir']);
  const database = join(directory, 'transit.duckdb');
  if (!(await stat(database)).isFile()) throw new Error('DATA_DIR/transit.duckdb must be an existing local database');
  const imports = join(directory, 'imports');
  await mkdir(imports, { recursive: true, mode: 0o700 });
  const auditPath = join(imports, `server-history-${new Date().toISOString().replaceAll(':', '-')}-${randomUUID()}.json`);
  const db = await DuckDBInstance.create(database, { access_mode: 'READ_WRITE', threads: '2', memory_limit: process.env.LOCAL_DB_MEMORY || '1GB' });
  try {
    const connection = await db.connect();
    try {
      const audit = await importServerHistory(connection, {
        transitDataFile: resolve(values['transit-data']), streetcarSnapshotsFile: resolve(values['streetcar-snapshots']),
      }, progress => { console.log(JSON.stringify(progress)); });
      let otpBackfill: ServerHistoryOtpQueue | { error: string };
      try { otpBackfill = await queueServerHistoryOtp(connection, audit.transit_data.study_dates); }
      catch (error) { otpBackfill = { error: safeError(error) }; }
      try {
        await atomicFile(auditPath, JSON.stringify({ ...audit, target_database: database, otp_backfill: otpBackfill }, null, 2) + '\n');
      } catch (error) {
        throw new Error(`Import committed, but its audit could not be saved to ${auditPath}. Rerunning the import is safe. ${safeError(error)}`);
      }
      if ('error' in otpBackfill) throw new Error(`Import committed and audited at ${auditPath}, but OTP dates could not be queued. Rerunning the import is safe. ${otpBackfill.error}`);
      console.log(JSON.stringify({ status: 'committed', audit: auditPath,
        inserted: { transit_data: audit.transit_data.inserted_rows, streetcar_snapshots: audit.streetcar_snapshots.inserted_rows },
        replay: { transit_data: audit.transit_data.replay_rows, streetcar_snapshots: audit.streetcar_snapshots.replay_rows },
        otp_backfill: otpBackfill }));
    } finally { connection.closeSync(); }
  } finally { db.closeSync(); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(error => { console.error(safeError(error)); process.exitCode = 1; });
