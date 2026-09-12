import { createHash } from 'node:crypto';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { initializeTransitSchema } from './motherduck';
import { initializeOtp } from './otp-worker';
import { processVehicle } from './vehicle';
import type { RawVehicle, TransitRecord } from './types';
import type { CollectionBatch, StudyObservation } from './observation-types';
import { localDay } from './otp';

export const sql = (v: unknown): string => v == null ? 'NULL' : typeof v === 'number' ? (Number.isFinite(v) ? String(v) : 'NULL') : typeof v === 'boolean' ? String(v) : `'${String(v).replace(/'/g, "''")}'`;
export const ident = (v: string): string => `"${v.replace(/"/g, '""')}"`;
export async function query<T = Record<string, unknown>>(c: DuckDBConnection, statement: string): Promise<T[]> {
  const values = (await c.runAndReadAll(statement)).getRowObjectsJS();
  // The driver's JSON accessor encodes BIGINT counts as strings. Convert the
  // typed values instead, without coercing string vehicle/route identifiers.
  return JSON.parse(JSON.stringify(values, (_key,value) => typeof value==='bigint'
    ? Number.isSafeInteger(Number(value))?Number(value):value.toString():value)) as T[];
}
const LEGACY_COLUMNS = ['vid','timestamp','lat','lon','heading','route','trip_id','destination','speed','is_delayed','is_off_route','segment_id','segment_name','segment_type','pdist','pid','rid','tablockid','srvtmstmp','observed_at','gtfs_trip_id'];
export function legacyValues(r: TransitRecord): string {
  const row = { ...r, observed_at: /(Z|[+-]\d\d:\d\d)$/.test(r.timestamp) ? r.timestamp : null };
  return `(${LEGACY_COLUMNS.map(k => sql(row[k as keyof typeof row])).join(',')})`;
}

export async function openLocalStore(directory: string) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const threads = process.env.LOCAL_DB_THREADS || '2';
  if (!/^\d+$/.test(threads) || Number(threads) < 1 || Number(threads) > 256) throw new Error('LOCAL_DB_THREADS must be between 1 and 256');
  const db = await DuckDBInstance.create(join(directory, 'transit.duckdb'), { threads, memory_limit: process.env.LOCAL_DB_MEMORY || '1GB' });
  const c = await db.connect();
  await initializeTransitSchema(c, 'transit');
  await initializeOtp(c);
  await c.run(`CREATE TABLE IF NOT EXISTS collection_payloads (
    payload_id VARCHAR PRIMARY KEY, source VARCHAR, vehicle_id VARCHAR, route_id VARCHAR,
    observed_at DOUBLE, body JSON, raw JSON
  )`);
  await c.run(`CREATE TABLE IF NOT EXISTS collection_receipts (
    observation_id VARCHAR PRIMARY KEY, payload_id VARCHAR, received_at DOUBLE,
    service_date DATE, batch_id VARCHAR
  )`);
  await c.run('CREATE TABLE IF NOT EXISTS collection_batches (batch_id VARCHAR PRIMARY KEY, source VARCHAR, received_at TIMESTAMPTZ, observations INTEGER, predictions JSON, provenance JSON)');
  await c.run('CREATE TABLE IF NOT EXISTS local_watermarks (vid VARCHAR PRIMARY KEY, observed_at TIMESTAMPTZ)');
  await c.run('CREATE TABLE IF NOT EXISTS local_state (key VARCHAR PRIMARY KEY, value JSON, updated_at TIMESTAMPTZ)');
  await c.run('CREATE TABLE IF NOT EXISTS study_events (id VARCHAR PRIMARY KEY, date DATE, kind VARCHAR, source VARCHAR, body JSON)');
  await c.run('CREATE TABLE IF NOT EXISTS study_dates (date DATE PRIMARY KEY, source_revision VARCHAR, method_revision VARCHAR, updated_at TIMESTAMPTZ)');
  await c.run('CREATE TABLE IF NOT EXISTS study_daily_results (date DATE PRIMARY KEY,method_revision VARCHAR,source_revision VARCHAR,body JSON,updated_at TIMESTAMPTZ)');
  await c.run('CREATE TABLE IF NOT EXISTS study_network_catalogs (version VARCHAR PRIMARY KEY,body JSON)');
  await c.run('CREATE TABLE IF NOT EXISTS cloud_batches (id VARCHAR PRIMARY KEY, first_at DOUBLE, last_at DOUBLE, rows BIGINT, uploaded_at TIMESTAMPTZ)');
  await c.run('CREATE TABLE IF NOT EXISTS local_archive_catalog (date DATE PRIMARY KEY,path VARCHAR,rows BIGINT,last_received_at DOUBLE,sha256 VARCHAR,verified_at TIMESTAMPTZ)');
  await c.run(`CREATE OR REPLACE VIEW collected_observations AS SELECT r.observation_id,r.received_at,r.service_date,r.batch_id,p.* EXCLUDE(payload_id),r.payload_id FROM collection_receipts r JOIN collection_payloads p USING(payload_id)`);
  return { db, c };
}

export async function ingestBatch(c: DuckDBConnection, batch: CollectionBatch): Promise<boolean> {
  if ((await query(c, `SELECT 1 FROM collection_batches WHERE batch_id=${sql(batch.batch_id)}`)).length) return false;
  const payloads = new Map<string, string>(), receipts: string[] = [], rawRows: TransitRecord[] = [];
  const watermarks = new Map((await query<{vid:string;instant:number}>(c, 'SELECT vid,epoch(observed_at) AS instant FROM local_watermarks')).map(r => [r.vid, r.instant]));
  const updates = new Map<string, number>();
  for (const entry of batch.observations) {
    const { observation_id, received_at, ...body } = entry.observation;
    if (!observation_id || !Number.isFinite(received_at) || body.source !== batch.source) throw new Error('Invalid normalized observation');
    const raw = JSON.stringify(entry.raw), serialized = JSON.stringify(body);
    const key = createHash('sha256').update(serialized).update('\0').update(raw).digest('hex');
    payloads.set(key, `(${[key,body.source,body.vehicle_id,body.route_id,body.observed_at,serialized,raw].map(sql).join(',')})`);
    receipts.push(`(${[observation_id,key,received_at,localDay(received_at,'America/Chicago'),batch.batch_id].map(sql).join(',')})`);
    if (batch.source === 'sse') {
      const r = processVehicle(entry.raw as unknown as RawVehicle);
      if (r) {
        const at = Date.parse(r.timestamp) / 1000;
        if (at > (watermarks.get(r.vid) ?? -Infinity)) { rawRows.push(r); watermarks.set(r.vid,at); updates.set(r.vid,at); }
      }
    }
  }
  await c.run('BEGIN');
  try {
    for (const [table,values] of [['collection_payloads',[...payloads.values()]],['collection_receipts',receipts]] as const) {
      for (let i=0;i<values.length;i+=2000) await c.run(`INSERT INTO ${table} VALUES ${values.slice(i,i+2000).join(',')} ON CONFLICT DO NOTHING`);
    }
    if (rawRows.length) await c.run(`INSERT INTO transit_data (${LEGACY_COLUMNS.join(',')}) VALUES ${rawRows.map(legacyValues).join(',')}`);
    if (updates.size) await c.run(`INSERT INTO local_watermarks VALUES ${[...updates].map(([vid,at])=>`(${sql(vid)},to_timestamp(${at}))`).join(',')} ON CONFLICT(vid) DO UPDATE SET observed_at=excluded.observed_at`);
    await c.run(`INSERT INTO collection_batches VALUES (${[batch.batch_id,batch.source,batch.received_at,batch.observations.length,JSON.stringify(batch.predictions??[]),JSON.stringify(batch.provenance??{})].map(sql).join(',')})`);
    await c.run('COMMIT');
  } catch(e) { await c.run('ROLLBACK'); throw e; }
  return true;
}

export async function loadObservations(c: DuckDBConnection, date: string, route?: string): Promise<StudyObservation[]> {
  const saved=await query<{path:string}>(c,`SELECT path FROM local_archive_catalog WHERE date=${sql(date)}::DATE`);
  const columns="observation_id,received_at,body,raw->>'des' AS destination";
  const archive=saved.length?` UNION ALL SELECT ${columns} FROM read_parquet(${sql(saved[0].path)}) WHERE observation_id NOT IN (SELECT observation_id FROM collection_receipts WHERE service_date=${sql(date)}::DATE) ${route?`AND route_id=${sql(route)}`:''}`:'';
  const rows = await query<{observation_id:string;received_at:number;body:string|object;destination:string|null}>(c, `SELECT * FROM (SELECT ${columns} FROM collected_observations WHERE service_date=${sql(date)}::DATE ${route?`AND route_id=${sql(route)}`:''}${archive}) ORDER BY received_at,observation_id`);
  return rows.map(r => ({...(typeof r.body==='string'?JSON.parse(r.body):r.body),observation_id:r.observation_id,received_at:r.received_at,...(r.destination&&/not\s+in\s+service/i.test(r.destination)?{in_service:false}:{})} as StudyObservation));
}
export async function state<T>(c: DuckDBConnection, key: string): Promise<T | undefined> {
  const r = await query<{value:string}>(c,`SELECT value::VARCHAR AS value FROM local_state WHERE key=${sql(key)}`);
  return r.length?JSON.parse(r[0].value) as T:undefined;
}
export async function setState(c: DuckDBConnection, key: string, value: unknown) {
  await c.run(`INSERT INTO local_state VALUES (${sql(key)},${sql(JSON.stringify(value))},now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`);
}
