import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import type { TransitRecord } from './types';

const MOTHER_DUCK_API_KEY = process.env.MOTHER_DUCK_API_KEY;
const DATABASE_NAME = process.env.MOTHERDUCK_DATABASE || 'my_db'; // MotherDuck default database

let instance: DuckDBInstance | undefined;
let connection: DuckDBConnection | undefined;

// Initialize MotherDuck connection
export async function initMotherDuck(): Promise<void> {
  if (!MOTHER_DUCK_API_KEY) {
    throw new Error('Missing MOTHER_DUCK_API_KEY environment variable');
  }

  // Open the MotherDuck database directly. The token is passed via the
  // connection string so no shared process env state is required.
  instance = await DuckDBInstance.create(
    `md:${DATABASE_NAME}?motherduck_token=${MOTHER_DUCK_API_KEY}`
  );
  connection = await instance.connect();

  // Create table if it doesn't exist. Column order here matches INSERT_COLUMNS
  // and the order ALTER TABLE appends new columns, so fresh and migrated
  // tables end up identical.
  await connection.run(`
    CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.transit_data (
      vid VARCHAR,
      timestamp TIMESTAMP,
      lat DOUBLE,
      lon DOUBLE,
      heading INTEGER,
      route VARCHAR,
      trip_id VARCHAR,
      destination VARCHAR,
      speed INTEGER,
      is_delayed BOOLEAN,
      is_off_route BOOLEAN,
      segment_id INTEGER,
      segment_name VARCHAR,
      segment_type VARCHAR,
      pdist INTEGER,
      pid INTEGER,
      rid VARCHAR,
      tablockid VARCHAR,
      srvtmstmp VARCHAR
    )
  `);

  // Idempotently migrate an already-existing table to add the new nullable
  // columns. Safe to run on every startup (no-op once present).
  const newColumns: Array<[string, string]> = [
    ['pdist', 'INTEGER'],
    ['pid', 'INTEGER'],
    ['rid', 'VARCHAR'],
    ['tablockid', 'VARCHAR'],
    ['srvtmstmp', 'VARCHAR'],
  ];
  for (const [name, type] of newColumns) {
    await connection.run(
      `ALTER TABLE ${DATABASE_NAME}.transit_data ADD COLUMN IF NOT EXISTS ${name} ${type}`
    );
  }

  console.log('MotherDuck initialized successfully');
}

// Explicit insert column list — keeps inserts correct regardless of the
// physical column order the live table happens to have.
const INSERT_COLUMNS = [
  'vid', 'timestamp', 'lat', 'lon', 'heading', 'route', 'trip_id',
  'destination', 'speed', 'is_delayed', 'is_off_route',
  'segment_id', 'segment_name', 'segment_type',
  'pdist', 'pid', 'rid', 'tablockid', 'srvtmstmp',
] as const;

// Escape a string value for inline SQL, or return NULL for nullish input.
function sqlString(value: string | null | undefined): string {
  return value == null ? 'NULL' : `'${value.replace(/'/g, "''")}'`;
}

// Batch insert records using bulk INSERT
export async function insertRecords(records: TransitRecord[]): Promise<void> {
  if (!connection) throw new Error('MotherDuck not initialized');

  // Build bulk INSERT statement. Value order MUST match INSERT_COLUMNS.
  const values = records.map(r =>
    `(${[
      sqlString(r.vid),
      `'${r.timestamp}'`,
      r.lat,
      r.lon,
      r.heading,
      sqlString(r.route),
      sqlString(r.trip_id),
      sqlString(r.destination),
      r.speed,
      r.is_delayed,
      r.is_off_route,
      r.segment_id ?? 'NULL',
      sqlString(r.segment_name),
      sqlString(r.segment_type),
      r.pdist ?? 'NULL',
      r.pid ?? 'NULL',
      sqlString(r.rid),
      sqlString(r.tablockid),
      sqlString(r.srvtmstmp)
    ].join(', ')})`
  ).join(',\n');

  const sql = `INSERT INTO ${DATABASE_NAME}.transit_data (${INSERT_COLUMNS.join(', ')}) VALUES\n${values}`;

  await connection.run(sql);
  console.log(`Inserted ${records.length} records into MotherDuck`);
}

// Read-only query helper for analysis scripts. Returns JS-typed row objects.
export async function runQuery(sql: string): Promise<Record<string, unknown>[]> {
  if (!connection) throw new Error('MotherDuck not initialized');
  const reader = await connection.runAndReadAll(sql);
  return reader.getRowObjectsJS();
}

export async function closeMotherDuck(): Promise<void> {
  if (connection) {
    connection.closeSync();
    connection = undefined;
  }
  if (instance) {
    instance.closeSync();
    instance = undefined;
  }
}
