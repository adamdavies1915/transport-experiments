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

  // Create table if it doesn't exist
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
      segment_type VARCHAR
    )
  `);

  console.log('MotherDuck initialized successfully');
}

// Escape a string value for inline SQL, or return NULL for nullish input.
function sqlString(value: string | null | undefined): string {
  return value == null ? 'NULL' : `'${value.replace(/'/g, "''")}'`;
}

// Batch insert records using bulk INSERT
export async function insertRecords(records: TransitRecord[]): Promise<void> {
  if (!connection) throw new Error('MotherDuck not initialized');

  // Build bulk INSERT statement
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
      sqlString(r.segment_type)
    ].join(', ')})`
  ).join(',\n');

  const sql = `INSERT INTO ${DATABASE_NAME}.transit_data VALUES\n${values}`;

  await connection.run(sql);
  console.log(`Inserted ${records.length} records into MotherDuck`);
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
