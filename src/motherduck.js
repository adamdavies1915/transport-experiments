import duckdb from 'duckdb';

const MOTHER_DUCK_API_KEY = process.env.MOTHER_DUCK_API_KEY;
const DATABASE_NAME = process.env.MOTHERDUCK_DATABASE || 'my_db'; // MotherDuck default database

let db;
let connection;

// Initialize MotherDuck connection
export async function initMotherDuck() {
  return new Promise((resolve, reject) => {
    // Set the token as env var - DuckDB reads it automatically with :md: connection
    process.env.motherduck_token = MOTHER_DUCK_API_KEY;

    db = new duckdb.Database(':md:', (err) => {
      if (err) return reject(err);

      // Attach to MotherDuck database (creates if doesn't exist)
      db.run(`ATTACH 'md:${DATABASE_NAME}' AS ${DATABASE_NAME}`, (err) => {
        if (err) {
          console.log('Database might already be attached, trying to use it directly...');
        }

        // Create table if it doesn't exist
        db.run(`
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
        `, (err) => {
          if (err) return reject(err);
          console.log('MotherDuck initialized successfully');
          resolve();
        });
      });
    });
  });
}

// Batch insert records using bulk INSERT
export async function insertRecords(records) {
  if (!db) throw new Error('MotherDuck not initialized');

  return new Promise((resolve, reject) => {
    // Build bulk INSERT statement
    const values = records.map(r =>
      `(${[
        `'${r.vid.replace(/'/g, "''")}'`,
        `'${r.timestamp}'`,
        r.lat,
        r.lon,
        r.heading,
        `'${r.route.replace(/'/g, "''")}'`,
        r.trip_id ? `'${r.trip_id.replace(/'/g, "''")}'` : 'NULL',
        r.destination ? `'${r.destination.replace(/'/g, "''")}'` : 'NULL',
        r.speed,
        r.is_delayed,
        r.is_off_route,
        r.segment_id || 'NULL',
        r.segment_name ? `'${r.segment_name.replace(/'/g, "''")}'` : 'NULL',
        r.segment_type ? `'${r.segment_type.replace(/'/g, "''")}'` : 'NULL'
      ].join(', ')})`
    ).join(',\n');

    const sql = `INSERT INTO ${DATABASE_NAME}.transit_data VALUES\n${values}`;

    db.run(sql, (err) => {
      if (err) return reject(err);
      console.log(`Inserted ${records.length} records into MotherDuck`);
      resolve();
    });
  });
}

export async function closeMotherDuck() {
  if (db) {
    return new Promise((resolve) => {
      db.close(() => resolve());
    });
  }
}
