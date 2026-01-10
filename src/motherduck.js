import duckdb from 'duckdb';

const MOTHERDUCK_TOKEN = process.env.MOTHERDUCK_TOKEN;
const DATABASE_NAME = process.env.MOTHERDUCK_DATABASE || 'nola_transit';

let db;
let connection;

// Initialize MotherDuck connection
export async function initMotherDuck() {
  return new Promise((resolve, reject) => {
    db = new duckdb.Database(':md:', (err) => {
      if (err) return reject(err);

      db.run(`SET motherduck_token='${MOTHERDUCK_TOKEN}'`, (err) => {
        if (err) return reject(err);

        // Create database if it doesn't exist
        db.run(`CREATE DATABASE IF NOT EXISTS ${DATABASE_NAME}`, (err) => {
          if (err) return reject(err);

          db.run(`USE ${DATABASE_NAME}`, (err) => {
            if (err) return reject(err);

            // Create table if it doesn't exist
            db.run(`
              CREATE TABLE IF NOT EXISTS transit_data (
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
    });
  });
}

// Batch insert records
export async function insertRecords(records) {
  if (!db) throw new Error('MotherDuck not initialized');

  return new Promise((resolve, reject) => {
    const appender = db.prepare(`
      INSERT INTO transit_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    for (const record of records) {
      appender.run([
        record.vid,
        record.timestamp,
        record.lat,
        record.lon,
        record.heading,
        record.route,
        record.trip_id,
        record.destination,
        record.speed,
        record.is_delayed,
        record.is_off_route,
        record.segment_id,
        record.segment_name,
        record.segment_type
      ]);
    }

    appender.finalize((err) => {
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
