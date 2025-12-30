import 'dotenv/config';
import duckdb from 'duckdb';

// Configure R2 credentials for DuckDB
const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_BUCKET = process.env.R2_BUCKET || 'nola-transit';

const DATA_PATH = `s3://${R2_BUCKET}/**/*.parquet`;

async function query(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      else resolve(rows);
    });
  });
}

async function run(db, sql) {
  return new Promise((resolve, reject) => {
    db.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function main() {
  console.log('Connecting to DuckDB...\n');

  const db = new duckdb.Database(':memory:');

  // Configure S3/R2 credentials
  await run(db, `INSTALL httpfs`);
  await run(db, `LOAD httpfs`);
  await run(db, `SET s3_region='auto'`);
  await run(db, `SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com'`);
  await run(db, `SET s3_access_key_id='${R2_ACCESS_KEY_ID}'`);
  await run(db, `SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}'`);
  await run(db, `SET s3_url_style='path'`);

  console.log('========================================');
  console.log('NOLA STREETCAR DELAY ANALYSIS');
  console.log('========================================\n');

  // Query 1: Mixed Traffic vs Dedicated ROW
  console.log('📊 DELAYS BY SEGMENT TYPE');
  console.log('─'.repeat(60));

  const segmentTypeRows = await query(db, `
    SELECT
      segment_type,
      COUNT(*) as readings,
      SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
      ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
      ROUND(AVG(speed), 1) as avg_speed
    FROM read_parquet('${DATA_PATH}')
    WHERE route = '12' AND segment_type IS NOT NULL
    GROUP BY segment_type
    ORDER BY delay_pct DESC
  `);
  console.table(segmentTypeRows);

  // Query 2: By individual segment
  console.log('\n📍 DELAYS BY SEGMENT');
  console.log('─'.repeat(60));

  const segmentRows = await query(db, `
    SELECT
      segment_name,
      segment_type,
      COUNT(*) as readings,
      ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
      ROUND(AVG(speed), 1) as avg_speed
    FROM read_parquet('${DATA_PATH}')
    WHERE route = '12' AND segment_name IS NOT NULL
    GROUP BY segment_name, segment_type
    ORDER BY delay_pct DESC
  `);
  console.table(segmentRows);

  // Query 3: Total data collected
  console.log('\n📈 DATA SUMMARY');
  console.log('─'.repeat(60));

  const summaryRows = await query(db, `
    SELECT
      COUNT(*) as total_records,
      COUNT(DISTINCT route) as routes,
      MIN(timestamp) as first_record,
      MAX(timestamp) as last_record
    FROM read_parquet('${DATA_PATH}')
  `);
  console.table(summaryRows);

  db.close();
}

main().catch(err => {
  console.error('Query failed:', err);
  process.exit(1);
});
