import dotenv from 'dotenv';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Load .env from parent directory
dotenv.config({ path: join(__dirname, '..', '.env') });
import cron from 'node-cron';
import duckdb from 'duckdb';
import pg from 'pg';

const { Pool } = pg;

// MotherDuck config
const MOTHER_DUCK_API_KEY = process.env.MOTHER_DUCK_API_KEY;
const MOTHERDUCK_DB = process.env.MOTHERDUCK_DATABASE || 'my_db';

// Postgres config
const pgPool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  database: process.env.POSTGRES_DB || 'transit',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : false
});

// MotherDuck connection
let mdDb;

function queryMotherDuck(sql) {
  return new Promise((resolve, reject) => {
    mdDb.all(sql, (err, rows) => {
      if (err) reject(err);
      else {
        const converted = rows.map(row => {
          const obj = {};
          for (const [key, value] of Object.entries(row)) {
            obj[key] = typeof value === 'bigint' ? Number(value) : value;
          }
          return obj;
        });
        resolve(converted);
      }
    });
  });
}

function runMotherDuck(sql) {
  return new Promise((resolve, reject) => {
    mdDb.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

async function initMotherDuck() {
  return new Promise((resolve, reject) => {
    process.env.motherduck_token = MOTHER_DUCK_API_KEY;

    mdDb = new duckdb.Database(':md:', (err) => {
      if (err) return reject(err);

      mdDb.run(`ATTACH 'md:${MOTHERDUCK_DB}' AS ${MOTHERDUCK_DB}`, (err) => {
        if (err) {
          console.log('Database might already be attached');
        }
        console.log('MotherDuck connected');
        resolve();
      });
    });
  });
}

async function consolidate() {
  console.log(`\n[${new Date().toISOString()}] Starting consolidation...`);

  try {
    // Test connections
    await pgPool.query('SELECT NOW()');
    console.log('✓ Postgres connected');

    // Get date range of data
    const dateRange = await queryMotherDuck(`
      SELECT
        MIN(DATE_TRUNC('day', timestamp)) as min_date,
        MAX(DATE_TRUNC('day', timestamp)) as max_date,
        COUNT(*) as total_records
      FROM ${MOTHERDUCK_DB}.transit_data
    `);

    if (!dateRange[0] || dateRange[0].total_records === 0) {
      console.log('No data to consolidate yet');
      return;
    }

    console.log(`Data range: ${dateRange[0].min_date} to ${dateRange[0].max_date}`);
    console.log(`Total records: ${dateRange[0].total_records.toLocaleString()}`);

    // 1. Daily summary
    console.log('\n1. Computing daily summaries...');
    const dailySummaries = await queryMotherDuck(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as total_records,
        COUNT(DISTINCT route) as total_routes,
        COUNT(DISTINCT vid) as total_vehicles,
        ROUND(AVG(speed), 2) as avg_speed,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_count,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct
      FROM ${MOTHERDUCK_DB}.transit_data
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date
    `);

    for (const row of dailySummaries) {
      await pgPool.query(`
        INSERT INTO daily_summary (date, total_records, total_routes, total_vehicles, avg_speed, delayed_count, on_time_pct)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (date) DO UPDATE SET
          total_records = $2,
          total_routes = $3,
          total_vehicles = $4,
          avg_speed = $5,
          delayed_count = $6,
          on_time_pct = $7
      `, [row.date, row.total_records, row.total_routes, row.total_vehicles, row.avg_speed, row.delayed_count, row.on_time_pct]);
    }
    console.log(`  ✓ Inserted ${dailySummaries.length} daily summaries`);

    // 2. Daily route performance
    console.log('\n2. Computing daily route performance...');
    const routePerf = await queryMotherDuck(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        route,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${MOTHERDUCK_DB}.transit_data
      WHERE route != 'U'
      GROUP BY DATE_TRUNC('day', timestamp), route
      HAVING COUNT(*) > 50
      ORDER BY date, route
    `);

    for (const row of routePerf) {
      await pgPool.query(`
        INSERT INTO daily_route_performance (date, route, readings, delayed, delay_pct, on_time_pct, avg_speed)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (date, route) DO UPDATE SET
          readings = $3,
          delayed = $4,
          delay_pct = $5,
          on_time_pct = $6,
          avg_speed = $7
      `, [row.date, row.route, row.readings, row.delayed, row.delay_pct, row.on_time_pct, row.avg_speed]);
    }
    console.log(`  ✓ Inserted ${routePerf.length} daily route records`);

    // 3. Daily segment performance (ROW vs mixed)
    console.log('\n3. Computing daily segment performance...');
    const segmentPerf = await queryMotherDuck(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${MOTHERDUCK_DB}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY DATE_TRUNC('day', timestamp), segment_type
      ORDER BY date, segment_type
    `);

    for (const row of segmentPerf) {
      await pgPool.query(`
        INSERT INTO daily_segment_performance (date, segment_type, readings, delayed, delay_pct, on_time_pct, avg_speed)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (date, segment_type) DO UPDATE SET
          readings = $3,
          delayed = $4,
          delay_pct = $5,
          on_time_pct = $6,
          avg_speed = $7
      `, [row.date, row.segment_type, row.readings, row.delayed, row.delay_pct, row.on_time_pct, row.avg_speed]);
    }
    console.log(`  ✓ Inserted ${segmentPerf.length} daily segment records`);

    // 4. Hourly segment performance (aggregate across all days)
    console.log('\n4. Computing hourly segment performance...');
    const hourlyPerf = await queryMotherDuck(`
      SELECT
        EXTRACT(HOUR FROM timestamp) as hour,
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${MOTHERDUCK_DB}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY EXTRACT(HOUR FROM timestamp), segment_type
      ORDER BY hour, segment_type
    `);

    for (const row of hourlyPerf) {
      await pgPool.query(`
        INSERT INTO hourly_segment_performance (hour, segment_type, readings, delayed, delay_pct, avg_speed, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
        ON CONFLICT (hour, segment_type) DO UPDATE SET
          readings = $3,
          delayed = $4,
          delay_pct = $5,
          avg_speed = $6,
          updated_at = CURRENT_TIMESTAMP
      `, [row.hour, row.segment_type, row.readings, row.delayed, row.delay_pct, row.avg_speed]);
    }
    console.log(`  ✓ Inserted ${hourlyPerf.length} hourly records`);

    // 5. Overall segment type performance
    console.log('\n5. Computing segment type summaries...');
    const segmentTypeSummary = await queryMotherDuck(`
      SELECT
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${MOTHERDUCK_DB}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY segment_type
    `);
    console.log(`  ✓ ${segmentTypeSummary.length} segment type summaries computed`);

    // 6. Individual segment summaries
    console.log('\n6. Computing individual segment summaries...');
    const segments = await queryMotherDuck(`
      SELECT
        segment_name,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${MOTHERDUCK_DB}.transit_data
      WHERE route = '12' AND segment_name IS NOT NULL
      GROUP BY segment_name, segment_type
      ORDER BY avg_speed DESC
    `);

    for (const row of segments) {
      await pgPool.query(`
        INSERT INTO segment_summary (segment_name, segment_type, readings, delay_pct, avg_speed, updated_at)
        VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
        ON CONFLICT (segment_name) DO UPDATE SET
          segment_type = $2,
          readings = $3,
          delay_pct = $4,
          avg_speed = $5,
          updated_at = CURRENT_TIMESTAMP
      `, [row.segment_name, row.segment_type, row.readings, row.delay_pct, row.avg_speed]);
    }
    console.log(`  ✓ Inserted ${segments.length} segment summaries`);

    console.log(`\n✓ Consolidation complete at ${new Date().toISOString()}`);

  } catch (err) {
    console.error('Consolidation error:', err);
    throw err;
  }
}

async function initPostgres() {
  const fs = await import('fs');
  const schema = fs.readFileSync(new URL('./schema.sql', import.meta.url), 'utf-8');
  await pgPool.query(schema);
  console.log('✓ Postgres schema initialized');
}

async function main() {
  console.log('Transit Data Consolidation Service');
  console.log('===================================');

  // Check if running once or scheduled
  const runOnce = process.argv.includes('--once');

  try {
    // Initialize connections
    await initMotherDuck();
    await initPostgres();

    if (runOnce) {
      console.log('\nRunning consolidation once...');
      await consolidate();
      await pgPool.end();
      process.exit(0);
    } else {
      console.log('\nScheduled to run at 6 AM and 6 PM daily');

      // Run immediately on startup
      await consolidate();

      // Schedule for 6 AM and 6 PM
      cron.schedule('0 6,18 * * *', async () => {
        await consolidate();
      });

      console.log('\nService running. Press Ctrl+C to stop.');
    }

  } catch (err) {
    console.error('Fatal error:', err);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGTERM', async () => {
  console.log('\nShutting down...');
  await pgPool.end();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('\nShutting down...');
  await pgPool.end();
  process.exit(0);
});

main();
