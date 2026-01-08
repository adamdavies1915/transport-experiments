import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import duckdb from 'duckdb';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(cors());
app.use(express.json());

// Serve static files in production
app.use(express.static(join(__dirname, 'dist')));

// Simple in-memory cache (1 hour TTL - data only updates hourly anyway)
const cache = new Map();
const CACHE_TTL = 60 * 60 * 1000; // 1 hour

function getCached(key) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expires) {
    cache.delete(key);
    return null;
  }
  return item.data;
}

function setCache(key, data) {
  cache.set(key, { data, expires: Date.now() + CACHE_TTL });
}

const R2_ACCOUNT_ID = process.env.R2_ACCOUNT_ID || process.env.VITE_R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID || process.env.VITE_R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY || process.env.VITE_R2_SECRET_ACCESS_KEY;
const R2_BUCKET = process.env.R2_BUCKET || process.env.VITE_R2_BUCKET || 'nola-transit';
const DATA_PATH = `s3://${R2_BUCKET}/**/*.parquet`;

// Initialize DuckDB
const db = new duckdb.Database(':memory:');

function query(sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) reject(err);
      else {
        // Convert BigInt to Number for JSON serialization
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

function run(sql) {
  return new Promise((resolve, reject) => {
    db.run(sql, (err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

let dataReady = false;

async function initDuckDB() {
  await run(`INSTALL httpfs`);
  await run(`LOAD httpfs`);
  await run(`SET s3_region='auto'`);
  await run(`SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com'`);
  await run(`SET s3_access_key_id='${R2_ACCESS_KEY_ID}'`);
  await run(`SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}'`);
  await run(`SET s3_url_style='path'`);

  // Load consolidated daily files into RAM (fast queries)
  console.log('Loading consolidated daily data into memory...');
  const startTime = Date.now();

  try {
    await run(`CREATE TABLE daily_data AS SELECT * FROM read_parquet('s3://${R2_BUCKET}/daily/*.parquet')`);
    const count = await query('SELECT COUNT(*) as cnt FROM daily_data');
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`Loaded ${count[0].cnt.toLocaleString()} daily records in ${elapsed}s`);
  } catch (err) {
    console.log('No daily files yet, creating empty table');
    await run(`CREATE TABLE daily_data (vid VARCHAR, timestamp TIMESTAMP, lat DOUBLE, lon DOUBLE, route VARCHAR, speed DOUBLE, heading INTEGER, is_delayed BOOLEAN, segment_id INTEGER, segment_name VARCHAR, segment_type VARCHAR)`);
  }

  // Create view combining daily (in RAM) + hourly files (streamed if they exist)
  try {
    await run(`
      CREATE VIEW transit AS
      SELECT * FROM daily_data
      UNION ALL
      SELECT * FROM read_parquet('s3://${R2_BUCKET}/*/*/transit-*.parquet')
    `);
    console.log('DuckDB ready - daily data in RAM, hourly streamed from R2');
  } catch (err) {
    // No hourly files yet, just use daily data
    console.log('No hourly files found, using daily data only');
    await run(`CREATE VIEW transit AS SELECT * FROM daily_data`);
  }
  dataReady = true;
}

// Reload data every 6 hours to pick up new consolidated files
async function reloadData() {
  if (!dataReady) return;
  console.log('Reloading data...');
  try {
    await run('DROP VIEW IF EXISTS transit');
    await run('DROP TABLE IF EXISTS daily_data');
    await run(`CREATE TABLE daily_data AS SELECT * FROM read_parquet('s3://${R2_BUCKET}/daily/*.parquet')`);
    try {
      await run(`
        CREATE VIEW transit AS
        SELECT * FROM daily_data
        UNION ALL
        SELECT * FROM read_parquet('s3://${R2_BUCKET}/*/*/transit-*.parquet')
      `);
    } catch {
      await run(`CREATE VIEW transit AS SELECT * FROM daily_data`);
    }
    cache.clear();
    console.log('Data reloaded');
  } catch (err) {
    console.error('Reload failed:', err.message);
  }
}

setInterval(reloadData, 6 * 60 * 60 * 1000); // Every 6 hours

// Clear cache hourly to pick up new hourly files
setInterval(() => {
  cache.clear();
  console.log('Cache cleared');
}, 60 * 60 * 1000);

// Health check
app.get('/api/health', (req, res) => {
  if (!dataReady) {
    return res.status(503).json({ status: 'loading', message: 'Loading data...' });
  }
  res.json({ status: 'ready' });
});

// API Routes
app.get('/api/summary', async (req, res) => {
    try {
    const cached = getCached('summary');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT route) as total_routes,
        COUNT(DISTINCT vid) as total_vehicles,
        MIN(timestamp) as first_record,
        MAX(timestamp) as last_record
      FROM transit
    `);
    setCache('summary', result[0]);
    res.json(result[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/segment-types', async (req, res) => {
  try {
    const cached = getCached('segment-types');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM transit
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY segment_type
      ORDER BY segment_type
    `);
    setCache('segment-types', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/segments', async (req, res) => {
  try {
    const cached = getCached('segments');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        segment_name,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM transit
      WHERE route = '12' AND segment_name IS NOT NULL
      GROUP BY segment_name, segment_type
      ORDER BY avg_speed DESC
    `);
    setCache('segments', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/routes', async (req, res) => {
  try {
    const cached = getCached('routes');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        route,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM transit
      WHERE route != 'U'
      GROUP BY route
      HAVING COUNT(*) > 100
      ORDER BY on_time_pct DESC
    `);
    setCache('routes', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/hourly', async (req, res) => {
  try {
    const cached = getCached('hourly');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        EXTRACT(HOUR FROM timestamp) as hour,
        segment_type,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM transit
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY EXTRACT(HOUR FROM timestamp), segment_type
      ORDER BY hour
    `);
    setCache('hourly', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Time-series endpoints for year-long analysis

// Daily aggregates - overall system performance over time
app.get('/api/daily', async (req, res) => {
    try {
    const cached = getCached('daily');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as readings,
        COUNT(DISTINCT vid) as vehicles,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM transit
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date
    `);
    setCache('daily', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Daily breakdown by route
app.get('/api/daily-routes', async (req, res) => {
    try {
    const route = req.query.route;
    const cacheKey = `daily-routes-${route || 'all'}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const whereClause = route ? `WHERE route = '${route}'` : "WHERE route != 'U'";
    const result = await query(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        route,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM transit
      ${whereClause}
      GROUP BY DATE_TRUNC('day', timestamp), route
      HAVING COUNT(*) > 50
      ORDER BY date, route
    `);
    setCache(cacheKey, result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Daily breakdown by segment type (ROW vs mixed traffic)
app.get('/api/daily-segments', async (req, res) => {
    try {
    const cached = getCached('daily-segments');
    if (cached) return res.json(cached);

    const result = await query(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM transit
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY DATE_TRUNC('day', timestamp), segment_type
      ORDER BY date, segment_type
    `);
    setCache('daily-segments', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// SPA fallback (Express 5 syntax)
app.get('/{*path}', (req, res) => {
  res.sendFile(join(__dirname, 'dist', 'index.html'));
});

const PORT = process.env.PORT || 3000;

initDuckDB().then(() => {
  app.listen(PORT, () => {
    console.log(`Dashboard API running on port ${PORT}`);
  });
}).catch(err => {
  console.error('Failed to initialize DuckDB:', err);
  process.exit(1);
});
