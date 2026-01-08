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

let dataLoaded = false;

async function initDuckDB() {
  await run(`INSTALL httpfs`);
  await run(`LOAD httpfs`);
  await run(`SET s3_region='auto'`);
  await run(`SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com'`);
  await run(`SET s3_access_key_id='${R2_ACCESS_KEY_ID}'`);
  await run(`SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}'`);
  await run(`SET s3_url_style='path'`);
  console.log('DuckDB initialized with R2 connection');

  // Pre-load all data into memory table (slow once, fast queries after)
  console.log('Loading transit data into memory (this may take a few minutes)...');
  const startTime = Date.now();
  await run(`CREATE TABLE transit AS SELECT * FROM transit`);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const count = await query('SELECT COUNT(*) as cnt FROM transit');
  console.log(`Loaded ${count[0].cnt.toLocaleString()} records in ${elapsed}s`);
  dataLoaded = true;
}

// Reload data every hour
async function reloadData() {
  if (!dataLoaded) return;
  console.log('Reloading transit data...');
  const startTime = Date.now();
  await run('DROP TABLE IF EXISTS transit');
  await run(`CREATE TABLE transit AS SELECT * FROM transit`);
  cache.clear(); // Clear cache after reload
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`Data reloaded in ${elapsed}s`);
}

setInterval(reloadData, 60 * 60 * 1000); // Reload hourly

// Health check - returns 503 while loading data
app.get('/api/health', (req, res) => {
  if (!dataLoaded) {
    return res.status(503).json({ status: 'loading', message: 'Loading transit data...' });
  }
  res.json({ status: 'ready' });
});

// API Routes
app.get('/api/summary', async (req, res) => {
  if (!dataLoaded) return res.status(503).json({ error: 'Data still loading...' });
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
  if (!dataLoaded) return res.status(503).json({ error: 'Data still loading...' });
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
  if (!dataLoaded) return res.status(503).json({ error: 'Data still loading...' });
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
  if (!dataLoaded) return res.status(503).json({ error: 'Data still loading...' });
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
