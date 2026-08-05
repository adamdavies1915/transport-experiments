import 'dotenv/config';
import express, { type Request, type Response } from 'express';
import cors from 'cors';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import type {
  Summary, SegmentTypeRow, SegmentRow, RouteRow,
  HourlyRow, DailyRow, DailySegmentRow
} from './src/types';

const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(cors());
app.use(express.json());

// Serve static files in production
app.use(express.static(join(__dirname, 'dist')));

// Simple in-memory cache (1 hour TTL) — the aggregate queries scan the whole
// transit_data table, so caching keeps the dashboard responsive.
interface CacheItem { data: unknown; expires: number; }
const cache = new Map<string, CacheItem>();
const CACHE_TTL = 60 * 60 * 1000; // 1 hour

function getCached<T>(key: string): T | null {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expires) {
    cache.delete(key);
    return null;
  }
  return item.data as T;
}

function setCache(key: string, data: unknown): void {
  cache.set(key, { data, expires: Date.now() + CACHE_TTL });
}

const MOTHER_DUCK_API_KEY = process.env.MOTHER_DUCK_API_KEY;
const DATABASE_NAME = process.env.MOTHERDUCK_DATABASE || 'my_db'; // MotherDuck default database

let instance: DuckDBInstance | undefined;
let connection: DuckDBConnection | undefined;
let dataReady = false;

async function query<T>(sql: string): Promise<T[]> {
  if (!connection) throw new Error('MotherDuck not initialized');
  const reader = await connection.runAndReadAll(sql);
  // getRowObjectsJson yields JSON-safe values (BigInt → number, TIMESTAMP → string).
  return reader.getRowObjectsJson() as unknown as T[];
}

async function initMotherDuck(): Promise<void> {
  if (!MOTHER_DUCK_API_KEY) {
    throw new Error('Missing MOTHER_DUCK_API_KEY environment variable');
  }
  instance = await DuckDBInstance.create(
    `md:${DATABASE_NAME}?motherduck_token=${MOTHER_DUCK_API_KEY}`
  );
  connection = await instance.connect();
  console.log('MotherDuck connected - ready to query');
  dataReady = true;
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Clear cache hourly
setInterval(() => {
  cache.clear();
  console.log('Cache cleared');
}, CACHE_TTL);

// Health check
app.get('/api/health', (_req: Request, res: Response) => {
  if (!dataReady) {
    return res.status(503).json({ status: 'loading', message: 'Connecting to MotherDuck...' });
  }
  res.json({ status: 'ready' });
});

// API Routes - query the transit_data table directly (aggregate on the fly)
app.get('/api/summary', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<Summary>('summary');
    if (cached) return res.json(cached);

    const result = await query<Summary>(`
      SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT route) as total_routes,
        COUNT(DISTINCT vid) as total_vehicles,
        MIN(timestamp) as first_record,
        MAX(timestamp) as last_record
      FROM ${DATABASE_NAME}.transit_data
    `);
    setCache('summary', result[0]);
    res.json(result[0]);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/segment-types', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<SegmentTypeRow[]>('segment-types');
    if (cached) return res.json(cached);

    const result = await query<SegmentTypeRow>(`
      SELECT
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY segment_type
      ORDER BY segment_type
    `);
    setCache('segment-types', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/segments', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<SegmentRow[]>('segments');
    if (cached) return res.json(cached);

    const result = await query<SegmentRow>(`
      SELECT
        segment_name,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_name IS NOT NULL
      GROUP BY segment_name, segment_type
      ORDER BY avg_speed DESC
    `);
    setCache('segments', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/routes', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<RouteRow[]>('routes');
    if (cached) return res.json(cached);

    const result = await query<RouteRow>(`
      SELECT
        route,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route != 'U'
      GROUP BY route
      HAVING COUNT(*) > 100
      ORDER BY on_time_pct DESC
    `);
    setCache('routes', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/hourly', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<HourlyRow[]>('hourly');
    if (cached) return res.json(cached);

    const result = await query<HourlyRow>(`
      SELECT
        EXTRACT(HOUR FROM timestamp) as hour,
        segment_type,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY EXTRACT(HOUR FROM timestamp), segment_type
      ORDER BY hour
    `);
    setCache('hourly', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

// Time-series endpoints for year-long analysis
app.get('/api/daily', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<DailyRow[]>('daily');
    if (cached) return res.json(cached);

    const result = await query<DailyRow>(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as readings,
        COUNT(DISTINCT vid) as vehicles,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date
    `);
    setCache('daily', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/daily-routes', async (req: Request, res: Response) => {
  try {
    const route = req.query.route as string | undefined;
    const cacheKey = `daily-routes-${route || 'all'}`;
    const cached = getCached<unknown[]>(cacheKey);
    if (cached) return res.json(cached);

    // Escape single quotes to avoid breaking the inline literal.
    const whereClause = route
      ? `WHERE route = '${route.replace(/'/g, "''")}'`
      : "WHERE route != 'U'";
    const result = await query(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        route,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      ${whereClause}
      GROUP BY DATE_TRUNC('day', timestamp), route
      HAVING COUNT(*) > 50
      ORDER BY date, route
    `);
    setCache(cacheKey, result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/daily-segments', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<DailySegmentRow[]>('daily-segments');
    if (cached) return res.json(cached);

    const result = await query<DailySegmentRow>(`
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as on_time_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY DATE_TRUNC('day', timestamp), segment_type
      ORDER BY date, segment_type
    `);
    setCache('daily-segments', result);
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

// SPA fallback (Express 5 syntax)
app.get('/{*path}', (_req: Request, res: Response) => {
  res.sendFile(join(__dirname, 'dist', 'index.html'));
});

const PORT = process.env.PORT || 3000;

initMotherDuck().then(() => {
  app.listen(PORT, () => {
    console.log(`Dashboard API running on port ${PORT}`);
    console.log('Reading from MotherDuck transit_data (aggregated on the fly)');
  });
}).catch(err => {
  console.error('Failed to initialize MotherDuck:', err);
  process.exit(1);
});
