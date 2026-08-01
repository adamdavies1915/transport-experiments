import 'dotenv/config';
import express, { type Request, type Response } from 'express';
import cors from 'cors';
import pg from 'pg';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const { Pool } = pg;
const __dirname = dirname(fileURLToPath(import.meta.url));

const app = express();
app.use(cors());
app.use(express.json());

// Serve static files in production
app.use(express.static(join(__dirname, 'dist')));

// Postgres config
const pgPool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: Number(process.env.POSTGRES_PORT) || 5432,
  database: process.env.POSTGRES_DB || 'transit',
  user: process.env.POSTGRES_USER || 'postgres',
  password: process.env.POSTGRES_PASSWORD,
  ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : false
});

let dataReady = false;

async function initPostgres(): Promise<void> {
  try {
    await pgPool.query('SELECT NOW()');
    console.log('✓ Postgres connected');
    dataReady = true;
  } catch (err) {
    console.error('Failed to connect to Postgres:', err);
    throw err;
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

// Health check
app.get('/api/health', (_req: Request, res: Response) => {
  if (!dataReady) {
    return res.status(503).json({ status: 'loading', message: 'Connecting to Postgres...' });
  }
  res.json({ status: 'ready' });
});

// API Routes - query pre-computed Postgres tables
app.get('/api/summary', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        SUM(total_records) as total_records,
        MAX(total_routes) as total_routes,
        MAX(total_vehicles) as total_vehicles,
        MIN(date) as first_record,
        MAX(date) as last_record
      FROM daily_summary
    `);
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/segment-types', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        segment_type,
        SUM(readings) as readings,
        SUM(delayed) as delayed,
        ROUND(100.0 * SUM(delayed) / SUM(readings), 2) as delay_pct,
        ROUND(AVG(avg_speed), 1) as avg_speed
      FROM daily_segment_performance
      GROUP BY segment_type
      ORDER BY segment_type
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/segments', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        segment_name,
        segment_type,
        readings,
        delay_pct,
        avg_speed
      FROM segment_summary
      ORDER BY avg_speed DESC
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/routes', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        route,
        SUM(readings) as readings,
        SUM(delayed) as delayed,
        ROUND(100.0 * SUM(delayed) / SUM(readings), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(delayed) / SUM(readings)), 2) as on_time_pct,
        ROUND(AVG(avg_speed), 1) as avg_speed
      FROM daily_route_performance
      GROUP BY route
      HAVING SUM(readings) > 100
      ORDER BY on_time_pct DESC
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/hourly', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        hour,
        segment_type,
        delay_pct,
        avg_speed
      FROM hourly_segment_performance
      ORDER BY hour, segment_type
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

// Time-series endpoints for year-long analysis
app.get('/api/daily', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        date,
        total_records as readings,
        total_vehicles as vehicles,
        (total_records - delayed_count) as delayed,
        on_time_pct,
        avg_speed
      FROM daily_summary
      ORDER BY date
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/daily-routes', async (req: Request, res: Response) => {
  try {
    const route = req.query.route as string | undefined;
    const whereClause = route ? `WHERE route = $1` : `WHERE route != 'U'`;
    const params = route ? [route] : [];

    const result = await pgPool.query(`
      SELECT
        date,
        route,
        readings,
        on_time_pct,
        avg_speed
      FROM daily_route_performance
      ${whereClause}
      ORDER BY date, route
    `, params);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

app.get('/api/daily-segments', async (_req: Request, res: Response) => {
  try {
    const result = await pgPool.query(`
      SELECT
        date,
        segment_type,
        readings,
        on_time_pct,
        avg_speed
      FROM daily_segment_performance
      ORDER BY date, segment_type
    `);
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: errorMessage(err) });
  }
});

// SPA fallback (Express 5 syntax)
app.get('/{*path}', (_req: Request, res: Response) => {
  res.sendFile(join(__dirname, 'dist', 'index.html'));
});

const PORT = process.env.PORT || 3000;

initPostgres().then(() => {
  app.listen(PORT, () => {
    console.log(`Dashboard API running on port ${PORT}`);
    console.log('Reading from Postgres pre-computed aggregates');
  });
}).catch(err => {
  console.error('Failed to initialize Postgres:', err);
  process.exit(1);
});
