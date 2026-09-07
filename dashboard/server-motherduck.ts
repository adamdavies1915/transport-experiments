import 'dotenv/config';
import express, { type Request, type Response } from 'express';
import cors from 'cors';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { gzip } from 'node:zlib';
import { promisify } from 'node:util';
import type {
  Summary, SegmentTypeRow, SegmentRow, RouteRow,
  HourlyRow, DailyRow, DailySegmentRow
} from './src/types';
import type { OtpData, OtpDay } from './src/otp-data';
import { otpDaysSql } from './src/otp-query';
import type { StreetcarData, StreetcarNetwork } from './src/streetcar-data';
import { decodeStreetcarBins, decodeStreetcarSiteBins, decodeStreetcarQuality, emptyStreetcarData,
  streetcarCatalog, streetcarQueries, streetcarQuote, streetcarRange } from './src/streetcar-query';

const __dirname = dirname(fileURLToPath(import.meta.url));
const gzipAsync=promisify(gzip);

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

function setCache(key: string, data: unknown, ttl = CACHE_TTL): void {
  cache.set(key, { data, expires: Date.now() + ttl });
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
app.get('/api/streetcars', async (req: Request, res: Response) => {
  try {
    const send=async(value:StreetcarData)=>{
      res.vary('Accept-Encoding');
      if(req.acceptsEncodings('gzip')) {
        const body=await gzipAsync(JSON.stringify(value));res.set('Content-Encoding','gzip').type('json').send(body);
      } else res.json(value);
    };
    const catalog=streetcarCatalog(DATABASE_NAME);
    const tables=await query<{count:number}>(`SELECT COUNT(*) AS count FROM information_schema.tables
      WHERE table_catalog=${streetcarQuote(DATABASE_NAME)} AND table_schema='main'
        AND table_name IN ('streetcar_networks','streetcar_bins','streetcar_site_bins','streetcar_quality')`);
    if(Number(tables[0].count)<4)return res.json(emptyStreetcarData());
    const networks=await query<{version:string;network_json:string;method_json:string}>(`SELECT version,network_json,method_json FROM ${catalog}.streetcar_networks ORDER BY installed_at DESC LIMIT 1`);
    if(!networks.length)return res.json(emptyStreetcarData());
    const saved=networks[0],network=JSON.parse(saved.network_json) as StreetcarNetwork;
    const method=JSON.parse(saved.method_json) as StreetcarData['method'];
    const ranges=await query<{from:string|null;to:string|null}>(`SELECT MIN(date)::VARCHAR AS "from",MAX(date)::VARCHAR AS "to" FROM ${catalog}.streetcar_quality WHERE network_version=${streetcarQuote(saved.version)} AND method=${streetcarQuote(method.name)}`);
    if(!ranges[0].from||!ranges[0].to)return res.json(emptyStreetcarData(network));
    const corridor=typeof req.query.corridor==='string'?req.query.corridor:'st_charles';
    if(!network.corridors.some(c=>c.id===corridor))return res.status(400).json({error:'Choose St. Charles, Canal, or Rampart–Loyola.'});
    let range:{from:string;to:string};
    try {range=streetcarRange({from:typeof req.query.from==='string'?req.query.from:undefined,to:typeof req.query.to==='string'?req.query.to:undefined},ranges[0].from,ranges[0].to);}
    catch(err){return res.status(400).json({error:errorMessage(err)});}
    const key=`streetcars:${saved.version}:${method.name}:${corridor}:${range.from}:${range.to}`;
    const cached=getCached<StreetcarData>(key);if(cached)return send(cached);
    const sql=streetcarQueries(DATABASE_NAME,saved.version,corridor,range.from,range.to,method.name);
    // One DuckDB connection: serialize its queries; all read compact derived tables.
    const bins=await query<Record<string,unknown>>(sql.bins);
    const sites=await query<Record<string,unknown>>(sql.site_bins);
    const quality=await query<Record<string,unknown>>(sql.quality);
    const result:StreetcarData={status:quality.length?'ready':'not_ready',network,
      bins:decodeStreetcarBins(bins,method.timestamp_quantization_seconds??60),site_bins:decodeStreetcarSiteBins(sites,method.timestamp_quantization_seconds??60),quality:decodeStreetcarQuality(quality),
      updated_at:quality.map(q=>String(q.updated_at)).sort().at(-1)??null,
      available_from:ranges[0].from,available_to:ranges[0].to,selected_from:range.from,selected_to:range.to,
      method};
    setCache(key,result,60_000);return send(result);
  } catch(err){res.status(500).json({error:errorMessage(err)});}
});

app.get('/api/otp', async (_req: Request, res: Response) => {
  try {
    const cached = getCached<OtpData>('otp');
    if (cached) return res.json(cached);
    const tables = await query<{ count: number }>(`SELECT COUNT(*) AS count
      FROM information_schema.tables WHERE table_catalog = '${DATABASE_NAME.replace(/'/g, "''")}'
        AND table_schema = 'main' AND table_name IN ('otp_events', 'otp_coverage', 'otp_trip_mappings', 'otp_sequence_mappings')`);
    if (Number(tables[0].count) < 4) return res.json({ status: 'not_ready', days: [] });
    const days = await query<OtpDay>(otpDaysSql(DATABASE_NAME));
    const result: OtpData = { status: days.length ? 'ready' : 'not_ready', days };
    // OTP reads small precomputed tables; show backfill progress within a minute.
    setCache('otp', result, 60 * 1000);
    res.json(result);
  } catch (err) { res.status(500).json({ error: errorMessage(err) }); }
});

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
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route != 'U'
      GROUP BY route
      HAVING COUNT(*) > 100
      ORDER BY not_flagged_pct DESC
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
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
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
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
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
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
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
