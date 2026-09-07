import 'dotenv/config';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { readFile } from 'node:fs/promises';
import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { addDays, localDay, observationEpoch } from './otp';
import { analyzeStreetcars, METHOD, type StreetcarObservation } from './streetcar-analysis';
import type { StreetcarNetwork } from '../dashboard/src/streetcar-data';

const quote = (s: string) => `'${s.replace(/'/g, "''")}'`;
export async function readStreetcarNetwork(file?: string): Promise<StreetcarNetwork> {
  const network = JSON.parse(await readFile(file ?? fileURLToPath(new URL('./data/streetcar-network.json', import.meta.url)), 'utf8')) as StreetcarNetwork;
  if (!network.version || !network.paths?.length || !network.sites?.length || network.corridors?.length !== 3) throw new Error('Invalid streetcar network catalog');
  return network;
}
export async function initializeStreetcars(c: DuckDBConnection, network: StreetcarNetwork): Promise<void> {
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_networks (
    version VARCHAR PRIMARY KEY, network_json VARCHAR, method_json VARCHAR, installed_at TIMESTAMPTZ
  )`);
  await c.run(`INSERT INTO streetcar_networks VALUES (${quote(network.version)},${quote(JSON.stringify(network))},${quote(JSON.stringify(METHOD))},now())
    ON CONFLICT(version) DO UPDATE SET network_json=excluded.network_json,method_json=excluded.method_json`);
  const dimensions = 'date DATE, corridor VARCHAR, route VARCHAR, direction VARCHAR, hour INTEGER, day_type VARCHAR, category VARCHAR';
  const metrics = 'intervals INTEGER, duration_seconds DOUBLE, distance_meters DOUBLE, slow_seconds DOUBLE, vehicle_ids VARCHAR, network_version VARCHAR, method VARCHAR, duration_lower_seconds DOUBLE, duration_upper_seconds DOUBLE';
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_bins (${dimensions},${metrics},
    PRIMARY KEY(date,corridor,route,direction,hour,day_type,category))`);
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_site_bins (site_id VARCHAR,${dimensions},${metrics},
    PRIMARY KEY(site_id,date,corridor,route,direction,hour,day_type,category))`);
  for(const table of ['streetcar_bins','streetcar_site_bins']) {
    await c.run(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS duration_lower_seconds DOUBLE`);
    await c.run(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS duration_upper_seconds DOUBLE`);
  }
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_quality (date DATE,corridor VARCHAR,
    raw_points INTEGER,candidate_intervals INTEGER,accepted_intervals INTEGER,excluded VARCHAR,
    network_version VARCHAR,method VARCHAR,updated_at TIMESTAMPTZ,PRIMARY KEY(date,corridor))`);
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_backfill_days (date DATE PRIMARY KEY,applied_version VARCHAR,completed_at TIMESTAMPTZ)`);
}
export async function calculateStreetcarDay(c: DuckDBConnection, network: StreetcarNetwork, day: string): Promise<void> {
  const routes = [...new Set(network.paths.map(p => p.route))];
  // Keep service-day boundary pairs out: no interval is attributed to two dates.
  const rows = (await c.runAndReadAll(`SELECT vid,route,COALESCE(trip_id,gtfs_trip_id) AS trip_id,
    lat,lon,heading,is_off_route,epoch(observed_at) AS instant,timestamp::VARCHAR AS wall_time
    FROM transit_data WHERE timestamp >= ${quote(day)}::DATE AND timestamp < ${quote(addDays(day, 1))}::DATE
      AND route IN (${routes.map(quote).join(',')})
      AND lower(COALESCE(destination,'')) NOT LIKE '%not in service%'`)).getRowObjectsJson();
  const observations: StreetcarObservation[] = rows.map(r => ({ vid: String(r.vid),route: String(r.route),
    trip_id: r.trip_id == null ? null : String(r.trip_id), lat: r.lat == null ? NaN : Number(r.lat),
    lon: r.lon == null ? NaN : Number(r.lon), heading: r.heading == null ? undefined : Number(r.heading),
    at: r.instant == null ? observationEpoch(String(r.wall_time),'America/Chicago') ?? NaN : Number(r.instant),
    off_route: r.is_off_route === true,
  }));
  const result = analyzeStreetcars(network, day, observations);
  const metricValues = (b: typeof result.bins[number]) => [quote(b.date),quote(b.corridor),quote(b.route),quote(b.direction),b.hour,
    quote(b.day_type),quote(b.category),b.intervals,b.duration_seconds,b.distance_meters,b.slow_seconds,
    quote(JSON.stringify(b.vehicle_ids)),quote(network.version),quote(METHOD.name),
    b.duration_lower_seconds??'NULL',b.duration_upper_seconds??'NULL'].join(',');
  await c.run('BEGIN TRANSACTION');
  try {
    for (const table of ['streetcar_bins','streetcar_site_bins','streetcar_quality']) await c.run(`DELETE FROM ${table} WHERE date=${quote(day)}::DATE`);
    for (let i = 0; i < result.bins.length; i += 500) await c.run(`INSERT INTO streetcar_bins VALUES ${result.bins.slice(i,i+500).map(b=>`(${metricValues(b)})`).join(',')}`);
    for (let i = 0; i < result.site_bins.length; i += 500) await c.run(`INSERT INTO streetcar_site_bins VALUES ${result.site_bins.slice(i,i+500).map(b=>`(${quote(b.site_id)},${metricValues(b)})`).join(',')}`);
    if (result.quality.length) await c.run(`INSERT INTO streetcar_quality VALUES ${result.quality.map(q=>`(${quote(q.date)},${quote(q.corridor)},${q.raw_points},${q.candidate_intervals},${q.accepted_intervals},${quote(JSON.stringify(q.excluded))},${quote(network.version)},${quote(METHOD.name)},now())`).join(',')}`);
    await c.run(`UPDATE streetcar_backfill_days SET applied_version=${quote(network.version + ':' + METHOD.name)},completed_at=now() WHERE date=${quote(day)}::DATE`);
    await c.run('COMMIT');
  } catch (err) { await c.run('ROLLBACK'); throw err; }
  console.log(`[Streetcars] ${day}: ${observations.length} GPS points, ${result.quality.reduce((n,q)=>n+q.accepted_intervals,0)} usable GPS intervals, ${result.bins.reduce((n,b)=>n+b.intervals,0)} completed passages, ${result.site_bins.length} site summaries`);
}
async function openDatabase(network: StreetcarNetwork) {
  if (!process.env.MOTHER_DUCK_API_KEY) throw new Error('Missing MOTHER_DUCK_API_KEY');
  const db = await DuckDBInstance.create(`md:${process.env.MOTHERDUCK_DATABASE || 'my_db'}?motherduck_token=${process.env.MOTHER_DUCK_API_KEY}`);
  const c = await db.connect();
  try { await initializeStreetcars(c,network); }
  catch(err) {c.closeSync();db.closeSync();throw err;}
  return { db,c };
}
export function startStreetcarWorker(): () => void {
  let child: ChildProcess | undefined, stopped = false;
  const run = () => {
    if (child || stopped) return;
    child = fork(fileURLToPath(import.meta.url),['--recent'],{execArgv:['--import','tsx'],stdio:'inherit'});
    child.on('error',err=>console.error('[Streetcars] Worker error:',String(err)));
    child.on('exit',code=>{if(code)console.error(`[Streetcars] Worker exited ${code}; retrying hourly`);child=undefined;});
  };
  const first = setTimeout(run,90_000); // let collector and OTP establish their connections first
  const timer = setInterval(run,60*60*1000);
  return () => { stopped=true;clearTimeout(first);clearInterval(timer);child?.kill('SIGTERM'); };
}
async function main() {
  const args=process.argv.slice(2),get=(flag:string)=>{const i=args.indexOf(flag);return i<0?undefined:args[i+1];};
  const network=await readStreetcarNetwork(get('--network'));
  const {db,c}=await openDatabase(network);
  try {
    if(args.includes('--recent')) {
      const today=localDay(Date.now()/1000,'America/Chicago');
      for(let offset=-1;offset<=0;offset++) await calculateStreetcarDay(c,network,addDays(today,offset));
      const pending=(await c.runAndReadAll(`SELECT date::VARCHAR AS day FROM streetcar_backfill_days
        WHERE applied_version IS NULL OR applied_version<>${quote(network.version+':'+METHOD.name)}
        ORDER BY completed_at ASC NULLS FIRST,date LIMIT 2`)).getRowObjectsJson();
      for(const r of pending) await calculateStreetcarDay(c,network,String(r.day));
      return;
    }
    const from=get('--from'),to=get('--to');
    if(!from||!to||addDays(from,0)!==from||addDays(to,0)!==to||from>to) throw new Error('Usage: npm run streetcars:backfill -- --from YYYY-MM-DD --to YYYY-MM-DD [--network catalog.json]');
    const days:string[]=[];for(let day=from;day<=to;day=addDays(day,1))days.push(day);
    await c.run(`INSERT INTO streetcar_backfill_days VALUES ${days.map(day=>`(${quote(day)},NULL,NULL)`).join(',')}
      ON CONFLICT(date) DO UPDATE SET applied_version=NULL`);
    for(const day of days) await calculateStreetcarDay(c,network,day);
  } finally {c.closeSync();db.closeSync();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)main().catch(err=>{console.error(String(err));process.exitCode=1;});
