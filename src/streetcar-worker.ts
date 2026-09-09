import 'dotenv/config';
import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { readFile } from 'node:fs/promises';
import { fork, type ChildProcess } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { addDays, localDay, observationEpoch } from './otp';
import { analyzeStreetcars, METHOD, type StreetcarObservation } from './streetcar-analysis';
import { detectStreetcarWaits, WAIT_METHOD, type WaitSnapshot } from './streetcar-waits';
import { prepareDerivedStage, replaceDerivedDay } from './derived-replacement';
import type { StreetcarNetwork } from '../dashboard/src/streetcar-data';

const quote = (s: string) => `'${s.replace(/'/g, "''")}'`;
const DERIVED_BATCH_SIZE = 5000; // keep remote round trips bounded for historical processing
const PRIORITY_VERSION = 'streetcar-priority-same-window-v1';
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
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_passages (
    date DATE,corridor VARCHAR,route VARCHAR,direction VARCHAR,path_id VARCHAR,window_id VARCHAR,
    from_meters DOUBLE,to_meters DOUBLE,run_id VARCHAR,vid VARCHAR,trip_id VARCHAR,
    entry_at DOUBLE,exit_at DOUBLE,hour INTEGER,day_type VARCHAR,category VARCHAR,
    signal_ids VARCHAR,stop_ids VARCHAR,duration_seconds DOUBLE,duration_lower_seconds DOUBLE,
    duration_upper_seconds DOUBLE,network_version VARCHAR,method VARCHAR,
    PRIMARY KEY(date,run_id,window_id)
  )`);
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_priority_days (
    date DATE PRIMARY KEY,applied_version VARCHAR,updated_at TIMESTAMPTZ
  )`);
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_waits (
    id VARCHAR PRIMARY KEY,date DATE,corridor VARCHAR,route VARCHAR,site_id VARCHAR,context VARCHAR,
    started_at DOUBLE,ended_at DOUBLE,duration_seconds DOUBLE,network_version VARCHAR,method VARCHAR
  )`);
  await c.run('ALTER TABLE streetcar_waits ADD COLUMN IF NOT EXISTS method VARCHAR');
  await c.run(`CREATE TABLE IF NOT EXISTS streetcar_wait_quality (
    date DATE,corridor VARCHAR,snapshots INTEGER,first_at DOUBLE,last_at DOUBLE,updated_at TIMESTAMPTZ,
    PRIMARY KEY(date,corridor)
  )`);
}
export async function calculateStreetcarWaitDay(c: DuckDBConnection, network: StreetcarNetwork, day: string) {
  const exists=(await c.runAndReadAll("SELECT COUNT(*) AS n FROM information_schema.tables WHERE table_name='streetcar_snapshots' AND table_catalog=current_database()")).getRowObjectsJson();
  if(!Number(exists[0].n))return;
  const rows=(await c.runAndReadAll(`SELECT vid,route,COALESCE(legacy_trip_id,gtfs_trip_id) AS trip_id,
    epoch(received_at) AS received_at,epoch(provider_observed_at) AS provider_at,lat,lon,speed,is_off_route
    FROM streetcar_snapshots WHERE received_at>=timezone('America/Chicago',${quote(day)}::TIMESTAMP)
      AND received_at<timezone('America/Chicago',${quote(addDays(day,1))}::TIMESTAMP)
      AND lower(COALESCE(destination,'')) NOT LIKE '%not in service%' ORDER BY received_at`)).getRowObjectsJson();
  const snapshots:WaitSnapshot[]=rows.map(r=>({vid:r.vid==null?'':String(r.vid),route:String(r.route),trip_id:r.trip_id==null?null:String(r.trip_id),
    received_at:Number(r.received_at),provider_at:r.provider_at==null?null:Number(r.provider_at),
    lat:r.lat==null?NaN:Number(r.lat),lon:r.lon==null?NaN:Number(r.lon),speed:r.speed==null?null:Number(r.speed),off_route:r.is_off_route===true}));
  const events=detectStreetcarWaits(network,snapshots);
  await c.run('BEGIN TRANSACTION');
  try {
    const waitsStage=await prepareDerivedStage(c,'streetcar_waits'),qualityStage=await prepareDerivedStage(c,'streetcar_wait_quality');
    for(let i=0;i<events.length;i+=DERIVED_BATCH_SIZE)await c.run(`INSERT INTO ${waitsStage} VALUES ${events.slice(i,i+DERIVED_BATCH_SIZE).map(e=>`(${[
      quote(e.id),quote(day),quote(e.corridor),quote(e.route),quote(e.site_id),quote(e.context),e.started_at,e.ended_at,e.duration_seconds,quote(network.version),quote(WAIT_METHOD.name),
    ].join(',')})`).join(',')}`);
    for(const corridor of network.corridors) {
      const selected=snapshots.filter(s=>corridor.routes.includes(s.route));
      await c.run(`INSERT INTO ${qualityStage} VALUES (${quote(day)},${quote(corridor.id)},${selected.length},${selected.length?selected[0].received_at:'NULL'},${selected.length?selected.at(-1)!.received_at:'NULL'},now())`);
    }
    await replaceDerivedDay(c,{table:'streetcar_waits',stage:waitsStage,date_column:'date',date:day,keys:['id']});
    await replaceDerivedDay(c,{table:'streetcar_wait_quality',stage:qualityStage,date_column:'date',date:day,keys:['date','corridor']});
    await c.run('COMMIT');
  }catch(err){await c.run('ROLLBACK');throw err;}
  console.log(`[Streetcars] ${day}: ${snapshots.length} retained snapshots, ${events.length} completed candidate waits (receipt clock)`);
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
    const stages:Record<string,string>={};
    for (const table of ['streetcar_bins','streetcar_site_bins','streetcar_quality','streetcar_passages']) stages[table]=await prepareDerivedStage(c,table);
    for(let i=0;i<result.passages.length;i+=DERIVED_BATCH_SIZE) {
      const values=result.passages.slice(i,i+DERIVED_BATCH_SIZE).map(p=>`(${[
        quote(p.date),quote(p.corridor),quote(p.route),quote(p.direction),quote(p.path_id),quote(p.window_id),
        p.from_meters,p.to_meters,quote(p.run_id),quote(p.vid),p.trip_id==null?'NULL':quote(p.trip_id),
        p.entry_at,p.exit_at,p.hour,quote(p.day_type),quote(p.category),quote(JSON.stringify(p.signal_ids)),quote(JSON.stringify(p.stop_ids)),
        p.duration_seconds,p.duration_lower_seconds,p.duration_upper_seconds,quote(network.version),quote(PRIORITY_VERSION),
      ].join(',')})`);
      await c.run(`INSERT INTO ${stages.streetcar_passages} VALUES ${values.join(',')}`);
    }
    for (let i = 0; i < result.bins.length; i += DERIVED_BATCH_SIZE) await c.run(`INSERT INTO ${stages.streetcar_bins} VALUES ${result.bins.slice(i,i+DERIVED_BATCH_SIZE).map(b=>`(${metricValues(b)})`).join(',')}`);
    for (let i = 0; i < result.site_bins.length; i += DERIVED_BATCH_SIZE) await c.run(`INSERT INTO ${stages.streetcar_site_bins} VALUES ${result.site_bins.slice(i,i+DERIVED_BATCH_SIZE).map(b=>`(${quote(b.site_id)},${metricValues(b)})`).join(',')}`);
    if (result.quality.length) await c.run(`INSERT INTO ${stages.streetcar_quality} VALUES ${result.quality.map(q=>`(${quote(q.date)},${quote(q.corridor)},${q.raw_points},${q.candidate_intervals},${q.accepted_intervals},${quote(JSON.stringify(q.excluded))},${quote(network.version)},${quote(METHOD.name)},now())`).join(',')}`);
    const keys:Record<string,string[]>={streetcar_passages:['date','run_id','window_id'],streetcar_bins:['date','corridor','route','direction','hour','day_type','category'],streetcar_site_bins:['site_id','date','corridor','route','direction','hour','day_type','category'],streetcar_quality:['date','corridor']};
    for(const table of Object.keys(stages)) await replaceDerivedDay(c,{table,stage:stages[table],date_column:'date',date:day,keys:keys[table]});
    await c.run(`UPDATE streetcar_backfill_days SET applied_version=${quote(network.version + ':' + METHOD.name)},completed_at=now() WHERE date=${quote(day)}::DATE`);
    await c.run(`INSERT INTO streetcar_priority_days VALUES (${quote(day)},${quote(network.version+':'+PRIORITY_VERSION)},now())
      ON CONFLICT(date) DO UPDATE SET applied_version=excluded.applied_version,updated_at=excluded.updated_at`);
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
      for(let offset=-1;offset<=0;offset++) {
        const day=addDays(today,offset);
        await calculateStreetcarDay(c,network,day);
        await calculateStreetcarWaitDay(c,network,day);
      }
      const pending=(await c.runAndReadAll(`SELECT b.date::VARCHAR AS day FROM streetcar_backfill_days b
        LEFT JOIN streetcar_priority_days p ON p.date=b.date
        WHERE b.applied_version IS NULL OR b.applied_version<>${quote(network.version+':'+METHOD.name)}
          OR p.applied_version IS NULL OR p.applied_version<>${quote(network.version+':'+PRIORITY_VERSION)}
        ORDER BY b.completed_at ASC NULLS FIRST,b.date LIMIT 2`)).getRowObjectsJson();
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
