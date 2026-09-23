import 'dotenv/config';
import EventSource from 'eventsource';
import { createServer } from 'node:http';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { fork, type ChildProcess } from 'node:child_process';
import { readFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { unzipSync, strFromU8 } from 'fflate';
import { parse } from 'csv-parse/sync';
import { LocalJournal, DATA_DIR, atomicFile } from './local-journal';
import { captureBudget } from './capture-budget';
import { captureStreetcarSnapshots, snapshotSourceUrl } from './streetcar-snapshots';
import type { CollectionBatch, StudyObservation } from './observation-types';
import { safeError } from './log-safety';
import type { LePassHealth } from './lepass-collector';
import { mergeSourceQuality, PersistedSourceClocks } from './source-quality';
import { CaptureExchange } from './capture-exchange';
import { ProcessingCoordinator } from './processing-coordinator';
import { processingRequestHandler } from './processing-server';
import { processingAnalysisRevision } from './processing-revision';
import { sealCaptureFrontier } from './capture-frontier';

const SSE_URL=process.env.SSE_URL||'https://nolatransit.fly.dev/sse';
const storageBudget=captureBudget(DATA_DIR);
const journal=new LocalJournal(DATA_DIR,storageBudget);
const STALE_MS=Number(process.env.STALE_FEED_THRESHOLD)||300_000;
const token=process.env.TRANSIT_SUMMARY_TOKEN;
let es:EventSource|undefined, worker:ChildProcess|undefined, stopping=false, paused=false;
let lastReceived=0,lastPersisted=0,frames=0,errors=0,pending=0;
const sourceClocks=new PersistedSourceClocks();
let restartTimer:NodeJS.Timeout|undefined, reconnectTimer:NodeJS.Timeout|undefined;
let lepassStop:(()=>void)|undefined;
let lepassHealth:LePassHealth|{status:'unavailable';message:string}={status:'unavailable',message:'LePass is starting'};
const routes=new Set<string>();
const processingServer=process.env.PROCESSING_SERVER_ENABLED==='true';
let exchange:CaptureExchange|undefined,coordinator:ProcessingCoordinator|undefined;
let processingHandler:ReturnType<typeof processingRequestHandler>|undefined;
let maintenance:Promise<unknown>|undefined,scheduleRefresh:Promise<void>|undefined,lastScheduleRefresh=0;

function sealCapture(frontier=false):Promise<unknown>{
  if(!exchange)return Promise.resolve();
  if(maintenance)return frontier?maintenance.then(()=>sealCapture(true)):maintenance;
  return maintenance=(frontier?sealCaptureFrontier(exchange,journal):exchange.seal(journal)).finally(()=>{maintenance=undefined;});
}

async function loadRoutes(){
  try {
    const catalog=JSON.parse(await readFile(join(fileURLToPath(new URL('./data/',import.meta.url)),'transit-study-network.json'),'utf8'));
    for(const p of catalog.paths??[])routes.add(String(p.route_id));
  }catch{}
  try {
    const response=await fetch(process.env.GTFS_URL||'https://www.norta.com/RTA/media/GTFS/GTFS.zip',{signal:AbortSignal.timeout(30_000)});
    if(!response.ok)throw new Error(`GTFS HTTP ${response.status}`);
    const bytes=new Uint8Array(await response.arrayBuffer());
    const zip=unzipSync(bytes);
    const rows=parse(strFromU8(zip['routes.txt']),{columns:true,bom:true,skip_empty_lines:true}) as Record<string,string>[];
    for(const r of rows)if(['0','3'].includes(r.route_type))routes.add(r.route_short_name);
    await atomicFile(join(DATA_DIR,'current-gtfs.zip'),bytes);
    if(exchange)await exchange.recordSchedule(bytes,process.env.GTFS_URL||'https://www.norta.com/RTA/media/GTFS/GTFS.zip',new Date().toISOString());
    lastScheduleRefresh=Date.now();
  }catch(e){console.error('[Routes] Using bundled validated routes:',safeError(e));}
  if(!routes.size)throw new Error('No validated RTA bus/streetcar routes available');
}

async function persist(batch:CollectionBatch){
  // Backpressure disconnects the source rather than claiming unsaved observations.
  if(paused||pending>=100){pause('Collection queue reached its ceiling');throw new Error('Collection is paused');}
  pending++;
  try{await sourceClocks.persist(batch,value=>journal.append(value));frames++;lastPersisted=Date.now();}
  catch(e){errors++;pause(safeError(e));throw e;}
  finally{pending--;}
}
function pause(reason:string){
  if(!paused)console.error('[Collection paused]',reason);
  paused=true;es?.close();es=undefined;lepassStop?.();lepassStop=undefined;
}
function capture(data:string,receivedAt:string,eventId?:string){
  const snapshots=captureStreetcarSnapshots(JSON.parse(data),{received_at:receivedAt,source_url:SSE_URL,feed_event_id:eventId},routes);
  const batch:CollectionBatch={schema_version:1,batch_id:randomUUID(),source:'sse',received_at:receivedAt,observations:snapshots.map(s=>({
    observation:{source:'sse',observation_id:s.snapshot_id,vehicle_id:s.vid?`sse:${s.vid}`:null,provider_vehicle_id:s.vid,
      route_id:s.route,trip_id:s.gtfs_trip_id??s.legacy_trip_id,observed_at:s.provider_observed_at?Date.parse(s.provider_observed_at)/1000:null,
      received_at:Date.parse(s.received_at)/1000,lat:s.lat,lon:s.lon,speed_mph:s.speed,off_route:s.is_off_route===true,
      in_service:s.destination&&/not\s+in\s+service/i.test(s.destination)?false:null,
      location_source:'provider_gps',timestamp_precision_seconds:60,direction_id:null,pattern_id:s.pid,mapping_confidence:'verified'} satisfies StudyObservation,
    raw:JSON.parse(s.raw_payload),
  })),provenance:{endpoint:snapshotSourceUrl(SSE_URL),feed_event_id:eventId??null,provider_clock:'minute_truncated',received_at_is_gps_time:false}};
  void persist(batch).catch(e=>console.error('[Journal]',safeError(e)));
}
function connect(){
  if(stopping||paused)return;
  es?.close();es=new EventSource(SSE_URL);const current=es;
  current.onopen=()=>{lastReceived=Date.now();console.log('[SSE] Connected; preserving every validated RTA receipt');};
  current.onmessage=event=>{if(current!==es||stopping||paused)return;lastReceived=Date.now();try{capture(event.data,new Date(lastReceived).toISOString(),event.lastEventId);}catch(e){errors++;console.error('[SSE] Invalid frame:',safeError(e));}};
  current.onerror=()=>{if(current.readyState===EventSource.CLOSED&&!stopping&&!paused){clearTimeout(reconnectTimer);reconnectTimer=setTimeout(connect,5000);}};
}
function supervise(){
  if(stopping||worker||process.env.LOCAL_ANALYSIS_ENABLED==='false')return;
  worker=fork(fileURLToPath(new URL('./local-worker.ts',import.meta.url)),[],{execArgv:['--import','tsx'],stdio:'inherit'});
  worker.on('error',e=>console.error('[Analysis worker]',safeError(e)));
  worker.on('exit',code=>{worker=undefined;if(!stopping){console.error(`[Analysis worker] exited ${code}; journal retained`);restartTimer=setTimeout(supervise,30_000);}});
}
function authorized(value:string|undefined){
  if(!token||!value?.startsWith('Bearer '))return false;
  const a=Buffer.from(value.slice(7)),b=Buffer.from(token);return a.length===b.length&&timingSafeEqual(a,b);
}
const server=createServer(async(req,res)=>{
  try{
  res.setHeader('Content-Type','application/json');res.setHeader('Cache-Control','no-store');
  if(processingHandler&&await processingHandler(req,res))return;
  if(req.url==='/api/health'||req.url==='/health'){res.statusCode=paused?503:200;res.end(JSON.stringify({status:paused?'paused':'ready',last_persisted_at:lastPersisted?new Date(lastPersisted).toISOString():null}));return;}
  if(!authorized(req.headers.authorization)){res.statusCode=401;res.end('{"error":"Authentication required"}');return;}
  if(req.url==='/internal/health'){
    const motherduck_capture=await readFile(join(DATA_DIR,'motherduck-capture-health.json'),'utf8').then(JSON.parse).catch(()=>null);
    res.end(JSON.stringify({paused,frames,pending,errors,analysis:{enabled:process.env.LOCAL_ANALYSIS_ENABLED!=='false',running:!!worker},processing:coordinator?await coordinator.status():undefined,last_received_at:lastReceived?new Date(lastReceived).toISOString():null,last_persisted_at:lastPersisted?new Date(lastPersisted).toISOString():null,lepass:lepassHealth,motherduck_capture,disk:await storageBudget()}));return;
  }
  if(req.url!=='/internal/summary'){res.statusCode=404;res.end('{"error":"Not found"}');return;}
  try{
    const saved=JSON.parse(await readFile(join(DATA_DIR,'summary.json'),'utf8'));
    saved.source_quality=mergeSourceQuality(saved.source_quality,{clocks:sourceClocks.values,paused,lepass:lepassHealth,stale_ms:STALE_MS});
    saved.source_quality.collection={paused,pending_frames:pending,last_received_at:lastReceived?new Date(lastReceived).toISOString():null};
    res.end(JSON.stringify(saved));
  }catch{res.statusCode=503;res.end('{"error":"The first local analysis snapshot is being prepared"}');}
  }catch(error){
    console.error('[Collector HTTP]',safeError(error));
    if(!res.headersSent){res.statusCode=503;res.end('{"error":"Collector status temporarily unavailable"}');}else res.destroy();
  }
});
async function startLePass(){
  // Kept optional during bootstrap; an absent or invalid LePass configuration never blocks SSE.
  try {
    const {startLePassFromEnvironment}=await import('./lepass-collector');
    const collector=await startLePassFromEnvironment({stateDir:join(DATA_DIR,'lepass'),onBatch:persist,onHealth:(health:LePassHealth)=>{lepassHealth=health;}});
    lepassStop=()=>collector.stop();
  }catch(e){lepassHealth={status:'unavailable',message:safeError(e)};console.error('[LePass] Startup unavailable:',safeError(e));}
}
let timer:NodeJS.Timeout;
async function shutdown(){
  if(stopping)return;stopping=true;clearInterval(timer);clearTimeout(restartTimer);clearTimeout(reconnectTimer);es?.close();lepassStop?.();
  await journal.drain();
  await Promise.allSettled([maintenance,scheduleRefresh]);
  server.close();worker?.kill('SIGTERM');
  const deadline=setTimeout(()=>process.exit(0),20_000);deadline.unref();
  if(worker)worker.once('exit',()=>process.exit(0));else process.exit(0);
}
async function main(){
  await mkdir(DATA_DIR,{recursive:true,mode:0o700});await journal.init();
  if(processingServer){
    if(process.env.LOCAL_ANALYSIS_ENABLED!=='false')throw new Error('Processing server must use LOCAL_ANALYSIS_ENABLED=false; workstation workers own the databases');
    const baseline=process.env.PROCESSING_BASELINE_ID;
    if(!baseline||!/^[a-f0-9]{64}$/.test(baseline))throw new Error('Set PROCESSING_BASELINE_ID from the verified workstation seed');
    exchange=new CaptureExchange(DATA_DIR);await exchange.init();
    coordinator=new ProcessingCoordinator({data_dir:DATA_DIR,baseline_id:baseline,analysis_revision:await processingAnalysisRevision(),
      first_service_date:process.env.PROCESSING_FIRST_SERVICE_DATE,due_hour:Number(process.env.PROCESSING_DUE_HOUR||6),lease_seconds:Number(process.env.PROCESSING_LEASE_SECONDS||900)});
    processingHandler=processingRequestHandler({exchange,coordinator,token:process.env.TRANSIT_PROCESSING_TOKEN||'',seal:()=>sealCapture(true)});
    await coordinator.status();
  }
  server.listen(Number(process.env.PORT)||3100,'0.0.0.0');
  await loadRoutes();supervise();
  if((await storageBudget()).allowed){connect();await startLePass();}else pause('Insufficient local disk headroom');
  timer=setInterval(()=>{
    if(!paused&&Date.now()-lastReceived>STALE_MS)connect();
    console.log(`[Collection] frames=${frames} pending=${pending} paused=${paused} errors=${errors}`);
    if(exchange){
      void sealCapture().catch(e=>console.error('[Capture packaging] Original data retained:',safeError(e)));
      if(!scheduleRefresh&&Date.now()-lastScheduleRefresh>6*3600_000){
        lastScheduleRefresh=Date.now()-5.5*3600_000;
        scheduleRefresh=loadRoutes().catch(e=>console.error('[Capture schedule]',safeError(e))).finally(()=>{scheduleRefresh=undefined;});
      }
    }
  },60_000);
  process.on('SIGTERM',()=>void shutdown());process.on('SIGINT',()=>void shutdown());
}
main().catch(e=>{console.error(safeError(e));server.close();process.exitCode=1;});
