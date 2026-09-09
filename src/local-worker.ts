import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { DuckDBConnection } from '@duckdb/node-api';
import { LocalJournal, DATA_DIR, diskBudget } from './local-journal';
import { openLocalStore, ingestBatch, query, sql, state, setState, loadObservations } from './local-store';
import { bootstrapMotherDuck, cloudUpload, archiveDay, type CloudHealth } from './cloud-archive';
import { refreshSchedule, processRecentDays, pendingOtpBackfillCount } from './otp-worker';
import { readStreetcarNetwork, initializeStreetcars, calculateStreetcarDay, calculateStreetcarWaitDay } from './streetcar-worker';
import { TRANSIT_STUDY_METHOD } from './transit-study';
import { analyzeStudyDay, legacyStudyObservation, LEGACY_STUDY_DATE_SQL, SNAPSHOT_STUDY_DATE_SQL, pendingStudyDaysSql, type LegacyStudyRow } from './local-worker-logic';
import { safeError } from './log-safety';
import { summarizeRowStudy, summarizeSignalStudy, rowStudyCells, signalStudyCells, compareRowCells, summarizeSignalCells } from './transit-study-summary';
import type { StudyCatalog, StudyPassage, StudyEncounter, StudyQuality } from './transit-study-types';
import { compactLocalHistory } from './local-retention';
import { restoreImportedStudyKeys } from './local-schema-repair';
import { compactStudyPublication, collectCompactStudyDays, writeBoundedStudyPublication, type PublicationDay } from './local-publication';
import { prepareDerivedStage, replaceDerivedDay } from './derived-replacement';
import { initializeLegacyBackfillState, pendingLegacyStudyDays, finishLegacyStudyDay, failedLegacyStudyDays } from './local-backfill-state';
import { METHOD as LEGACY_METHOD } from './streetcar-analysis';
import { WAIT_METHOD } from './streetcar-waits';
import type { StudyObservation } from './observation-types';
import { buildLegacySummary } from '../dashboard/src/legacy-summary';
import type { TransitSummaryEnvelope, SourceQualityData } from '../dashboard/src/summary-data';
import { addDays, localDay } from './otp';

let stopped=false;
process.on('SIGTERM',()=>{stopped=true;});process.on('SIGINT',()=>{stopped=true;});
const journal=new LocalJournal(DATA_DIR);
const json=<T>(value:unknown):T=>(typeof value==='string'?JSON.parse(value):value) as T;
const sleep=(ms:number)=>new Promise(resolve=>setTimeout(resolve,ms));
const methodRevision=TRANSIT_STUDY_METHOD;
const backfillMode=process.argv.includes('--backfill');

async function loadCatalog():Promise<StudyCatalog>{
  return JSON.parse(await readFile(process.env.TRANSIT_CATALOG_FILE||fileURLToPath(new URL('./data/transit-study-network.json',import.meta.url)),'utf8'));
}

/** Retained legacy points have no receipt evidence; never manufacture historical signal waits. */
async function legacyObservations(c:DuckDBConnection,date:string,route:string):Promise<StudyObservation[]>{
  const rows=await query<LegacyStudyRow>(c,`SELECT vid,route,COALESCE(gtfs_trip_id,trip_id) AS trip,epoch(observed_at) AS instant,timestamp::VARCHAR AS wall_time,lat,lon,is_off_route,pid
    FROM transit_data WHERE ${LEGACY_STUDY_DATE_SQL}=${sql(date)}::DATE AND route=${sql(route)}
    AND NOT regexp_matches(lower(COALESCE(destination,'')),'not\\s+in\\s+service')`);
  return rows.map(legacyStudyObservation).filter((o):o is StudyObservation=>o!==null);
}
async function historicalSnapshots(c:DuckDBConnection,date:string,route:string):Promise<StudyObservation[]>{
  const rows=await query<Record<string,unknown>>(c,`SELECT snapshot_id,vid,route,COALESCE(gtfs_trip_id,legacy_trip_id) AS trip,epoch(provider_observed_at) AS instant,epoch(received_at) AS receipt,lat,lon,speed,is_off_route,pid
    FROM streetcar_snapshots WHERE ${SNAPSHOT_STUDY_DATE_SQL}=${sql(date)}::DATE AND route=${sql(route)}
    AND NOT regexp_matches(lower(COALESCE(destination,'')),'not\\s+in\\s+service')`);
  return rows.map(r=>({source:'sse',observation_id:String(r.snapshot_id),vehicle_id:r.vid?`sse:${r.vid}`:null,provider_vehicle_id:r.vid==null?null:String(r.vid),route_id:String(r.route),trip_id:r.trip==null?null:String(r.trip),observed_at:r.instant==null?null:Number(r.instant),received_at:Number(r.receipt),lat:r.lat==null?null:Number(r.lat),lon:r.lon==null?null:Number(r.lon),speed_mph:r.speed==null?null:Number(r.speed),off_route:r.is_off_route===true,location_source:'provider_gps',timestamp_precision_seconds:60,direction_id:null,pattern_id:r.pid==null?null:String(r.pid),mapping_confidence:'verified'}));
}
async function processStudyDate(c:DuckDBConnection,catalog:StudyCatalog,date:string,revision:string){
  const passages:StudyPassage[]=[],encounters:StudyEncounter[]=[],quality:StudyQuality[]=[];
  for(const route of [...new Set(catalog.paths.map(p=>p.route_id))]){
    // Archives retain physical receipt days. Include the following receipt day
    // so fresh samples received just after midnight stay on their provider day.
    const dense=[...await loadObservations(c,date,route),...await loadObservations(c,addDays(date,1),route)];
    const historical=await historicalSnapshots(c,date,route),legacy=await legacyObservations(c,date,route);
    const output=analyzeStudyDay(catalog,date,{dense,historical,legacy});
    passages.push(...output.passages);encounters.push(...output.encounters);quality.push(...output.quality);
  }
  await c.run('BEGIN');
  try{
    const eventStage=await prepareDerivedStage(c,'study_events');
    for(const [kind,events] of [['passage',passages],['encounter',encounters],['quality',quality]] as const){
      for(let i=0;i<events.length;i+=500){
        const values=events.slice(i,i+500).map((e,n)=>`(${[kind==='quality'?`quality:${date}:${i+n}`:(e as StudyPassage).id,date,kind,e.source,JSON.stringify(e)].map(sql).join(',')})`);
        await c.run(`INSERT INTO ${eventStage} VALUES ${values.join(',')}`);
      }
    }
    await replaceDerivedDay(c,{table:'study_events',stage:eventStage,date_column:'date',date,keys:['id']});
    await c.run(`INSERT INTO study_dates VALUES (${sql(date)},${sql(revision)},${sql(catalog.version+':'+methodRevision)},now()) ON CONFLICT(date) DO UPDATE SET source_revision=excluded.source_revision,method_revision=excluded.method_revision,updated_at=excluded.updated_at`);
    const daily={schema_version:1,date,network_version:catalog.version,method:methodRevision,row_cells:rowStudyCells(catalog,passages),signal_cells:signalStudyCells(catalog,encounters),quality};
    await c.run(`INSERT INTO study_daily_results VALUES (${sql(date)},${sql(catalog.version+':'+methodRevision)},${sql(revision)},${sql(JSON.stringify(daily))},now()) ON CONFLICT(date) DO UPDATE SET method_revision=excluded.method_revision,source_revision=excluded.source_revision,body=excluded.body,updated_at=excluded.updated_at`);
    await c.run('COMMIT');
  }catch(e){await c.run('ROLLBACK');throw e;}
  console.log(`[Study] ${date}: ${passages.length} passages, ${encounters.length} complete signal encounters`);
}
async function pendingStudyDays(c:DuckDBConnection,catalog:StudyCatalog){
  return query<{date:string;revision:string}>(c,pendingStudyDaysSql(catalog.version+':'+methodRevision));
}
async function sourceQuality(c:DuckDBConnection):Promise<SourceQualityData>{
  const rows=await query<{source:string;n:number;first:number;last:number;provider:number|null}>(c,`SELECT b.source,SUM(b.observations) AS n,MIN(epoch(b.received_at)) AS first,MAX(epoch(b.received_at)) AS last,MAX(p.provider) AS provider
    FROM collection_batches b LEFT JOIN (SELECT source,MAX(observed_at) AS provider FROM collection_payloads GROUP BY source) p USING(source) GROUP BY b.source`);
  const cloud=await state<CloudHealth>(c,'cloud_health');
  const sources=['sse','lepass'].map(id=>{
    const r=rows.find(r=>r.source===id);return {id,label:id==='sse'?'RTA relay':'LePass',status:!r?'collecting' as const:Date.now()/1000-r.last>300?'degraded' as const:'ready' as const,observations:r?.n??0,last_received_at:r?new Date(r.last*1000).toISOString():null,last_provider_at:r?.provider?new Date(r.provider*1000).toISOString():null,from:r?new Date(r.first*1000).toISOString():null,to:r?new Date(r.last*1000).toISOString():null};
  });
  return {status:sources.some(s=>s.status==='ready')?'ready':'collecting',sources,archive:{last_uploaded_at:cloud?.last_uploaded_at,pending_records:cloud?.pending_records,storage_bytes:cloud?.storage_bytes??undefined,budget_bytes:cloud?.budget_bytes},limitations:['Receipt time is not GPS fix time.','LePass and the relay may share an upstream vehicle feed; they are not independent samples.',cloud?.reason??'Cloud usage is being checked.']};
}
async function publish(c:DuckDBConnection,catalog:StudyCatalog){
  // Read one daily body at a time and compact before retaining it. Loading all
  // detailed identity lists first would make historical publication memory grow
  // with the number of runs rather than the number of public coverage cells.
  const stored=await query<{date:string}>(c,`SELECT date::VARCHAR AS date FROM study_daily_results WHERE method_revision=${sql(catalog.version+':'+methodRevision)} ORDER BY date DESC`);
  const firstPublishedDay=addDays(localDay(Date.now()/1000,'America/Chicago'),-89);
  const saved=stored.filter(d=>d.date>=firstPublishedDay).slice(0,90);
  const rowBase=summarizeRowStudy(catalog,[]),signalBase=summarizeSignalStudy(catalog,[]);
  const days=await collectCompactStudyDays(saved.map(d=>d.date),async date=>{
    const [r]=await query<{body:string}>(c,`SELECT body::VARCHAR AS body FROM study_daily_results WHERE date=${sql(date)}::DATE`);
    return json<PublicationDay>(r.body);
  },rowBase,signalBase);
  const {row_cells:rowCells,coverage_cells:coverageCells,signal_cells:signalCells,quality}=days;
  const row=summarizeRowStudy(catalog,[],quality),signals=summarizeSignalStudy(catalog,[],quality);
  row.coverage_cells=coverageCells;
  row.cells=rowCells;row.comparisons=compareRowCells(rowCells);row.status=row.comparisons.some(c=>c.status==='ready')?'ready':'collecting';
  row.coverage={passages:rowCells.reduce((n,c)=>n+c.passages,0),classified_passages:rowCells.filter(c=>c.row_class!=='unknown').reduce((n,c)=>n+c.passages,0),unknown_passages:rowCells.filter(c=>c.row_class==='unknown').reduce((n,c)=>n+c.passages,0)};
  signals.cells=signalCells;signals.signals=summarizeSignalCells(signalCells);signals.status=signals.signals.some(s=>s.status==='ready')?'ready':'collecting';
  const compact=compactStudyPublication(row,signals);
  const envelope:TransitSummaryEnvelope={schema_version:1,generated_at:new Date().toISOString(),source_quality:await sourceQuality(c),row_study:compact.row,signal_study:compact.signal,legacy:await buildLegacySummary(statement=>query(c,statement),'transit')};
  const publication=await writeBoundedStudyPublication(join(DATA_DIR,'summary.json'),envelope,{available_dates:stored.map(d=>d.date),included_dates:days.retained_dates});
  await setState(c,'last_summary',{generated_at:envelope.generated_at,bytes:publication.bytes,omitted_dates:publication.omitted_dates});
}
async function main(){
  await journal.init();
  if(!(await diskBudget(DATA_DIR)).allowed){console.error('[Worker] Disk ceiling reached; analysis/bootstrap paused');return;}
  const {c,db}=await openLocalStore(DATA_DIR);
  try{
    const catalog=await loadCatalog(),legacyNetwork=await readStreetcarNetwork();
    const legacyRevision=[legacyNetwork.version,LEGACY_METHOD.name,WAIT_METHOD.name,'atomic-refresh-v1'].join(':');
    await restoreImportedStudyKeys(c);
    // Create keyed tables before Parquet imports; CREATE TABLE AS drops keys.
    await initializeStreetcars(c,legacyNetwork);
    await initializeLegacyBackfillState(c);
    if(process.env.MOTHERDUCK_BOOTSTRAP!=='false')try{await bootstrapMotherDuck(c,DATA_DIR);}catch(e){console.error('[Bootstrap] Cloud history unavailable; continuing local collection:',safeError(e));}
    await c.run(`INSERT INTO study_network_catalogs VALUES (${sql(catalog.version)},${sql(JSON.stringify(catalog))}) ON CONFLICT DO NOTHING`);
    await publish(c,catalog);
    let lastAnalysis=0,lastCloud=0;
    while(!stopped){
      for(const file of await journal.files()){
        if(stopped)break;
        try{const batch=await journal.read(file);await ingestBatch(c,batch);await journal.acknowledge(file);}
        catch(e){console.error('[Journal ingest] Retained unacknowledged frame:',safeError(e));break;}
      }
      const now=Date.now();
      if(now-lastCloud>=Math.max(900_000,Number(process.env.UPLOAD_INTERVAL)||900_000)){
        try{await cloudUpload(c,DATA_DIR);}catch(e){console.error('[Cloud] Upload failed; local collection continues:',safeError(e));}lastCloud=now;
      }
      if(backfillMode||now-lastAnalysis>=Math.max(60_000,Number(process.env.ANALYSIS_INTERVAL)||900_000)){
        try{await refreshSchedule(c);}catch(e){console.error('[OTP] Schedule refresh unavailable; using retained schedules:',safeError(e));}
        let otpFailure:string|null=null;
        try{await processRecentDays(c);}catch(e){otpFailure=safeError(e);console.error('[OTP] Local cycle:',otpFailure);}
        const days=await pendingStudyDays(c,catalog);
        for(const day of days){
          if(stopped)break;
          await processStudyDate(c,catalog,day.date,day.revision);
          if(day.date<localDay(Date.now()/1000,'America/Chicago'))await archiveDay(c,DATA_DIR,day.date);
          if(!backfillMode)await publish(c,catalog);
        }
        const legacyDays=await pendingLegacyStudyDays(c,legacyRevision);
        for(const day of legacyDays){
          if(stopped)break;
          try{
            await calculateStreetcarDay(c,legacyNetwork,day.date);
            await calculateStreetcarWaitDay(c,legacyNetwork,day.date);
            await finishLegacyStudyDay(c,day.date,day.revision,legacyRevision);
          }catch(e){
            const error=safeError(e);await finishLegacyStudyDay(c,day.date,day.revision,legacyRevision,error);
            console.error(`[Legacy study] ${day.date} remains pending:`,error);
          }
        }
        await publish(c,catalog);
        await compactLocalHistory(c,addDays(localDay(Date.now()/1000,'America/Chicago'),-7));
        if(backfillMode&&!days.length&&!legacyDays.length){
          const failed=await failedLegacyStudyDays(c,legacyRevision);
          if(failed.length)throw new Error(`Legacy backfill remains incomplete for ${failed.join(', ')}`);
          const otpPending=await pendingOtpBackfillCount(c);
          if(!otpPending)break;
          if(otpFailure)throw new Error(`OTP backfill still has ${otpPending} requested dates pending: ${otpFailure}`);
        }
        lastAnalysis=Date.now();
      }
      await sleep(5000);
    }
    await c.run('CHECKPOINT');
  }finally{c.closeSync();db.closeSync();}
}
main().catch(e=>{const token=process.env.MOTHER_DUCK_API_KEY;console.error('[Local worker]',token?safeError(e).replaceAll(token,'[redacted]'):safeError(e));process.exitCode=1;});
