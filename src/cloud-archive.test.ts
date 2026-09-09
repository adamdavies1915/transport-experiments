import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile,writeFile,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import {DuckDBInstance,type DuckDBConnection} from '@duckdb/node-api';
import {openLocalStore,ingestBatch,query,sql} from './local-store';
import {compactLocalHistory} from './local-retention';
import {archiveDay,bootstrapMotherDuck,cloudUpload,prepareObservationUpload,commitObservations,pruneVerifiedRemoteDay,selectUploadBatches,storageAccountingFresh,usageAllowsCloud,transientDatabaseSql,UPLOAD_LIMITS} from './cloud-archive';
import type {CollectionBatch} from './observation-types';

const instant=Date.parse('2026-07-01T12:00:00Z')/1000;
function batch(id:string,offset=0,empty=false):CollectionBatch{
  return{schema_version:1,batch_id:id,source:'lepass',received_at:new Date((instant+offset)*1000).toISOString(),predictions:[{id,arrival:instant+100}],observations:empty?[]:[{observation:{source:'lepass',observation_id:id+':0',vehicle_id:'lepass:v',provider_vehicle_id:'v',route_id:'12',trip_id:'trip',observed_at:instant,received_at:instant+offset,lat:29.95,lon:-90.08,speed_mph:null,off_route:false,location_source:'provider_gps',timestamp_precision_seconds:.001,direction_id:'0',pattern_id:'pattern',mapping_confidence:'verified'},raw:{lat:29.95,lon:-90.08}}]};
}
async function fixture(run:(value:{directory:string;local:DuckDBConnection;remote:DuckDBConnection;commands:string[];connection:DuckDBConnection})=>Promise<void>){
  const directory=await mkdtemp(join(tmpdir(),'transit-cloud-')),store=await openLocalStore(directory),db=await DuckDBInstance.create(':memory:'),remote=await db.connect(),commands:string[]=[];
  const previous=Object.fromEntries(['MOTHERDUCK_CLOUD_WRITES','MOTHER_DUCK_API_KEY','MOTHERDUCK_DATABASE','TRANSIT_RESULTS_DATABASE','MOTHERDUCK_USAGE_FILE'].map(key=>[key,process.env[key]]));
  try{
    process.env.MOTHERDUCK_CLOUD_WRITES='true';process.env.MOTHER_DUCK_API_KEY='offline-'+randomUUID();process.env.MOTHERDUCK_DATABASE='my_db';process.env.TRANSIT_RESULTS_DATABASE='transit_results';process.env.MOTHERDUCK_USAGE_FILE=join(directory,'usage.json');
    const now=new Date();await writeFile(process.env.MOTHERDUCK_USAGE_FILE,JSON.stringify({measured_at:now.toISOString(),month:now.toISOString().slice(0,7),compute_cu_hours:1,plan:'lite',billing_mode:'free'}));
    await remote.run("ATTACH ':memory:' AS my_db; CREATE SCHEMA MD_INFORMATION_SCHEMA; CREATE TABLE MD_INFORMATION_SCHEMA.STORAGE_INFO(active_bytes BIGINT,historical_bytes BIGINT,retained_for_clone_bytes BIGINT,failsafe_bytes BIGINT,computed_ts TIMESTAMPTZ); INSERT INTO MD_INFORMATION_SCHEMA.STORAGE_INFO VALUES (1000,0,0,0,now()); CREATE TABLE MD_INFORMATION_SCHEMA.DATABASES(name VARCHAR,transient BOOLEAN)");
    const connection={run:async(statement:string)=>{commands.push(statement);if(statement.startsWith('CREATE DATABASE IF NOT EXISTS')){await remote.run("ATTACH ':memory:' AS transit_results; INSERT INTO MD_INFORMATION_SCHEMA.DATABASES VALUES ('transit_results',true)");return;}return remote.run(statement);},runAndReadAll:async(statement:string)=>{commands.push(statement);return remote.runAndReadAll(statement);}} as unknown as DuckDBConnection;
    await run({directory,local:store.c,remote,commands,connection});
  }finally{
    for(const [key,value] of Object.entries(previous))if(value===undefined)delete process.env[key];else process.env[key]=value;
    store.c.closeSync();store.db.closeSync();remote.closeSync();db.closeSync();await rm(directory,{recursive:true,force:true});
  }
}
test('batch sizing accommodates both feeds but caps rows, bytes and batch count',()=>{
  const rows=Array.from({length:200},(_,i)=>({batch_id:String(i),observations:30,estimated_bytes:1000}));assert.equal(selectUploadBatches(rows).length,200);
  assert.equal(selectUploadBatches([{batch_id:'huge',observations:UPLOAD_LIMITS.rows+1,estimated_bytes:1}]).length,0);
  assert.equal(selectUploadBatches([{batch_id:'huge',observations:1,estimated_bytes:UPLOAD_LIMITS.bytes+1}]).length,0);
  assert.equal(selectUploadBatches(Array.from({length:10001},(_,i)=>({batch_id:String(i),observations:0,estimated_bytes:1}))).length,10000);
  const usage={measured_at:new Date().toISOString(),month:new Date().toISOString().slice(0,7),compute_cu_hours:1,plan:'lite' as const};
  assert.match(usageAllowsCloud(usage,1000)!,/free-tier/);
  assert.match(usageAllowsCloud({...usage,billing_mode:'paid'},1000)!,/free-tier/);
  assert.equal(usageAllowsCloud({...usage,billing_mode:'free'},1000),null);
  assert.equal(storageAccountingFresh('invalid'),false);assert.equal(storageAccountingFresh(new Date(Date.now()+60000).toISOString()),false);
  assert.equal(transientDatabaseSql('transit_results'),'CREATE DATABASE IF NOT EXISTS "transit_results" (TRANSIENT, SNAPSHOT_RETENTION_DAYS 0)');
});
test('remote connection failure returns sanitized cloud health and local collection remains usable',async()=>fixture(async({directory,local})=>{
  await ingestBatch(local,batch('a'));const health=await cloudUpload(local,directory,{connect:async()=>{throw new Error('sensitive connection string');}});
  assert.equal(health.status,'error');assert.doesNotMatch(health.reason,/sensitive/);assert.equal(health.pending_records,1);
  await ingestBatch(local,batch('b',10));assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM collection_receipts'))[0].n,2);
}));
test('archive supersession preserves cold receipts after compaction and late arrival',async()=>fixture(async({directory,local})=>{
  await ingestBatch(local,batch('a'));await ingestBatch(local,batch('b',10));await archiveDay(local,directory,'2026-07-01');
  await compactLocalHistory(local,'2026-07-02');await ingestBatch(local,batch('late',20));await archiveDay(local,directory,'2026-07-01');
  const manifest=JSON.parse(await readFile(join(directory,'archives','2026-07-01','manifest.json'),'utf8'));
  assert.equal(manifest.rows,3);assert.equal((await query<{n:number}>(local,`SELECT COUNT(*) AS n FROM read_parquet(${sql(manifest.file)})`))[0].n,3);
  assert.equal((await readdir(join(directory,'archives','2026-07-01'))).filter(name=>name.endsWith('.parquet')).length,1);
  const upload=await prepareObservationUpload(local,directory);assert.equal(upload?.batches.length,3);assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM collection_receipts'))[0].n,3);
}));
test('exact upload verification acknowledges zero-observation predictions and steady cycles use five statements',async()=>fixture(async({directory,local,remote,connection,commands})=>{
  await ingestBatch(local,batch('a'));await ingestBatch(local,batch('prediction-only',10,true));
  const dependencies={connect:async()=>({c:connection,close:()=>{}})};
  const health=await cloudUpload(local,directory,dependencies);assert.equal(health.status,'ready');assert.equal(health.pending_records,0);
  assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM cloud_batches'))[0].n,2);
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM my_db.observation_samples'))[0].n,1);
  await ingestBatch(local,batch('next',20));commands.length=0;await cloudUpload(local,directory,dependencies);assert.equal(commands.length,5);
  const upload=await prepareObservationUpload(local,directory);assert.equal(upload,null);
}));
test('same-count but changed raw payload fails readback and cannot authorize pruning',async()=>fixture(async({directory,local,remote,connection})=>{
  await ingestBatch(local,batch('a'));await archiveDay(local,directory,'2026-07-01');
  await cloudUpload(local,directory,{connect:async()=>({c:connection,close:()=>{}})});
  await local.run('DELETE FROM cloud_batches');const upload=await prepareObservationUpload(local,directory);assert.ok(upload);
  await remote.run("UPDATE my_db.observation_payloads SET raw='{\"different\":true}'");
  await assert.rejects(commitObservations(local,remote,'my_db',upload),/differs/);
  assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM cloud_batches'))[0].n,0);
  await assert.rejects(pruneVerifiedRemoteDay(local,remote,directory,'my_db','2026-08-01'),/missing remote evidence/);
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM my_db.observation_receipts'))[0].n,1);
}));
test('verified remote pruning touches only covered observation detail and retains batch metadata and legacy tables',async()=>fixture(async({directory,local,remote,connection})=>{
  await ingestBatch(local,batch('a'));await archiveDay(local,directory,'2026-07-01');await cloudUpload(local,directory,{connect:async()=>({c:connection,close:()=>{}})});
  await remote.run('CREATE TABLE my_db.transit_data(value INTEGER); INSERT INTO my_db.transit_data VALUES (123)');
  assert.equal(await pruneVerifiedRemoteDay(local,remote,directory,'my_db','2026-08-01'),'2026-07-01');
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM my_db.observation_receipts'))[0].n,0);
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM my_db.observation_batches'))[0].n,1);
  assert.equal((await query<{value:number}>(remote,'SELECT * FROM my_db.transit_data'))[0].value,123);
}));
test('closed daily results and catalog versions persist once; mutable summaries are never uploaded',async()=>fixture(async({directory,local,remote,connection,commands})=>{
  const network={version:'network-v1',paths:[],sites:[]};
  await local.run(`INSERT INTO study_network_catalogs VALUES ('network-v1',${sql(JSON.stringify(network))})`);
  const body={schema_version:1,date:'2026-07-01',network_version:'network-v1',method:'method-v1',row_cells:[],signal_cells:[],quality:[]};
  await local.run(`INSERT INTO study_daily_results VALUES ('2026-07-01','method-v1','revision-v1',${sql(JSON.stringify(body))},now())`);
  await writeFile(join(directory,'summary.json'),JSON.stringify({schema_version:1,generated_at:new Date().toISOString(),legacy:{arbitrary_mutable_counter:123}}));
  const dependencies={connect:async()=>({c:connection,close:()=>{}})};
  const first=await cloudUpload(local,directory,dependencies);assert.equal(first.status,'ready');
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM transit_results.transit_study_daily_results'))[0].n,1);
  assert.equal((await query<{n:number}>(remote,'SELECT COUNT(*) AS n FROM transit_results.transit_study_networks'))[0].n,1);
  assert.ok(commands.every(statement=>!statement.includes('arbitrary_mutable_counter')&&!statement.includes('transit_summary_sections')));
  commands.length=0;await cloudUpload(local,directory,dependencies);assert.equal(commands.length,1,'only the storage guard contacts cloud once daily results are acknowledged');
}));
test('conflicting late receipt cannot replace an archived observation identity',async()=>fixture(async({directory,local})=>{
  await ingestBatch(local,batch('a'));await archiveDay(local,directory,'2026-07-01');await compactLocalHistory(local,'2026-07-02');
  const changed=batch('conflict',10);changed.observations[0].observation.observation_id='a:0';changed.observations[0].raw={different:true};await ingestBatch(local,changed);
  await assert.rejects(archiveDay(local,directory,'2026-07-01'),/conflicting hot and archived evidence/);
  const manifest=JSON.parse(await readFile(join(directory,'archives','2026-07-01','manifest.json'),'utf8'));assert.equal(manifest.rows,1);
}));

test('bootstrap without a primary key preserves source multiplicity and reuses completed downloads',async()=>fixture(async({directory,local,remote,connection,commands})=>{
  await local.run("INSERT INTO transit_data(vid,timestamp,lat,lon,route) VALUES ('legacy-v','2026-07-01 12:00:00',29.95,-90.08,'12')");
  const source=join(directory,'source.parquet');await local.run(`COPY transit_data TO ${sql(source)} (FORMAT PARQUET)`);
  await remote.run(`CREATE TABLE my_db.transit_data AS SELECT * FROM read_parquet(${sql(source)}); INSERT INTO my_db.transit_data SELECT * FROM read_parquet(${sql(source)})`);
  const dependencies={connect:async()=>({c:connection,close:()=>{}})};
  await bootstrapMotherDuck(local,directory,dependencies);
  assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM transit_data'))[0].n,2);
  await local.run("DELETE FROM local_state WHERE key='bootstrap_complete' OR key='bootstrap:transit_data'");
  commands.length=0;await bootstrapMotherDuck(local,directory,dependencies);
  assert.equal((await query<{n:number}>(local,'SELECT COUNT(*) AS n FROM transit_data'))[0].n,2);
  assert.ok(commands.every(statement=>!statement.startsWith('COPY ')),'verified local Parquet is reused');
}));
