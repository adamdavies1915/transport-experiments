import { DuckDBInstance, type DuckDBConnection } from '@duckdb/node-api';
import { createHash } from 'node:crypto';
import { readFile, mkdir, stat, readdir, unlink, open } from 'node:fs/promises';
import { join, resolve, dirname } from 'node:path';
import { createReadStream } from 'node:fs';
import { atomicFile } from './local-journal';
import { ident, query, setState, sql, state } from './local-store';
import { restoreArchivedBatches } from './local-retention';
import { addDays, localDay } from './otp';

export interface CloudUsage { measured_at:string; month:string; compute_cu_hours:number; plan:'lite'; billing_mode?:'free'|'paid' }
export interface CloudHealth {
  status:'paused'|'ready'|'error'; reason:string; checked_at:string; storage_bytes:number|null;
  budget_bytes:number; compute_cu_hours:number|null; last_uploaded_at:string|null; pending_records:number;
  storage_basis?:'active'|'total_accounted'; total_accounted_storage_bytes?:number|null;
}
export const STORAGE_BUDGET=8_000_000_000;
export const UPLOAD_LIMITS={batches:10_000,rows:100_000,bytes:32*1024*1024};
export function usageAllowsCloud(usage:CloudUsage|undefined,storageBytes:number|null,at=Date.now()):string|null {
  if(!usage||usage.plan!=='lite'||usage.billing_mode!=='free'||usage.month!==new Date(at).toISOString().slice(0,7))return 'Current Lite free-tier billing mode and usage have not been verified';
  if(!Number.isFinite(Date.parse(usage.measured_at))||at-Date.parse(usage.measured_at)>6*3600_000||Date.parse(usage.measured_at)>at)return 'Billing usage verification is stale';
  if(!Number.isFinite(usage.compute_cu_hours)||usage.compute_cu_hours<0||usage.compute_cu_hours>=8)return 'Monthly compute operating ceiling reached';
  if(storageBytes===null||!Number.isFinite(storageBytes)||storageBytes<0||storageBytes>=STORAGE_BUDGET)return 'Total accounted storage is above the operating ceiling';
  return null;
}
export function storageAccountingFresh(timestamp:string|null|undefined,at=Date.now()):boolean {
  const value=timestamp==null?NaN:Date.parse(timestamp);return Number.isFinite(value)&&value<=at&&at-value<=6*3600_000;
}
export interface PendingBatch {batch_id:string;observations:number;estimated_bytes:number}
export function selectUploadBatches(rows:PendingBatch[]):PendingBatch[]{
  const result:PendingBatch[]=[];let count=0,bytes=0;
  for(const row of rows){
    if(result.length>=UPLOAD_LIMITS.batches||!Number.isSafeInteger(row.observations)||row.observations<0||!Number.isFinite(row.estimated_bytes)||row.estimated_bytes<0||count+row.observations>UPLOAD_LIMITS.rows||bytes+row.estimated_bytes>UPLOAD_LIMITS.bytes)break;
    result.push(row);count+=row.observations;bytes+=row.estimated_bytes;
  }
  return result;
}
export interface CloudRemote {c:DuckDBConnection;close:()=>void}
export interface CloudDependencies {connect?:(database:string,token:string)=>Promise<CloudRemote>;now?:()=>number}
async function connectCloud(database:string,token:string):Promise<CloudRemote>{
  const db=await DuckDBInstance.create(`md:${database}?motherduck_token=${encodeURIComponent(token)}`);
  try{const c=await db.connect();return{c,close:()=>{try{c.closeSync();}finally{db.closeSync();}}};}
  catch(error){db.closeSync();throw error;}
}
const columns='observation_id,payload_id,received_at,service_date,batch_id,source,vehicle_id,route_id,observed_at,body,raw';
const digest=(bytes:string|Uint8Array)=>createHash('sha256').update(bytes).digest('hex');
async function syncFile(path:string){const file=await open(path,'r');try{await file.sync();}finally{await file.close();}}
async function fileDigest(path:string){const hash=createHash('sha256');for await(const chunk of createReadStream(path))hash.update(chunk);return hash.digest('hex');}
const validDay=(value:string)=>/^\d{4}-\d{2}-\d{2}$/.test(value)&&new Date(`${value}T00:00:00Z`).toISOString().slice(0,10)===value;
export interface ArchiveManifest {schema_version:1;date:string;file:string;rows:number;last_received_at:number;sha256:string;verified_at:string}
async function checkedArchive(directory:string,date:string):Promise<ArchiveManifest>{
  if(!validDay(date))throw new Error('Invalid archive day');
  const folder=resolve(directory,'archives',date),value=JSON.parse(await readFile(join(folder,'manifest.json'),'utf8')) as ArchiveManifest;
  if(value.schema_version!==1||value.date!==date||dirname(resolve(value.file))!==folder||!/^observations-[a-zA-Z0-9-]+\.parquet$/.test(value.file.split('/').at(-1)!)||!Number.isSafeInteger(value.rows)||value.rows<0||!/^[a-f0-9]{64}$/.test(value.sha256))throw new Error('Invalid archive manifest');
  if(await fileDigest(value.file)!==value.sha256)throw new Error('Archive checksum mismatch');
  await syncFile(value.file);
  return value;
}
/** Exact row/content comparison, including identity, source clocks and raw payloads. */
export async function exactParquetMatch(c:DuckDBConnection,left:string,right:string):Promise<boolean>{
  const [{n}]=await query<{n:number}>(c,`SELECT COUNT(*) AS n FROM ((SELECT * FROM (${left}) EXCEPT ALL SELECT * FROM (${right})) UNION ALL (SELECT * FROM (${right}) EXCEPT ALL SELECT * FROM (${left})))`);
  return n===0;
}

/** Daily files are superseded only after full content verification and durable metadata. */
export async function archiveDay(c:DuckDBConnection,directory:string,date:string){
  if(!validDay(date))throw new Error('Invalid archive date');
  const [{n:hotCount}]=await query<{n:number}>(c,`SELECT COUNT(*) AS n FROM collection_receipts WHERE service_date=${sql(date)}::DATE`);
  if(!hotCount)return;
  const folder=join(directory,'archives',date);await mkdir(folder,{recursive:true,mode:0o700});
  let existing:ArchiveManifest|undefined;
  try{existing=await checkedArchive(directory,date);}catch(error){
    const catalog=await query(c,`SELECT 1 FROM local_archive_catalog WHERE date=${sql(date)}::DATE`);
    if(catalog.length||(error as NodeJS.ErrnoException).code!=='ENOENT')throw new Error('Existing cold archive cannot be verified; it must not be replaced by partial hot data');
  }
  const hot=`SELECT ${columns} FROM collected_observations WHERE service_date=${sql(date)}::DATE`;
  const cold=existing?`SELECT ${columns} FROM read_parquet(${sql(existing.file)})`:null;
  if(cold){
    const [{conflicts}]=await query<{conflicts:number}>(c,`SELECT COUNT(*) AS conflicts FROM (SELECT * FROM (${hot}) WHERE observation_id IN (SELECT observation_id FROM (${cold})) EXCEPT ALL ${cold})`);
    if(conflicts)throw new Error('An observation identity has conflicting hot and archived evidence');
  }
  const merged=cold?`${cold} UNION ${hot}`:hot;
  const [{n,last}]=await query<{n:number;last:number}>(c,`SELECT COUNT(*) AS n,MAX(received_at) AS last FROM (${merged})`);
  if(existing&&existing.rows===n&&await exactParquetMatch(c,merged,cold!)){
    await c.run(`INSERT INTO local_archive_catalog VALUES (${sql(date)},${sql(existing.file)},${n},${last},${sql(existing.sha256)},${sql(existing.verified_at)}) ON CONFLICT(date) DO UPDATE SET path=excluded.path,rows=excluded.rows,last_received_at=excluded.last_received_at,sha256=excluded.sha256,verified_at=excluded.verified_at`);return;
  }
  const file=resolve(folder,`observations-${n}-${Date.now()}.parquet`);
  await c.run(`COPY (SELECT * FROM (${merged}) ORDER BY source,vehicle_id,received_at) TO ${sql(file)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
  if(!await exactParquetMatch(c,merged,`SELECT ${columns} FROM read_parquet(${sql(file)})`))throw new Error('Local archive differs from observation evidence');
  await syncFile(file);
  const manifest:ArchiveManifest={schema_version:1,date,file,rows:n,last_received_at:last,sha256:await fileDigest(file),verified_at:new Date().toISOString()};
  await atomicFile(join(folder,'manifest.json'),JSON.stringify(manifest));
  await c.run(`INSERT INTO local_archive_catalog VALUES (${sql(date)},${sql(file)},${n},${last},${sql(manifest.sha256)},${sql(manifest.verified_at)}) ON CONFLICT(date) DO UPDATE SET path=excluded.path,rows=excluded.rows,last_received_at=excluded.last_received_at,sha256=excluded.sha256,verified_at=excluded.verified_at`);
  await setState(c,`archive:${date}`,n);
  for(const name of await readdir(folder))if(/^observations-[a-zA-Z0-9-]+\.parquet$/.test(name)&&resolve(folder,name)!==file)await unlink(join(folder,name));
}
/** Explicit migration, once: original tables are downloaded, never modified remotely. */
export async function bootstrapMotherDuck(local: DuckDBConnection, directory: string, dependencies: CloudDependencies = {}): Promise<void> {
  if (!process.env.MOTHER_DUCK_API_KEY || await state(local,'bootstrap_complete')) return;
  const remoteName = process.env.MOTHERDUCK_DATABASE || 'my_db';
  const remote = await (dependencies.connect ?? connectCloud)(remoteName,process.env.MOTHER_DUCK_API_KEY);
  const c = remote.c;
  const target = join(directory,'bootstrap');
  try {
    await mkdir(target,{recursive:true,mode:0o700});
    const tables = await query<{table_name:string}>(c,`SELECT table_name FROM information_schema.tables WHERE table_catalog=${sql(remoteName)} AND table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name`);
    const allowed = /^(transit_data|otp_[a-z_]+|streetcar_[a-z_]+)$/;
    for (const {table_name:table} of tables.filter(t=>allowed.test(t.table_name))) {
      if(await state(local,`bootstrap:${table}`))continue;
      const file=join(target,`${table}.parquet`);
      let reusable=false;
      try {
        const [{n}]=await query<{n:number}>(local,`SELECT COUNT(*) AS n FROM read_parquet(${sql(file)})`);
        if(Number.isSafeInteger(n)&&n>=0){
          const sha256=await fileDigest(file);await syncFile(file);
          let saved:{sha256:string;rows:number}|undefined;
          try{saved=JSON.parse(await readFile(file+'.manifest.json','utf8'));}catch{}
          reusable=!saved||(saved.sha256===sha256&&saved.rows===n);
          if(reusable&&!saved)await atomicFile(file+'.manifest.json',JSON.stringify({rows:n,sha256,verified_at:new Date().toISOString(),provenance:'recovered completed local Parquet copy; original remote cutoff unknown'}));
        }
      }catch{}
      if(!reusable){
        await c.run(`COPY (SELECT * FROM ${ident(remoteName)}.${ident(table)}) TO ${sql(file)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
        const [{n}]=await query<{n:number}>(local,`SELECT COUNT(*) AS n FROM read_parquet(${sql(file)})`);
        await syncFile(file);
        await atomicFile(file+'.manifest.json',JSON.stringify({rows:n,sha256:await fileDigest(file),verified_at:new Date().toISOString(),provenance:'read-only MotherDuck COPY'}));
      }
      const exists=(await query(local,`SELECT 1 FROM information_schema.tables WHERE table_catalog=current_database() AND table_name=${sql(table)}`)).length;
      await local.run('BEGIN');
      try {
        if(exists&&table==='transit_data'){
          const sourceColumns=await query<{column_name:string}>(local,`DESCRIBE SELECT * FROM read_parquet(${sql(file)})`);
          const list=sourceColumns.map(row=>ident(row.column_name)).join(',');
          // transit_data has no primary key. Multiset subtraction preserves source duplicates
          // while avoiding duplicate evidence after an interrupted/repeated migration.
          await local.run(`INSERT INTO ${ident(table)} BY NAME SELECT ${list} FROM read_parquet(${sql(file)}) EXCEPT ALL SELECT ${list} FROM ${ident(table)}`);
        }else if(exists)await local.run(`INSERT INTO ${ident(table)} BY NAME SELECT * FROM read_parquet(${sql(file)}) ON CONFLICT DO NOTHING`);
        else await local.run(`CREATE TABLE ${ident(table)} AS SELECT * FROM read_parquet(${sql(file)})`);
        const [{n}]=await query<{n:number}>(local,`SELECT COUNT(*) AS n FROM read_parquet(${sql(file)})`);
        await setState(local,`bootstrap:${table}`,{rows:n,at:new Date().toISOString()});
        await local.run('COMMIT');
        console.log(`[Bootstrap] Preserved ${table}: ${n} rows locally`);
      } catch(e) { await local.run('ROLLBACK'); throw e; }
    }
    await local.run('INSERT INTO local_watermarks SELECT vid,MAX(observed_at) FROM transit_data WHERE observed_at IS NOT NULL GROUP BY vid ON CONFLICT(vid) DO UPDATE SET observed_at=greatest(local_watermarks.observed_at,excluded.observed_at)');
    await setState(local,'bootstrap_complete',{at:new Date().toISOString()});
  } finally { remote.close(); }
}


interface UploadFiles {batches:PendingBatch[];receipts:string;payloads:string;batchFile:string;bytes:number}
export async function prepareObservationUpload(local:DuckDBConnection,directory:string):Promise<UploadFiles|null>{
  const candidates=await query<PendingBatch>(local,`SELECT batch_id,observations,COALESCE(octet_length(encode(predictions::VARCHAR)),0)+COALESCE(octet_length(encode(provenance::VARCHAR)),0)+256 AS estimated_bytes FROM collection_batches WHERE batch_id NOT IN (SELECT id FROM cloud_batches) ORDER BY received_at,batch_id LIMIT ${UPLOAD_LIMITS.batches}`);
  let batches=selectUploadBatches(candidates);if(!batches.length){if(candidates.length)throw new Error('An individual pending batch exceeds the upload limit');return null;}
  const folder=join(directory,'upload');await mkdir(folder,{recursive:true,mode:0o700});
  const receipts=join(folder,'receipts.parquet'),payloads=join(folder,'payloads.parquet'),batchFile=join(folder,'batches.parquet');
  await restoreArchivedBatches(local,batches.map(row=>row.batch_id));
  for(;;){
    const ids=batches.map(row=>sql(row.batch_id)).join(',');
    const [{n}]=await query<{n:number}>(local,`SELECT COUNT(*) AS n FROM collection_receipts WHERE batch_id IN (${ids})`);
    if(n!==batches.reduce((total,row)=>total+row.observations,0))throw new Error('Pending batch receipt restoration is incomplete');
    const [{missing}]=await query<{missing:number}>(local,`SELECT COUNT(*) AS missing FROM collection_receipts r LEFT JOIN collection_payloads p USING(payload_id) JOIN collection_batches b USING(batch_id) WHERE r.batch_id IN (${ids}) AND (p.payload_id IS NULL OR p.source<>b.source)`);
    if(missing)throw new Error('Pending receipts lack matching source payloads');
    await local.run(`COPY (SELECT * FROM collection_receipts WHERE batch_id IN (${ids})) TO ${sql(receipts)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    await local.run(`COPY (SELECT * FROM collection_payloads WHERE payload_id IN (SELECT payload_id FROM collection_receipts WHERE batch_id IN (${ids}))) TO ${sql(payloads)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    await local.run(`COPY (SELECT * FROM collection_batches WHERE batch_id IN (${ids})) TO ${sql(batchFile)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    const bytes=(await stat(receipts)).size+(await stat(payloads)).size+(await stat(batchFile)).size;
    if(bytes<=UPLOAD_LIMITS.bytes)return{batches,receipts,payloads,batchFile,bytes};
    if(batches.length===1)throw new Error('An individual pending batch exceeds the compressed upload limit');
    batches=batches.slice(0,Math.max(1,Math.floor(batches.length/2)));
  }
}
const initializedSchemas=new Set<string>();
async function observationSchema(c:DuckDBConnection,database:string,cacheKey:string){
  if(initializedSchemas.has(cacheKey))return;
  const prefix=ident(database)+'.';
  await c.run(`CREATE TABLE IF NOT EXISTS ${prefix}observation_payloads (payload_id VARCHAR PRIMARY KEY,source VARCHAR,vehicle_id VARCHAR,route_id VARCHAR,observed_at DOUBLE,body JSON,raw JSON)`);
  await c.run(`CREATE TABLE IF NOT EXISTS ${prefix}observation_receipts (observation_id VARCHAR PRIMARY KEY,payload_id VARCHAR,received_at DOUBLE,service_date DATE,batch_id VARCHAR)`);
  await c.run(`CREATE TABLE IF NOT EXISTS ${prefix}observation_batches (batch_id VARCHAR PRIMARY KEY,source VARCHAR,received_at TIMESTAMPTZ,observations INTEGER,predictions JSON,provenance JSON)`);
  await c.run(`CREATE OR REPLACE VIEW ${prefix}observation_samples AS SELECT r.*,p.* EXCLUDE(payload_id) FROM ${prefix}observation_receipts r JOIN ${prefix}observation_payloads p USING(payload_id) JOIN ${prefix}observation_batches b ON b.batch_id=r.batch_id AND b.source=p.source`);
  initializedSchemas.add(cacheKey);
}
export async function commitObservations(local:DuckDBConnection,c:DuckDBConnection,database:string,upload:UploadFiles){
  const prefix=ident(database)+'.',ids=upload.batches.map(row=>sql(row.batch_id)).join(',');
  // Separate idempotent appends are safe to retry after partial failure. Batch metadata is
  // written last and acts as a completion marker; no per-cycle BEGIN/COMMIT is required.
  for(const [table,path] of [['observation_payloads',upload.payloads],['observation_receipts',upload.receipts],['observation_batches',upload.batchFile]])await c.run(`INSERT INTO ${prefix}${table} SELECT * FROM read_parquet(${sql(path)}) ON CONFLICT DO NOTHING`);
  const checks=[['observation_payloads',upload.payloads,'payload_id'],['observation_receipts',upload.receipts,'observation_id'],['observation_batches',upload.batchFile,'batch_id']].map(([table,path,key])=>{
    const expected=`SELECT * FROM read_parquet(${sql(path)})`,actual=`SELECT * FROM ${prefix}${table} WHERE ${key} IN (SELECT ${key} FROM read_parquet(${sql(path)}))`;
    return `SELECT COUNT(*) AS n FROM ((${actual} EXCEPT ALL ${expected}) UNION ALL (${expected} EXCEPT ALL ${actual}))`;
  });
  const [{n}]=await query<{n:number}>(c,`SELECT SUM(n) AS n FROM (${checks.join(' UNION ALL ')})`);
  if(n!==0)throw new Error('Cloud readback differs from local evidence');
  await local.run(`INSERT INTO cloud_batches SELECT b.batch_id,COALESCE(MIN(r.received_at),epoch(b.received_at)),COALESCE(MAX(r.received_at),epoch(b.received_at)),COUNT(r.observation_id),now() FROM collection_batches b LEFT JOIN collection_receipts r USING(batch_id) WHERE b.batch_id IN (${ids}) GROUP BY b.batch_id,b.received_at ON CONFLICT DO NOTHING`);
}

/** Only our oldest observation receipts/payloads may be removed; all original tables stay intact.
 * No immediate storage saving is assumed: accounting/failsafe must catch up before uploading. */
export async function pruneVerifiedRemoteDay(local:DuckDBConnection,c:DuckDBConnection,directory:string,database:string,cutoff:string):Promise<string|null>{
  const prefix=ident(database)+'.';
  const days=await query<{date:string}>(c,`SELECT service_date::VARCHAR AS date FROM ${prefix}observation_receipts WHERE service_date<${sql(cutoff)}::DATE GROUP BY service_date ORDER BY service_date LIMIT 1`);
  if(!days.length)return null;
  const date=days[0].date,archive=await checkedArchive(directory,date);
  const [catalog]=await query<{sha256:string;path:string;rows:number}>(local,`SELECT sha256,path,rows FROM local_archive_catalog WHERE date=${sql(date)}::DATE`);
  if(!catalog||catalog.sha256!==archive.sha256||resolve(catalog.path)!==resolve(archive.file)||catalog.rows!==archive.rows)throw new Error('Archive metadata does not match the verified catalog');
  const remoteRows=`SELECT r.observation_id,r.payload_id,r.received_at,r.service_date,r.batch_id,p.source,p.vehicle_id,p.route_id,p.observed_at,p.body,p.raw FROM ${prefix}observation_receipts r LEFT JOIN ${prefix}observation_payloads p USING(payload_id) WHERE r.service_date=${sql(date)}::DATE`;
  await c.run('BEGIN');
  try{
    const [{missing}]=await query<{missing:number}>(c,`SELECT COUNT(*) AS missing FROM (${remoteRows} EXCEPT ALL SELECT ${columns} FROM read_parquet(${sql(archive.file)}))`);
    if(missing)throw new Error('Local archive is missing remote evidence');
    // Recheck the complete local file immediately before deleting remote rows.
    if(await fileDigest(archive.file)!==archive.sha256)throw new Error('Archive changed during verification');
    await c.run(`DELETE FROM ${prefix}observation_receipts WHERE service_date=${sql(date)}::DATE`);
    await c.run(`DELETE FROM ${prefix}observation_payloads WHERE payload_id IN (SELECT payload_id FROM read_parquet(${sql(archive.file)})) AND payload_id NOT IN (SELECT payload_id FROM ${prefix}observation_receipts)`);
    await c.run('COMMIT');
  }catch(error){await c.run('ROLLBACK');throw error;}
  await setState(local,`cloud_pruned:${date}`,{date,sha256:archive.sha256,archive_rows:archive.rows,pruned_at:new Date().toISOString()});
  return date;
}

/** Official syntax: https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-database/ */
export function transientDatabaseSql(database:string){return `CREATE DATABASE IF NOT EXISTS ${ident(database)} (TRANSIENT, SNAPSHOT_RETENTION_DAYS 0)`;}
async function resultsSchema(c:DuckDBConnection,database:string,cacheKey:string){
  if(initializedSchemas.has(cacheKey))return;
  await c.run(transientDatabaseSql(database));
  const [info]=await query<{transient:boolean}>(c,`SELECT transient FROM MD_INFORMATION_SCHEMA.DATABASES WHERE name=${sql(database)}`);
  if(info?.transient!==true)throw new Error('Results database is not verified transient');
  const prefix=ident(database)+'.';
  await c.run(`CREATE TABLE IF NOT EXISTS ${prefix}transit_study_networks (version VARCHAR PRIMARY KEY,digest VARCHAR,body JSON)`);
  await c.run(`CREATE TABLE IF NOT EXISTS ${prefix}transit_study_daily_results (date DATE,method_revision VARCHAR,source_revision VARCHAR,body JSON,updated_at TIMESTAMPTZ,PRIMARY KEY(date,method_revision,source_revision))`);
  initializedSchemas.add(cacheKey);
}
/** Only closed daily versions are archived. Mutable public snapshots remain local. */
async function persistResults(local:DuckDBConnection,c:DuckDBConnection,directory:string,database:string,remainingBytes:number,cacheKey:string):Promise<number>{
  const exists=(await query(local,"SELECT 1 FROM information_schema.tables WHERE table_name='study_daily_results' AND table_catalog=current_database()")).length;
  if(!exists)return 0;
  let days=await query<{date:string;method_revision:string;source_revision:string;network_version:string}>(local,"SELECT date::VARCHAR AS date,method_revision,source_revision,body->>'network_version' AS network_version FROM study_daily_results d WHERE date<timezone('America/Chicago',now())::DATE AND NOT EXISTS(SELECT 1 FROM local_state s WHERE s.key='cloud_daily:'||d.date||':'||d.method_revision||':'||d.source_revision) ORDER BY date LIMIT 10");
  if(!days.length)return 0;
  const networks=new Map<string,string>();
  const hasCatalogs=(await query(local,"SELECT 1 FROM information_schema.tables WHERE table_name='study_network_catalogs' AND table_catalog=current_database()")).length;
  if(hasCatalogs){for(const row of await query<{version:string;body:string}>(local,`SELECT version,body::VARCHAR AS body FROM study_network_catalogs WHERE version IN (${[...new Set(days.map(day=>day.network_version))].map(sql).join(',')})`))networks.set(row.version,row.body);}
  // Compatibility with the first local-worker version, which only saved the current catalog.
  if(days.some(day=>!networks.has(day.network_version))){try{
    const saved=JSON.parse(await readFile(join(directory,'summary.json'),'utf8'));
    const network=saved.row_study?.network??saved.signal_study?.network;
    if(network?.version)networks.set(network.version,JSON.stringify(network));
  }catch{}}
  days=days.filter(day=>networks.has(day.network_version));if(!days.length)return 0;
  const folder=join(directory,'upload');await mkdir(folder,{recursive:true,mode:0o700});
  const dailyFile=join(folder,'daily-results.parquet'),networkFile=join(folder,'study-networks.parquet');
  let bytes=0,where='';
  for(;;){
    where=days.map(day=>`(date=${sql(day.date)}::DATE AND method_revision=${sql(day.method_revision)} AND source_revision=${sql(day.source_revision)})`).join(' OR ');
    await local.run(`COPY (SELECT date,method_revision,source_revision,body,updated_at FROM study_daily_results WHERE ${where}) TO ${sql(dailyFile)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    await local.run('CREATE OR REPLACE TEMP TABLE cloud_study_networks (version VARCHAR,digest VARCHAR,body JSON)');
    for(const version of new Set(days.map(day=>day.network_version))){const body=networks.get(version)!;await local.run(`INSERT INTO cloud_study_networks VALUES (${sql(version)},${sql(digest(body))},${sql(body)})`);}
    await local.run(`COPY cloud_study_networks TO ${sql(networkFile)} (FORMAT PARQUET,COMPRESSION ZSTD)`);
    bytes=(await stat(dailyFile)).size+(await stat(networkFile)).size;
    if(bytes<=remainingBytes)break;
    if(days.length===1)return 0;days=days.slice(0,Math.max(1,Math.floor(days.length/2)));
  }
  await resultsSchema(c,database,cacheKey);const prefix=ident(database)+'.';
  await c.run(`INSERT INTO ${prefix}transit_study_networks SELECT * FROM read_parquet(${sql(networkFile)}) ON CONFLICT DO NOTHING`);
  await c.run(`INSERT INTO ${prefix}transit_study_daily_results SELECT * FROM read_parquet(${sql(dailyFile)}) ON CONFLICT DO NOTHING`);
  const actualDaily=`SELECT date,method_revision,source_revision,body FROM ${prefix}transit_study_daily_results WHERE ${where}`,expectedDaily=`SELECT date,method_revision,source_revision,body FROM read_parquet(${sql(dailyFile)})`;
  const actualNetworks=`SELECT * FROM ${prefix}transit_study_networks WHERE version IN (SELECT version FROM read_parquet(${sql(networkFile)}))`,expectedNetworks=`SELECT * FROM read_parquet(${sql(networkFile)})`;
  const [{n}]=await query<{n:number}>(c,`SELECT SUM(n) AS n FROM (SELECT COUNT(*) AS n FROM ((${actualDaily} EXCEPT ALL ${expectedDaily}) UNION ALL (${expectedDaily} EXCEPT ALL ${actualDaily})) UNION ALL SELECT COUNT(*) AS n FROM ((${actualNetworks} EXCEPT ALL ${expectedNetworks}) UNION ALL (${expectedNetworks} EXCEPT ALL ${actualNetworks})))`);
  if(n!==0)throw new Error('Daily result or network readback mismatch');
  for(const day of days)await setState(local,`cloud_daily:${day.date}:${day.method_revision}:${day.source_revision}`,{uploaded_at:new Date().toISOString()});
  return bytes;
}

/** One bounded archive cycle. Unknown costs, connection failures and missing evidence pause cloud work only. */
export async function cloudUpload(local:DuckDBConnection,directory:string,dependencies:CloudDependencies={}):Promise<CloudHealth>{
  const now=dependencies.now??Date.now,previous=await state<CloudHealth>(local,'cloud_health');
  const [{n}]=await query<{n:number}>(local,'SELECT COALESCE(SUM(observations),0) AS n FROM collection_batches WHERE batch_id NOT IN (SELECT id FROM cloud_batches)');
  const health:CloudHealth={status:'paused',reason:'Cloud writes are disabled until usage is verified',checked_at:new Date(now()).toISOString(),storage_bytes:previous?.storage_bytes??null,budget_bytes:STORAGE_BUDGET,compute_cu_hours:null,last_uploaded_at:previous?.last_uploaded_at??null,pending_records:n};
  let remote:CloudRemote|undefined,cacheKey='';
  try{
    if(process.env.MOTHERDUCK_CLOUD_WRITES!=='true'||!process.env.MOTHER_DUCK_API_KEY)return health;
    // Explicit account-owner attestation, not a change to MotherDuck billing.
    // No-card accounts rely on provider quota enforcement; unavailable monthly
    // CU telemetry must not be replaced with an invented usage measurement.
    const noCard=process.env.MOTHERDUCK_BILLING_MODE==='free_no_card';
    health.storage_basis=noCard?'active':'total_accounted';
    let usage:CloudUsage|undefined;try{usage=JSON.parse(await readFile(process.env.MOTHERDUCK_USAGE_FILE||join(directory,'motherduck-usage.json'),'utf8'));}catch{}
    health.compute_cu_hours=noCard?null:usage?.compute_cu_hours??null;
    if(!noCard){const usageProblem=usageAllowsCloud(usage,0,now());if(usageProblem){health.reason=usageProblem;return health;}}
    const database=process.env.MOTHERDUCK_DATABASE||'my_db',resultsDatabase=process.env.TRANSIT_RESULTS_DATABASE||'transit_results';
    cacheKey=digest(database+':'+process.env.MOTHER_DUCK_API_KEY);
    if(resultsDatabase===database){health.reason='Results database must be separate from the original archive';return health;}
    remote=await (dependencies.connect??connectCloud)(database,process.env.MOTHER_DUCK_API_KEY);const c=remote.c;
    const [storage]=await query<{bytes:number|null;active_bytes:number|null;computed_at_epoch:number|null}>(c,'SELECT SUM(active_bytes) AS active_bytes,SUM(active_bytes+historical_bytes+retained_for_clone_bytes+failsafe_bytes) AS bytes,MIN(epoch(computed_ts)) AS computed_at_epoch FROM MD_INFORMATION_SCHEMA.STORAGE_INFO');
    health.total_accounted_storage_bytes=storage?.bytes??null;
    health.storage_bytes=(noCard?storage?.active_bytes:storage?.bytes)??null;
    // Metadata timestamps are UTC; do not parse a zone-less SQL string in the
    // workstation's Chicago timezone, which can make a fresh sample look future.
    const computedAt=storage?.computed_at_epoch;
    if(computedAt==null||!Number.isFinite(computedAt)||!storageAccountingFresh(new Date(computedAt*1000).toISOString(),now())){health.reason='Storage accounting is missing or stale';return health;}
    if(health.storage_bytes==null||!Number.isFinite(health.storage_bytes)||health.storage_bytes<0){health.reason='Storage accounting is unavailable';return health;}
    // Reclaim only exact, locally verified copies. Never assume DELETE immediately frees billed storage.
    const mayNeedPrune=health.storage_bytes>=STORAGE_BUDGET-UPLOAD_LIMITS.bytes*4;
    if(mayNeedPrune){
      const tableExists=(await query(c,`SELECT 1 FROM information_schema.tables WHERE table_catalog=${sql(database)} AND table_name='observation_receipts'`)).length;
      const cutoff=addDays(localDay(now()/1000,'America/Chicago'),-28);
      const date=tableExists?await pruneVerifiedRemoteDay(local,c,directory,database,cutoff):null;
      health.reason=date?`Verified detailed receipts for ${date} are retained locally; waiting for storage accounting to update`:'Storage ceiling reached; no eligible verified detail can be removed safely';return health;
    }
    if(!noCard){const usageAgain=usageAllowsCloud(usage,health.storage_bytes,now());if(usageAgain){health.reason=usageAgain;return health;}}
    const upload=await prepareObservationUpload(local,directory);let bytes=0;
    if(upload){
      if(health.storage_bytes+upload.bytes*4>=STORAGE_BUDGET){health.reason='Upload would consume storage headroom';return health;}
      await observationSchema(c,database,cacheKey);await commitObservations(local,c,database,upload);bytes=upload.bytes;
      health.pending_records-=upload.batches.reduce((total,row)=>total+row.observations,0);health.last_uploaded_at=new Date(now()).toISOString();
    }
    // A conservative multiplier reserves headroom for compression/layout and transient history.
    const remaining=Math.min(UPLOAD_LIMITS.bytes-bytes,Math.floor((STORAGE_BUDGET-health.storage_bytes)/4)-bytes);
    if(remaining>0)await persistResults(local,c,directory,resultsDatabase,remaining,cacheKey+':'+resultsDatabase);
    health.status='ready';health.reason=health.pending_records?'Verified bounded batch committed; more records remain queued':'Verified archive cycle completed';
  }catch{initializedSchemas.delete(cacheKey);initializedSchemas.delete(cacheKey+':'+(process.env.TRANSIT_RESULTS_DATABASE||'transit_results'));health.status='error';health.reason='Cloud archive failed; local observations and pending batches are retained';}
  finally{try{remote?.close();}catch{}await setState(local,'cloud_health',health);}
  return health;
}
