import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import type { DuckDBConnection } from '@duckdb/node-api';
import { query, sql } from './local-store';

async function verifyFile(path:string,expected:string){
  if(createHash('sha256').update(await readFile(path)).digest('hex')!==expected)throw new Error('Archive checksum mismatch; original rows retained');
}
/** Cloud retries can rehydrate exact selected receipts from the local cold archive. */
export async function restoreArchivedBatches(c:DuckDBConnection,ids:string[]){
  if(!ids.length)return;
  const list=ids.map(sql).join(',');
  const missing=await query<{date:string}>(c,`SELECT DISTINCT timezone('America/Chicago',b.received_at)::DATE::VARCHAR AS date FROM collection_batches b
    LEFT JOIN (SELECT batch_id,COUNT(*) AS n FROM collection_receipts GROUP BY 1) r USING(batch_id)
    WHERE b.batch_id IN (${list}) AND COALESCE(r.n,0)<b.observations`);
  for(const day of missing){
    const [archive]=await query<{path:string;sha256:string}>(c,`SELECT path,sha256 FROM local_archive_catalog WHERE date=${sql(day.date)}::DATE`);
    if(!archive)throw new Error('Pending cloud batch has no verified local archive');
    await verifyFile(archive.path,archive.sha256);
    await c.run('BEGIN');
    try{
      await c.run(`INSERT INTO collection_payloads SELECT DISTINCT payload_id,source,vehicle_id,route_id,observed_at,body,raw FROM read_parquet(${sql(archive.path)}) WHERE batch_id IN (${list}) ON CONFLICT DO NOTHING`);
      await c.run(`INSERT INTO collection_receipts SELECT observation_id,payload_id,received_at,service_date,batch_id FROM read_parquet(${sql(archive.path)}) WHERE batch_id IN (${list}) ON CONFLICT DO NOTHING`);
      await c.run('COMMIT');
    }catch(e){await c.run('ROLLBACK');throw e;}
  }
}

/** Keep seven days of duplicate hot detail; verified Parquet remains available for reanalysis. */
export async function compactLocalHistory(c:DuckDBConnection,cutoff:string):Promise<number>{
  const days=await query<{date:string;path:string;sha256:string}>(c,`SELECT date::VARCHAR AS date,path,sha256 FROM local_archive_catalog WHERE date<${sql(cutoff)}::DATE ORDER BY date`);
  let removed=0;
  for(const archive of days){
    const [{n}]=await query<{n:number}>(c,`SELECT COUNT(*) AS n FROM collection_receipts WHERE service_date=${sql(archive.date)}::DATE`);
    if(!n)continue;
    await verifyFile(archive.path,archive.sha256);
    const columns='observation_id,payload_id,received_at,service_date,batch_id,source,vehicle_id,route_id,observed_at,body,raw';
    const [{missing}]=await query<{missing:number}>(c,`SELECT COUNT(*) AS missing FROM (
      SELECT ${columns} FROM collected_observations WHERE service_date=${sql(archive.date)}::DATE
      EXCEPT ALL SELECT ${columns} FROM read_parquet(${sql(archive.path)})
    )`);
    if(missing)throw new Error('Archive is missing hot observation evidence; originals retained');
    await c.run('BEGIN');
    try{
      await c.run(`DELETE FROM collection_receipts WHERE service_date=${sql(archive.date)}::DATE`);
      await c.run('DELETE FROM collection_payloads WHERE payload_id NOT IN (SELECT payload_id FROM collection_receipts)');
      await c.run('COMMIT');removed+=n;
    }catch(e){await c.run('ROLLBACK');throw e;}
  }
  return removed;
}
