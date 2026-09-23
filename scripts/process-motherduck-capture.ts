import 'dotenv/config';
import { randomUUID } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { localProcessingLock, ingestManifest, defaultAnalysis } from '../src/daily-processing';
import { latestProcessingDate } from '../src/processing-coordinator';
import { cloudCaptureManifest, MotherDuckCaptureReader } from '../src/motherduck-capture';
import { atomicFile, diskBudget } from '../src/local-journal';
import { openLocalStore, state } from '../src/local-store';
import { pushSummaryFile } from '../src/summary-transfer';
import { safeError } from '../src/log-safety';
import { addDays, localDay } from '../src/otp';

async function secret(name:string) {
  const path = process.env[name+'_FILE'];
  const value = path ? (await readFile(path,'utf8')).trim() : process.env[name];
  if (!value) throw new Error(name+' is required');
  return value;
}
async function main() {
  const directory=process.env.TRANSIT_DATA_DIR,captureId=process.env.MOTHERDUCK_CAPTURE_ID;
  if (!directory || !captureId) throw new Error('Data directory and capture ID are required');
  await stat(join(directory,'transit.duckdb'));
  const unlock = await localProcessingLock(directory), abort = new AbortController();
  const stop=()=>abort.abort(new Error('MotherDuck processing interrupted'));
  process.on('SIGTERM',stop);process.on('SIGINT',stop);
  try {
    const completionFile=join(directory,'processing/motherduck-capture-completed.json');
    const due=latestProcessingDate(Date.now(),6);
    let completed:{service_date:string}|undefined;
    try { completed=JSON.parse(await readFile(completionFile,'utf8')); }
    catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
    const importOnly=process.argv.includes('--import-only');
    if (!importOnly && !process.argv.includes('--force') &&
      (due < (process.env.MOTHERDUCK_PROCESSING_FIRST_DATE || '2026-09-22') || (completed && completed.service_date>=due))) {
      console.log('[MotherDuck processing] No daily job due');return;
    }
    if (!(await diskBudget(directory)).allowed) throw new Error('Workstation disk reserve not satisfied');
    const token=await secret('MOTHER_DUCK_API_KEY'),database=process.env.MOTHERDUCK_DATABASE||'my_db';
    const remote=await DuckDBInstance.create(`md:${database}?motherduck_token=${encodeURIComponent(token)}`,{threads:'1',memory_limit:'512MB'});
    let manifest;
    try {
      const c=await remote.connect();
      try {
        manifest=await cloudCaptureManifest(c,database,captureId);
        if (!manifest || !manifest.bundle_count) throw new Error('No committed MotherDuck capture is available');
        if (!importOnly && localDay(Date.parse(manifest.bundles.at(-1)!.last_received_at)/1000,'America/Chicago') < addDays(due,1))
          throw new Error('MotherDuck capture has not reached the end of the due service date');
        await ingestManifest(directory,manifest,new MotherDuckCaptureReader(c,database,captureId),abort.signal,console.log);
      } finally { c.closeSync(); }
    } finally { remote.closeSync(); }
    if (importOnly) {console.log(JSON.stringify({status:'imported',source:'MotherDuck',bundles:manifest.bundle_count,manifest_sha256:manifest.manifest_sha256}));return;}
    const jobId='motherduck-'+randomUUID();
    await defaultAnalysis(directory,jobId,abort.signal);
    abort.signal.throwIfAborted();
    const store=await openLocalStore(directory);
    try {
      const marker=await state<{job_id:string;completed:boolean}>(store.c,'last_backfill_run');
      if (!marker?.completed || marker.job_id!==jobId) throw new Error('Incomplete analysis cannot publish');
    } finally {store.c.closeSync();store.db.closeSync();}
    const url=process.env.TRANSIT_SUMMARY_PUBLISH_URL;
    if(!url)throw new Error('Summary gateway URL required');
    const publication=await pushSummaryFile({path:join(directory,'summary.json'),url,token:await secret('TRANSIT_SUMMARY_PUBLISH_TOKEN'),timeoutMs:120_000});
    if(!publication.published)throw new Error(publication.reason);
    await atomicFile(completionFile,JSON.stringify({service_date:due,job_id:jobId,completed_at:new Date().toISOString(),capture_id:captureId,manifest_sha256:manifest.manifest_sha256,publication}));
    console.log('[MotherDuck processing] Analysis and publication completed');
  } finally {process.off('SIGTERM',stop);process.off('SIGINT',stop);await unlock();}
}
main().catch(error=>{console.error(safeError(error));process.exitCode=1;});
