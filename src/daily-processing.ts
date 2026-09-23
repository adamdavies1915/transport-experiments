import { hostname } from 'node:os';
import { mkdir, readFile, readdir, stat } from 'node:fs/promises';
import { DuckDBInstance } from '@duckdb/node-api';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';
import { atomicFile, diskBudget } from './local-journal';
import { CAPTURE_LIMITS, decodeCaptureBundle, validateCaptureManifest, type CaptureManifest } from './capture-exchange';
import type { ProcessingLease } from './processing-coordinator';
import { sha256File } from './processing-seed';
import { processingAnalysisRevision } from './processing-revision';
import { ingestBatch, openLocalStore, setState, state, query, sql } from './local-store';
import { saveSchedule } from './otp-worker';
import { readSchedule, localDay } from './otp';
import { queueServerHistoryOtp } from '../scripts/import-server-history';
import { parseSummary } from '../dashboard/src/summary-validation';

export interface DailyProcessingOptions {
  dataDirectory: string; serverUrl: string; token: string; workerId?: string;
  analysisRevision?: string; fetcher?: typeof fetch;
  runAnalysis?: (directory: string, jobId: string, signal: AbortSignal) => Promise<void>;
  log?: (message: string) => void;
}
async function responseBytes(response: Response, limit: number): Promise<Buffer> {
  if (!response.ok || !response.body) throw new Error(`Processing server returned HTTP ${response.status}`);
  const reader = response.body.getReader(), chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const item = await reader.read(); if (item.done) break;
      total += item.value.byteLength; if (total > limit) throw new Error('Processing response exceeds its size limit'); chunks.push(item.value);
    }
  } finally { await reader.cancel(); }
  return Buffer.concat(chunks);
}
export class ProcessingClient {
  readonly base: string;
  constructor(private options: Pick<DailyProcessingOptions, 'serverUrl' | 'token' | 'fetcher'>) {
    const url = new URL(options.serverUrl);
    if (url.username || url.password || url.search || url.hash || (url.protocol !== 'https:' && !(url.protocol === 'http:' && ['127.0.0.1', '[::1]', 'localhost'].includes(url.hostname))))
      throw new Error('Use HTTPS for the processing server, or a loopback URL through an SSH tunnel');
    if (options.token.length < 32) throw new Error('A processing token of at least 32 characters is required');
    this.base = url.href.replace(/\/$/, '') + '/internal/processing';
  }
  private request(path: string, data?: unknown, signal?: AbortSignal) {
    return (this.options.fetcher ?? fetch)(this.base + path, { method: data === undefined ? 'GET' : 'POST',
      headers: { Authorization: `Bearer ${this.options.token}`, ...(data === undefined ? {} : { 'Content-Type': 'application/json' }) },
      body: data === undefined ? undefined : JSON.stringify(data), redirect: 'error',
      signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(120_000)]) : AbortSignal.timeout(120_000) });
  }
  async json<T>(path: string, data?: unknown, signal?: AbortSignal): Promise<T> {
    return JSON.parse((await responseBytes(await this.request(path, data, signal), 64 * 1024 * 1024)).toString('utf8')) as T;
  }
  async download(path: string, file: string, expected: { bytes: number; sha256: string }, maximum: number, signal?: AbortSignal) {
    if (!Number.isSafeInteger(expected.bytes) || expected.bytes < 1 || expected.bytes > maximum) throw new Error('Invalid download size');
    try {
      const info = await stat(file);
      if (info.size === expected.bytes && await sha256File(file) === expected.sha256) return;
      throw new Error('Existing local capture archive failed verification');
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    const bytes = await responseBytes(await this.request(path, undefined, signal), expected.bytes);
    const { createHash } = await import('node:crypto');
    if (bytes.length !== expected.bytes || createHash('sha256').update(bytes).digest('hex') !== expected.sha256) throw new Error('Capture download checksum mismatch');
    await mkdir(dirname(file), { recursive: true, mode: 0o700 });
    await atomicFile(file, bytes);
  }
}
const localLocks = new Set<string>();
/** A tiny separate DuckDB file supplies a native process lock, released on crash.
 * The server lease coordinates hosts; the actual research database stays separate. */
export async function localProcessingLock(directory: string): Promise<() => Promise<void>> {
  const folder = join(directory, 'processing'), lock = resolve(folder, 'worker-lock.duckdb');
  await mkdir(folder, { recursive: true, mode: 0o700 });
  if(localLocks.has(lock))throw new Error('A local processing worker is already running');
  localLocks.add(lock);
  try {
    const db=await DuckDBInstance.create(lock,{threads:'1',memory_limit:'64MB'});
    return async()=>{db.closeSync();localLocks.delete(lock);};
  } catch(error){localLocks.delete(lock);throw error;}
}
export async function defaultAnalysis(directory: string, jobId: string, signal: AbortSignal): Promise<void> {
  const root = dirname(dirname(fileURLToPath(import.meta.url)));
  await new Promise<void>((resolvePromise, reject) => {
    signal.throwIfAborted();
    const child = spawn(process.execPath, ['--import', 'tsx', join(root, 'src/local-worker.ts'), '--backfill'], {
      cwd: root, stdio: 'inherit',
      env: { ...process.env, TRANSIT_DATA_DIR: directory, MOTHERDUCK_BOOTSTRAP: 'false', MOTHERDUCK_CLOUD_WRITES: 'false',
        OTP_SCHEDULE_REFRESH: 'false', PROCESSING_JOB_ID: jobId, LOCAL_ANALYSIS_ENABLED: 'false', NODE_TEST_CONTEXT:undefined },
    });
    let failure:Error|undefined,killTimer:ReturnType<typeof setTimeout>|undefined;
    const stop=()=>{child.kill('SIGTERM');killTimer??=setTimeout(()=>child.kill('SIGKILL'),30_000);};
    signal.addEventListener('abort',stop,{once:true});
    child.on('error',error=>{failure=error;});
    child.on('close',(code,termination)=>{
      if(killTimer)clearTimeout(killTimer);signal.removeEventListener('abort',stop);
      if(code===0&&!signal.aborted&&!failure)resolvePromise();
      else reject(failure??new Error(`Daily analysis failed (${termination??code})`));
    });
    if(signal.aborted)stop();
  });
}
export async function ingestManifest(directory: string, manifest: CaptureManifest, client: Pick<ProcessingClient,'download'>, signal: AbortSignal, log: (s: string) => void) {
  const archive = join(directory, 'processing', 'capture', manifest.capture_id);
  await mkdir(archive,{recursive:true,mode:0o700});
  const store = await openLocalStore(directory);
  try {
    const previous = await state<{ capture_id: string }>(store.c, 'processing_capture');
    if (previous && previous.capture_id !== manifest.capture_id) throw new Error('This database belongs to a different capture stream');
    const markers=new Map((await query<{key:string;value:{sha256:string}|string}>(store.c,"SELECT key,value FROM local_state WHERE key LIKE 'processing_bundle:%'")).map(row=>[row.key,typeof row.value==='string'?JSON.parse(row.value):row.value]));
  // Raw archives stay on disk after ingestion, independently of the mutable database.
  for (const descriptor of manifest.bundles) {
    signal.throwIfAborted();
    const marker=markers.get(`processing_bundle:${manifest.capture_id}:${descriptor.id}`),file=join(archive,descriptor.id+'.json.gz');
    if(marker&&marker.sha256!==descriptor.sha256)throw new Error('Imported capture bundle changed');
    // Database commit records the previously verified digest. Avoid rereading all
    // historical raw bytes every day; restore/replay still verifies full hashes.
    const cached=marker?await stat(file).catch((e:NodeJS.ErrnoException)=>{if(e.code==='ENOENT')return null;throw e;}):null;
    if(!cached||cached.size!==descriptor.bytes)await client.download('/bundles/' + descriptor.id, file, descriptor, CAPTURE_LIMITS.bundle_bytes, signal);
  }
  for (const schedule of manifest.schedules) await client.download('/schedules/' + schedule.id, join(archive, schedule.id + '.zip'), schedule, CAPTURE_LIMITS.schedule_bytes, signal);
  await atomicFile(join(archive, 'manifest.json'), JSON.stringify(manifest));
    await setState(store.c, 'processing_capture', { capture_id: manifest.capture_id });
    for (const descriptor of manifest.schedules) {
      signal.throwIfAborted();
      const key = 'processing_schedule:' + descriptor.id;
      if (await state(store.c, key)) continue;
      const bytes = await readFile(join(archive, descriptor.id + '.zip')), schedule = readSchedule(bytes);
      const observedDate = localDay(Date.parse(descriptor.first_seen_at) / 1000, schedule.timezone);
      const usable = observedDate > schedule.start ? observedDate : schedule.start;
      if (usable <= schedule.end){
        await saveSchedule(store.c, bytes, descriptor.source, usable);
        // Selection between versions first observed on the same date must follow
        // server evidence, never SHA order or the workstation's import clock.
        await store.c.run(`UPDATE otp_schedules SET fetched_at=${sql(descriptor.first_seen_at)}::TIMESTAMPTZ WHERE hash=${sql(schedule.hash)}`);
      }
      await setState(store.c, key, { first_seen_at: descriptor.first_seen_at });
    }
    const dates = new Set<string>(); let newBundles = 0, newFrames = 0;
    for (const descriptor of manifest.bundles) {
      signal.throwIfAborted();
      const key = `processing_bundle:${manifest.capture_id}:${descriptor.id}`;
      const previous = markers.get(key);
      if (previous) { if (previous.sha256 !== descriptor.sha256) throw new Error('Imported capture bundle changed'); continue; }
      const batches = decodeCaptureBundle(await readFile(join(archive, descriptor.id + '.json.gz')), descriptor, manifest.capture_id);
      for (const batch of batches) {
        signal.throwIfAborted(); await ingestBatch(store.c, batch); newFrames++;
        dates.add(localDay(Date.parse(batch.received_at) / 1000, 'America/Chicago'));
        for (const { observation } of batch.observations) if (observation.observed_at != null && Number.isFinite(observation.observed_at)) dates.add(localDay(observation.observed_at, 'America/Chicago'));
      }
      // Queue OTP before marking this bundle done. An interrupted retry replays IDs safely.
      await queueServerHistoryOtp(store.c, [...dates]); dates.clear();
      await setState(store.c, key, { sha256: descriptor.sha256 }); newBundles++;
    }
    await cCheckpoint(store.c);
    log(`Imported ${newBundles} new bundles / ${newFrames} frames; raw archives retained locally`);
  } finally { store.c.closeSync(); store.db.closeSync(); }
}
async function cCheckpoint(c: Awaited<ReturnType<typeof openLocalStore>>['c']) { await c.run('CHECKPOINT'); }

export async function runDailyProcessing(options: DailyProcessingOptions): Promise<{ status: 'idle' | 'completed'; job_id?: string }> {
  const directory = resolve(options.dataDirectory), unlock = await localProcessingLock(directory);
  const log = options.log ?? console.log;
  let client:ProcessingClient|undefined;
  let lease: ProcessingLease | null = null, timer: ReturnType<typeof setTimeout> | undefined;
  const abort = new AbortController(); let renewal: Promise<void> | undefined;
  const stopped=()=>abort.abort(new Error('Workstation worker is shutting down'));
  process.on('SIGTERM',stopped);process.on('SIGINT',stopped);
  const reference = () => ({ job_id: lease!.job_id, lease_token: lease!.lease_token, fence: lease!.fence });
  try {
    client=new ProcessingClient(options);
    const baseline = JSON.parse(await readFile(join(directory, 'processing/baseline.json'), 'utf8')) as { baseline_id: string };
    if (!/^[a-f0-9]{64}$/.test(baseline.baseline_id)) throw new Error('Initialize this worker from the verified processing seed');
    await stat(join(directory, 'transit.duckdb'));
    if ((await readdir(join(directory, 'incoming')).catch((e: NodeJS.ErrnoException) => e.code === 'ENOENT' ? [] : Promise.reject(e))).some(n => n.endsWith('.json.gz')))
      throw new Error('Daily processing needs its own database directory without a live collection journal');
    if (!(await diskBudget(directory)).allowed) throw new Error('Insufficient workstation disk space for processing');
    const analysisRevision = options.analysisRevision ?? await processingAnalysisRevision();
    lease = await client.json<ProcessingLease | null>('/claim', { worker_id: options.workerId ?? hostname(), baseline_id: baseline.baseline_id, analysis_revision: analysisRevision });
    if (!lease) { log('No daily job is due, or another workstation owns it'); return { status: 'idle' }; }
    const manifest = validateCaptureManifest(lease.manifest);
    if (lease.manifest_sha256 !== manifest.manifest_sha256 || lease.baseline_id !== baseline.baseline_id || lease.analysis_revision !== analysisRevision ||
      !Number.isInteger(lease.lease_seconds) || lease.lease_seconds < 10) throw new Error('Invalid processing lease');
    const renew = () => {
      timer = setTimeout(() => {
        renewal = client!.json<ProcessingLease>('/renew', reference()).then(next => {
          if (next.fence !== lease!.fence || next.manifest_sha256 !== lease!.manifest_sha256) throw new Error('Processing lease changed');
          lease = next; if (!abort.signal.aborted) renew();
        }).catch(error => abort.abort(error));
      }, Math.max(1000, Math.floor(lease!.lease_seconds * 1000 / 3)));
    };
    renew(); const started = performance.now();
    log(`Claimed daily job ${lease.job_id} for ${lease.service_date}; ${manifest.bundle_count} capture bundles`);
    await ingestManifest(directory, manifest, client, abort.signal, log);
    await (options.runAnalysis ?? defaultAnalysis)(directory, lease.job_id, abort.signal);
    abort.signal.throwIfAborted();
    const store = await openLocalStore(directory);
    try {
      const completion = await state<{ job_id: string; completed: boolean }>(store.c, 'last_backfill_run');
      if (!completion?.completed || completion.job_id !== lease.job_id) throw new Error('Backfill did not confirm complete processing of this job');
    } finally { store.c.closeSync(); store.db.closeSync(); }
    const summary = parseSummary(JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8')));
    const result = await client.json('/complete', { ...reference(), manifest_sha256: manifest.manifest_sha256, summary }, abort.signal);
    await atomicFile(join(directory, 'processing/last-completed.json'), JSON.stringify({ job_id: lease.job_id, manifest_sha256: manifest.manifest_sha256, elapsed_ms: Math.round(performance.now() - started), result }));
    log(`Daily job completed and published in ${Math.round((performance.now() - started) / 1000)} seconds`);
    return { status: 'completed', job_id: lease.job_id };
  } finally {
    process.removeListener('SIGTERM',stopped);process.removeListener('SIGINT',stopped);
    if (timer) clearTimeout(timer); abort.abort(); await renewal;
    if (lease&&client) await client.json('/release', reference()).catch(() => {});
    await unlock();
  }
}
