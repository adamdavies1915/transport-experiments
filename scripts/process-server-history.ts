import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { hostname } from 'node:os';
import { pathToFileURL } from 'node:url';
import { runDailyProcessing, localProcessingLock } from '../src/daily-processing';
import { atomicFile } from '../src/local-journal';
import { pushSummaryFile } from '../src/summary-transfer';
import { openLocalStore, state } from '../src/local-store';
import { safeError } from '../src/log-safety';

export async function publishGatewaySummary(directory: string, jobId: string, url: string, token: string, push = pushSummaryFile) {
  const result = await push({ path: join(directory, 'summary.json'), url, token });
  if (!result.published) throw new Error(result.reason || 'Gateway publication failed');
  await atomicFile(join(directory, 'processing/gateway-published.json'),
    JSON.stringify({ job_id: jobId, published_at: new Date().toISOString() }));
}

async function main() {
  const directory = process.env.TRANSIT_DATA_DIR, server = process.env.PROCESSING_SERVER_URL;
  if (!directory || !server) throw new Error('Data directory and processing server are required');
  for (const name of ['TRANSIT_PROCESSING_TOKEN', 'TRANSIT_SUMMARY_PUBLISH_TOKEN']) {
    const path = process.env[`${name}_FILE`];
    if (path) process.env[name] = (await readFile(path, 'utf8')).trim();
  }
  console.log(JSON.stringify(await runDailyProcessing({ dataDirectory: directory, serverUrl: server,
    token: process.env.TRANSIT_PROCESSING_TOKEN || '', workerId: process.env.PROCESSING_WORKER_ID || hostname() })));
  // Publishing to the existing public gateway is independently retryable after
  // the leased server job succeeds, including when no further job is due.
  const unlock = await localProcessingLock(directory);
  try {
    let completed: { job_id: string };
    try { completed = JSON.parse(await readFile(join(directory, 'processing/last-completed.json'), 'utf8')); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return; throw error; }
    const marker = join(directory, 'processing/gateway-published.json');
    let published: { job_id: string } | undefined;
    try { published = JSON.parse(await readFile(marker, 'utf8')); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    if (published?.job_id === completed.job_id) return;
    const store = await openLocalStore(directory);
    try {
      const backfill = await state<{ job_id: string; completed: boolean }>(store.c, 'last_backfill_run');
      if (!backfill?.completed || backfill.job_id !== completed.job_id) throw new Error('Gateway publication needs the completed job snapshot');
    } finally { store.c.closeSync(); store.db.closeSync(); }
    const url = process.env.TRANSIT_SUMMARY_PUBLISH_URL, token = process.env.TRANSIT_SUMMARY_PUBLISH_TOKEN;
    if (!url || !token) throw new Error('Gateway publication is not configured');
    await publishGatewaySummary(directory, completed.job_id, url, token);
    console.log('[Gateway] Completed analysis published');
  } finally { await unlock(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(error => { console.error(safeError(error)); process.exitCode = 1; });
