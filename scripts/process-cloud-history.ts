import 'dotenv/config';
import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdir, readFile, stat } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DuckDBInstance } from '@duckdb/node-api';
import { localProcessingLock } from '../src/daily-processing';
import { atomicFile, diskBudget } from '../src/local-journal';
import { ident, query, sql, state } from '../src/local-store';
import { addDays, localDay } from '../src/otp';
import { pushSummaryFile } from '../src/summary-transfer';
import { safeError } from '../src/log-safety';

/** Retry the same overlap until import, analysis AND publication all succeed. */
export function catchupFrom(lastCutoff: string | undefined, initialDate: string): string {
  if (lastCutoff !== undefined) {
    if (!Number.isFinite(Date.parse(lastCutoff))) throw new Error('Invalid completed cutoff');
    return addDays(localDay(Date.parse(lastCutoff) / 1000, 'America/Chicago'), -1);
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(initialDate) || new Date(initialDate).toISOString().slice(0, 10) !== initialDate)
    throw new Error('CLOUD_HISTORY_FROM must be a valid initial date');
  return initialDate;
}

async function secret(name: string): Promise<string> {
  const value = process.env[`${name}_FILE`] ? (await readFile(process.env[`${name}_FILE`]!, 'utf8')).trim() : process.env[name];
  if (!value) throw new Error(`${name} or ${name}_FILE is required`);
  return value;
}
async function child(args: string[], env: NodeJS.ProcessEnv) {
  await new Promise<void>((done, reject) => {
    let interrupted = false;
    const processChild = spawn(process.execPath, ['--import', 'tsx', ...args], { env, stdio: 'inherit' });
    const stop = () => { interrupted = true; processChild.kill('SIGTERM'); };
    process.on('SIGTERM', stop); process.on('SIGINT', stop);
    processChild.once('error', reject);
    processChild.once('close', code => {
      process.off('SIGTERM', stop); process.off('SIGINT', stop);
      code === 0 && !interrupted ? done() : reject(new Error(`Processing stage failed or interrupted (${code})`));
    });
  });
}
async function digest(path: string) {
  const hash = createHash('sha256');
  for await (const bytes of createReadStream(path)) hash.update(bytes);
  return hash.digest('hex');
}

async function main() {
  if (!process.env.TRANSIT_DATA_DIR) throw new Error('TRANSIT_DATA_DIR is required');
  const directory = resolve(process.env.TRANSIT_DATA_DIR);
  const unlock = await localProcessingLock(directory);
  try {
    const completionFile = join(directory, 'processing', 'cloud-history-completed.json');
    let last: { cutoff: string; completed_at: string } | undefined;
    try { last = JSON.parse(await readFile(completionFile, 'utf8')); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    const today = localDay(Date.now() / 1000, 'America/Chicago');
    if (!process.argv.includes('--force') && last && localDay(Date.parse(last.completed_at) / 1000, 'America/Chicago') === today) {
      console.log('[Cloud catchup] Already completed today'); return;
    }
    if (!(await diskBudget(directory)).allowed) throw new Error('Local disk reserve is not satisfied');
    await stat(join(directory, 'transit.duckdb')); // Never silently create an empty baseline.
    const from = catchupFrom(last?.cutoff, process.env.CLOUD_HISTORY_FROM || '');
    const token = await secret('MOTHER_DUCK_API_KEY');
    const publishToken = await secret('TRANSIT_SUMMARY_PUBLISH_TOKEN');
    const url = process.env.TRANSIT_SUMMARY_PUBLISH_URL;
    if (!url) throw new Error('TRANSIT_SUMMARY_PUBLISH_URL is required');
    const root = dirname(dirname(fileURLToPath(import.meta.url)));
    const run = join(directory, 'imports', `cloud-${new Date().toISOString().replaceAll(':', '-')}-${randomUUID()}`);
    await mkdir(run, { recursive: true, mode: 0o700 });
    const database = process.env.MOTHERDUCK_DATABASE || 'my_db';
    const remote = await DuckDBInstance.create(`md:${database}?motherduck_token=${encodeURIComponent(token)}`, { threads: '2', memory_limit: '1GB' });
    let cutoff: string;
    const files: Array<{ table: string; path: string; bytes: number; sha256: string; rows: number }> = [];
    try {
      const c = await remote.connect();
      try {
        cutoff = new Date().toISOString();
        // Only SELECT/COPY from the existing cloud tables. No cloud DDL or writes.
        for (const [table, clock] of [['transit_data', "COALESCE(observed_at, timestamp AT TIME ZONE 'America/Chicago')"], ['streetcar_snapshots', 'received_at']]) {
          const path = join(run, `${table}.parquet`);
          await c.run(`COPY (SELECT * FROM ${ident(database)}.main.${ident(table)} WHERE ${clock} >= (${sql(from)}::DATE::TIMESTAMP AT TIME ZONE 'America/Chicago') AND ${clock} < ${sql(cutoff)}::TIMESTAMPTZ) TO ${sql(path)} (FORMAT PARQUET, COMPRESSION ZSTD)`);
          const [{ rows }] = await query<{ rows: number }>(c, `SELECT count(*) AS rows FROM read_parquet(${sql(path)})`);
          files.push({ table, path, rows, bytes: (await stat(path)).size, sha256: await digest(path) });
          console.log(`[Cloud catchup] Exported ${table}: ${rows} rows`);
        }
      } finally { c.closeSync(); }
    } finally { remote.closeSync(); }
    await atomicFile(join(run, 'manifest.json'), JSON.stringify({ from, cutoff, files }, null, 2));
    const env: NodeJS.ProcessEnv = { ...process.env, MOTHERDUCK_BOOTSTRAP: 'false', MOTHERDUCK_CLOUD_WRITES: 'false',
      PROCESSING_JOB_ID: `cloud-catchup-${randomUUID()}` };
    // Child stages cannot publish an intermediate or failed analysis.
    delete env.MOTHER_DUCK_API_KEY;
    delete env.TRANSIT_SUMMARY_PUBLISH_TOKEN;
    await child([join(root, 'scripts/import-server-history.ts'), '--data-dir', directory,
      '--transit-data', files[0].path, '--streetcar-snapshots', files[1].path], env);
    await child([join(root, 'src/local-worker.ts'), '--backfill'], env);
    const local = await DuckDBInstance.create(join(directory, 'transit.duckdb'), { access_mode: 'READ_ONLY', threads: '1', memory_limit: '256MB' });
    try {
      const c = await local.connect();
      try {
        const marker = await state<{ job_id: string; completed: boolean }>(c, 'last_backfill_run');
        if (!marker?.completed || marker.job_id !== env.PROCESSING_JOB_ID) throw new Error('Backfill completion was not verified');
      } finally { c.closeSync(); }
    } finally { local.closeSync(); }
    const result = await pushSummaryFile({ path: join(directory, 'summary.json'), url, token: publishToken, timeoutMs: 120_000 });
    if (!result.published) throw new Error(result.reason);
    await atomicFile(completionFile, JSON.stringify({ from, cutoff, completed_at: new Date().toISOString(), manifest: join(run, 'manifest.json'), publication: result }, null, 2));
    console.log('[Cloud catchup] Analysis and publication completed');
  } finally { await unlock(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(error => { console.error('[Cloud catchup]', safeError(error)); process.exitCode = 1; });
