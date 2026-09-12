import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Readable, Writable } from 'node:stream';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { gzipSync } from 'node:zlib';
import { LocalJournal, atomicFile } from './local-journal';
import { CaptureExchange } from './capture-exchange';
import { ProcessingCoordinator } from './processing-coordinator';
import { processingRequestHandler } from './processing-server';
import { runDailyProcessing, type DailyProcessingOptions } from './daily-processing';
import { openLocalStore, query, setState, state } from './local-store';
import { rowData, signalData } from '../dashboard/src/study-fixtures';
import type { CollectionBatch } from './observation-types';

const BASELINE = 'c'.repeat(64), REVISION = 'flow-test-revision', TOKEN = 'synthetic-flow-test-token-'.repeat(2);
const START = Date.parse('2026-09-14T11:00:00Z');

/** Exercise the actual Node request handler, including its streamed file
 * responses, without binding a port or replacing any protocol implementation. */
function inProcessFetch(handler: ReturnType<typeof processingRequestHandler>): typeof fetch {
  return async (url, options) => {
    const parsed = new URL(String(url));
    assert.equal(parsed.origin, 'https://processing.test');
    const req = Readable.from(options?.body == null ? [] : [Buffer.from(String(options.body))]);
    const requestHeaders: Record<string, string> = {};
    new Headers(options?.headers).forEach((value, name) => { requestHeaders[name] = value; });
    Object.assign(req, { method: options?.method ?? 'GET', url: parsed.pathname + parsed.search,
      headers: requestHeaders });
    const chunks: Buffer[] = [], headers = new Headers();
    const res = new Writable({ write(chunk, _encoding, callback) { chunks.push(Buffer.from(chunk)); callback(); } });
    Object.assign(res, { statusCode: 200, headersSent: false,
      setHeader(name: string, value: string | number) { headers.set(name, String(value)); } });
    return new Promise<Response>((resolve, reject) => {
      res.once('error', reject);
      res.once('finish', () => {
        req.destroy();
        resolve(new Response(new Uint8Array(Buffer.concat(chunks)), { status: (res as unknown as ServerResponse).statusCode, headers }));
      });
      void handler(req as unknown as IncomingMessage, res as unknown as ServerResponse).then(handled => {
        if (!handled) { (res as unknown as ServerResponse).statusCode = 404; res.end(); }
      }).catch(reject);
    });
  };
}

async function fixture(run: (f: {
  root: string; server: string; desktop: string; mac: string;
  journal: LocalJournal; exchange: CaptureExchange; coordinator: ProcessingCoordinator;
  append: (id: string) => Promise<void>; advanceDay: () => void;
  worker: (directory: string, id: string, analysis?: DailyProcessingOptions['runAnalysis']) => DailyProcessingOptions;
  analyze: NonNullable<DailyProcessingOptions['runAnalysis']>;
}) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), 'transit-processing-flow-'));
  const server = join(root, 'server'), desktop = join(root, 'desktop'), mac = join(root, 'mac'); let now = START;
  const journal = new LocalJournal(server), exchange = new CaptureExchange(server);
  try {
    await journal.init(); await exchange.init();
    for (const directory of [desktop, mac]) {
      const store = await openLocalStore(directory); store.c.closeSync(); store.db.closeSync();
      await mkdir(join(directory, 'processing'), { recursive: true });
      await atomicFile(join(directory, 'processing', 'baseline.json'), JSON.stringify({ baseline_id: BASELINE }));
    }
    const coordinator = new ProcessingCoordinator({ data_dir: server, baseline_id: BASELINE, analysis_revision: REVISION,
      first_service_date: '2026-09-07', lease_seconds: 60, now: () => now });
    const handler = processingRequestHandler({ exchange, coordinator, token: TOKEN, seal: () => exchange.seal(journal) });
    const fetcher = inProcessFetch(handler);
    const analyze: NonNullable<DailyProcessingOptions['runAnalysis']> = async (directory, jobId, signal) => {
      signal.throwIfAborted();
      const store = await openLocalStore(directory);
      try {
        await setState(store.c, 'last_backfill_run', { job_id: jobId, completed: true });
        await store.c.run('CHECKPOINT');
      } finally { store.c.closeSync(); store.db.closeSync(); }
      await atomicFile(join(directory, 'summary.json'), JSON.stringify({ schema_version: 1, generated_at: new Date(now).toISOString(),
        row_study: rowData(), signal_study: signalData(), legacy: {} }));
    };
    const worker = (directory: string, id: string, analysis = analyze): DailyProcessingOptions => ({
      dataDirectory: directory, serverUrl: 'https://processing.test', token: TOKEN, workerId: id,
      analysisRevision: REVISION, fetcher, runAnalysis: analysis, log: () => {},
    });
    const append = async (id: string) => {
      const batch: CollectionBatch = { schema_version: 1, batch_id: id, source: 'sse', received_at: '2026-09-13T23:00:00.123Z', observations: [] };
      await writeFile(join(journal.pending, `${id}.json.gz`), gzipSync(JSON.stringify(batch)));
    };
    await run({ root, server, desktop, mac, journal, exchange, coordinator, append, advanceDay: () => { now += 86400000; }, worker, analyze });
  } finally { await rm(root, { recursive: true, force: true }); }
}
async function imported(directory: string) {
  const store = await openLocalStore(directory);
  try { return await query(store.c, 'SELECT batch_id,source,received_at::VARCHAR AS received_at,observations FROM collection_batches ORDER BY batch_id'); }
  finally { store.c.closeSync(); store.db.closeSync(); }
}

test('daily HTTP flow claims, verifies and imports capture, publishes server metadata, and leaves a second worker idle', async () => {
  await fixture(async ({ server, desktop, mac, exchange, journal, coordinator, append, worker }) => {
    await append('captured-frame');
    const completed = await runDailyProcessing(worker(desktop, 'desktop'));
    assert.equal(completed.status, 'completed');
    const published = JSON.parse(await readFile(join(server, 'summary.json'), 'utf8'));
    assert.equal(published.processing.job_id, completed.job_id); assert.equal(published.processing.worker_id, 'desktop');
    assert.equal(published.processing.service_date, '2026-09-13'); assert.equal(published.processing.analysis_revision, REVISION);
    const manifest = await exchange.manifest();
    assert.equal(published.processing.manifest_sha256, manifest.manifest_sha256);
    assert.equal(manifest.bundle_count, 1); assert.equal((await journal.files()).length, 0);
    assert.deepEqual(await readFile(join(desktop, 'processing', 'capture', manifest.capture_id, manifest.bundles[0].id + '.json.gz')),
      await readFile(await exchange.bundlePath(manifest.bundles[0].id)));
    assert.equal((await imported(desktop)).length, 1);
    assert.deepEqual(await runDailyProcessing(worker(mac, 'mac')), { status: 'idle' });
    assert.equal((await imported(mac)).length, 0); assert.equal((await coordinator.status()).active, null);
    assert.equal(JSON.stringify(published).includes(TOKEN), false);
  });
});

test('failed analysis releases the fixed job to another machine and later retry imports no duplicate batches', async () => {
  await fixture(async ({ server, desktop, mac, coordinator, exchange, append, advanceDay, worker }) => {
    await append('first');
    await assert.rejects(runDailyProcessing(worker(desktop, 'desktop', async () => { throw new Error('analysis interrupted'); })), /analysis interrupted/);
    const failed = (await coordinator.status()).active!;
    assert.equal(failed.status, 'pending'); assert.equal(failed.worker_id, null);
    assert.equal((await imported(desktop)).length, 1);
    await assert.rejects(readFile(join(server, 'summary.json')), { code: 'ENOENT' });
    await append('second');
    const recovered = await runDailyProcessing(worker(mac, 'mac'));
    assert.equal(recovered.job_id, failed.job_id);
    assert.equal((await exchange.manifest()).bundle_count, 2, 'new server input remains for the next daily generation');
    assert.deepEqual((await imported(mac)).map(r => r.batch_id), ['first'], 'retry receives the original immutable input only');
    advanceDay();
    const next = await runDailyProcessing(worker(desktop, 'desktop'));
    assert.equal(next.status, 'completed'); assert.notEqual(next.job_id, failed.job_id);
    assert.deepEqual((await imported(desktop)).map(r => r.batch_id), ['first', 'second']);
    const published = JSON.parse(await readFile(join(server, 'summary.json'), 'utf8'));
    assert.equal(published.processing.manifest_sha256, (await exchange.manifest()).manifest_sha256);
  });
});

test('empty capture history still processes a seeded baseline without a missing archive directory', async () => {
  await fixture(async ({ desktop, worker, exchange }) => {
    assert.equal((await exchange.manifest()).bundle_count, 0);
    assert.equal((await runDailyProcessing(worker(desktop, 'desktop'))).status, 'completed');
    assert.equal((await imported(desktop)).length, 0);
  });
});

test('download corruption or missing job completion evidence fails closed and leaves the job available', async () => {
  await fixture(async ({ server, desktop, coordinator, append, worker }) => {
    await append('captured');
    const options = worker(desktop, 'desktop'), fetcher = options.fetcher!;
    options.fetcher = async (url, init) => {
      const response = await fetcher(url, init);
      if (String(url).includes('/bundles/')) {
        const bytes = new Uint8Array(await response.arrayBuffer()); bytes[0] ^= 1;
        return new Response(bytes, { status: response.status, headers: response.headers });
      }
      return response;
    };
    await assert.rejects(runDailyProcessing(options), /checksum mismatch/);
    assert.equal((await imported(desktop)).length, 0);
    assert.equal((await coordinator.status()).active!.status, 'pending');
    await assert.rejects(runDailyProcessing(worker(desktop, 'desktop', async () => {})), /did not confirm complete processing/);
    assert.equal((await imported(desktop)).length, 1);
    assert.equal((await coordinator.status()).active!.status, 'pending');
    await assert.rejects(readFile(join(server, 'summary.json')), { code: 'ENOENT' });
  });
});

test('real backfill subprocess completes an empty seeded database and publishes its verified job result', { timeout: 120000 }, async () => {
  await fixture(async ({ desktop, server, coordinator, exchange, worker }) => {
    assert.equal((await exchange.manifest()).bundle_count, 0);
    const options = worker(desktop, 'real-subprocess');
    options.runAnalysis = undefined; // Exercise the actual child lifecycle and environment.
    const result = await runDailyProcessing(options);
    assert.equal(result.status, 'completed');
    const store = await openLocalStore(desktop);
    try {
      const marker = await state<{ job_id: string; completed: boolean }>(store.c, 'last_backfill_run');
      assert.equal(marker?.job_id, result.job_id); assert.equal(marker?.completed, true);
      assert.equal((await query<{ n: number }>(store.c, 'SELECT COUNT(*) AS n FROM collection_batches'))[0].n, 0);
      assert.equal((await query<{ n: number }>(store.c, 'SELECT COUNT(*) AS n FROM otp_schedules'))[0].n, 0, 'child must not download a current schedule');
    } finally { store.c.closeSync(); store.db.closeSync(); }
    const summary = JSON.parse(await readFile(join(server, 'summary.json'), 'utf8'));
    assert.equal(summary.processing.job_id, result.job_id); assert.equal(summary.processing.worker_id, 'real-subprocess');
    assert.equal(summary.row_study.coverage.passages, 0); assert.equal(summary.signal_study.cells.length, 0);
    assert.ok(summary.row_study.network.paths.length > 0, 'the real analysis loads its checked-in route catalog');
    assert.equal((await coordinator.status()).last_completed!.job_id, result.job_id);
  });
});
