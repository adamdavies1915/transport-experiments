import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer, request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { gzipSync, gunzipSync } from 'node:zlib';
import { createSummaryTransferHandler, publishSummaryIfConfigured, pushSummaryFile, SUMMARY_TRANSFER_MAX_BYTES } from './summary-transfer';

const readToken = 'summary-read-fixture-'.repeat(3), publishToken = 'summary-publish-fixture-'.repeat(3);
const summary = { schema_version: 1, generated_at: '2026-09-13T04:00:00Z', source_quality: { status: 'ready', sources: [
  { id: 'sse', last_received_at: '2026-09-13T03:59:00Z', last_provider_at: '2026-09-13T03:58:00Z' },
] } };
async function fixture(run: (value: { directory: string; url: string }) => Promise<void>, maxBytes = 4096) {
  const directory = await mkdtemp(join(tmpdir(), 'summary-transfer-'));
  const handle = createSummaryTransferHandler({ directory, readToken, publishToken, maxBytes });
  const server = createServer((req, res) => { void handle(req, res).then(handled => { if (!handled) { res.statusCode = 404; res.end(); } }).catch(() => res.destroy()); });
  try {
    await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
    const address = server.address(); assert.ok(address && typeof address !== 'string');
    await run({ directory, url: `http://127.0.0.1:${address.port}/internal/summary` });
  } finally {
    server.closeAllConnections();
    await new Promise<void>(resolve => server.close(() => resolve()));
    await rm(directory, { recursive: true, force: true });
  }
}
function post(url: string, value: unknown, token = publishToken) {
  return fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(value) });
}

test('read and write authentication remain separate and rejected large bodies are not accepted', async () => fixture(async ({ directory, url }) => {
  assert.equal((await fetch(url)).status, 401);
  assert.equal((await post(url, summary, readToken)).status, 401);
  assert.equal((await fetch(url, { headers: { Authorization: `Bearer ${publishToken}` } })).status, 401);
  assert.equal((await fetch(url, { headers: { Authorization: `Bearer ${readToken}` } })).status, 503);
  const status = await new Promise<number>((resolve, reject) => {
    const req = request(url, { method: 'POST', headers: { 'Content-Length': 100_000_000, 'Content-Type': 'application/json' } }, res => {
      resolve(res.statusCode!); res.resume();
    });
    req.on('error', reject); req.flushHeaders();
  });
  assert.equal(status, 401, 'authentication finishes without waiting for the declared large body');
  assert.deepEqual(await readdir(directory), []);
  assert.throws(() => createSummaryTransferHandler({ directory, readToken, publishToken: readToken }), /different/);
  assert.throws(() => createSummaryTransferHandler({ directory, publishToken: 'short' }), /32/);
}));

test('gzip publication persists only the parsed public envelope and GET retains actual source clocks', async () => fixture(async ({ directory, url }) => {
  const response = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${publishToken}`, 'Content-Type': 'application/json', 'Content-Encoding': 'gzip' },
    body: gzipSync(JSON.stringify({ ...summary, private_token: 'must not be published' })) });
  assert.equal(response.status, 200);
  assert.deepEqual(JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8')), summary);
  const saved = await fetch(url, { headers: { Authorization: `Bearer ${readToken}`, 'Accept-Encoding': 'gzip' } });
  assert.equal(saved.status, 200); assert.equal(saved.headers.get('Content-Encoding'), 'gzip');
  assert.deepEqual(await saved.json(), summary);
  assert.deepEqual(await readdir(directory), ['summary.json']);
  const plain = await fetch(url, { headers: { Authorization: `Bearer ${readToken}`, 'Accept-Encoding': 'gzip;q=0' } });
  assert.equal(plain.headers.get('Content-Encoding'), null);
}));

test('invalid JSON, malformed gzip, stale summaries and decoded or encoded overflow preserve last good publication', async () => fixture(async ({ directory, url }) => {
  assert.equal((await post(url, summary)).status, 200);
  const before = await readFile(join(directory, 'summary.json'), 'utf8');
  for (const [body, encoding, status] of [
    [Buffer.from('{broken'), 'identity', 400],
    [Buffer.from('not gzip'), 'gzip', 400],
    [Buffer.alloc(4097, 120), 'identity', 413],
    [gzipSync(Buffer.alloc(100_000, 120)), 'gzip', 413],
    [Buffer.concat([gzipSync(Buffer.alloc(3000, 120)), gzipSync(Buffer.alloc(3000, 120))]), 'gzip', 413],
    [Buffer.from('{}'), 'br', 415],
  ] as const) {
    const response = await fetch(url, { method: 'POST', headers: { Authorization: `Bearer ${publishToken}`, 'Content-Type': 'application/json', 'Content-Encoding': encoding }, body });
    assert.equal(response.status, status);
    assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), before);
  }
  assert.equal((await post(url, { ...summary, generated_at: '2026-09-12T04:00:00Z' })).status, 409);
  assert.equal((await post(url, { ...summary, row_study: {} })).status, 400);
  assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), before);
}));

test('publisher verifies local JSON before sending, uses gzip with no redirects, and retains local file on failure', async () => fixture(async ({ directory, url }) => {
  const local = join(directory, 'local-summary.json');
  await writeFile(local, JSON.stringify({ ...summary, private_token: 'stripped' }));
  assert.equal((await pushSummaryFile({ path: local, url, token: publishToken })).published, true);
  let calls = 0;
  const failure: typeof fetch = async (_url, options) => {
    calls++; assert.equal(options?.redirect, 'error'); assert.ok(options?.signal);
    assert.equal((options?.headers as Record<string, string>)['Content-Encoding'], 'gzip');
    assert.deepEqual(JSON.parse(gunzipSync(Buffer.from(options?.body as Uint8Array)).toString('utf8')), summary);
    throw new Error(`fixture error that contains ${publishToken}`);
  };
  const result = await pushSummaryFile({ path: local, url, token: publishToken, fetcher: failure });
  assert.equal(result.published, false); assert.ok(!result.reason?.includes(publishToken));
  assert.equal(JSON.parse(await readFile(local, 'utf8')).private_token, 'stripped');
  await writeFile(local, 'broken');
  assert.equal((await pushSummaryFile({ path: local, url, token: publishToken, fetcher: failure })).published, false);
  assert.equal(calls, 1, 'invalid local publication never reaches the network');
}));

test('publisher rejects unsafe endpoints and configuration without affecting local analysis', async () => {
  let calls = 0; const logs: string[] = [];
  const fetcher: typeof fetch = async () => { calls++; throw new Error('Unexpected network'); };
  for (const url of ['http://remote.example/internal/summary', 'https://name:password@example.test/internal/summary', 'https://example.test/internal/summary?token=x', 'https://example.test/internal/summary#fragment', 'https://example.test/wrong']) {
    assert.equal((await pushSummaryFile({ path: '/nonexistent', url, token: publishToken, fetcher })).published, false);
  }
  await publishSummaryIfConfigured('/nonexistent', { env: {}, fetcher, log: message => logs.push(message) });
  await publishSummaryIfConfigured('/nonexistent', { env: { TRANSIT_SUMMARY_PUBLISH_TOKEN: publishToken }, fetcher, log: message => logs.push(message) });
  await publishSummaryIfConfigured('/nonexistent', { env: { PROCESSING_JOB_ID: 'leased-job', TRANSIT_SUMMARY_PUBLISH_URL: 'https://example.test/internal/summary', TRANSIT_SUMMARY_PUBLISH_TOKEN: publishToken }, fetcher, log: message => logs.push(message) });
  assert.equal(calls, 0); assert.equal(logs.length, 1); assert.ok(!logs[0].includes(publishToken));
});

test('the actual finite local worker publishes completed files to the bridge without cloud access', { timeout: 30_000 }, async () => fixture(async ({ directory, url }) => {
  const data = join(directory, 'worker');
  const child = spawn(process.execPath, ['--import', 'tsx', 'src/local-worker.ts', '--backfill'], { cwd: process.cwd(), stdio: ['ignore', 'pipe', 'pipe'], env: {
    ...process.env, NODE_TEST_CONTEXT: undefined, TRANSIT_DATA_DIR: data, LOCAL_DB_MEMORY: '256MB', LOCAL_DB_THREADS: '1',
    MOTHERDUCK_BOOTSTRAP: 'false', MOTHERDUCK_CLOUD_WRITES: 'false', MOTHER_DUCK_API_KEY: '', OTP_SCHEDULE_REFRESH: 'false', PROCESSING_JOB_ID: undefined,
    TRANSIT_SUMMARY_PUBLISH_URL: url, TRANSIT_SUMMARY_PUBLISH_TOKEN: publishToken,
  } });
  let output = ''; child.stdout.on('data', chunk => { output += String(chunk); }); child.stderr.on('data', chunk => { output += String(chunk); });
  const timer = setTimeout(() => child.kill('SIGKILL'), 25_000);
  try {
    const code = await new Promise<number | null>((resolve, reject) => { child.once('close', resolve); child.once('error', reject); });
    assert.equal(code, 0, output); assert.match(output, /\[Summary upload\] Published/);
    const local = JSON.parse(await readFile(join(data, 'summary.json'), 'utf8'));
    const remote = JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8'));
    assert.deepEqual(remote, local);
  } finally { clearTimeout(timer); if (child.exitCode == null && child.signalCode == null) child.kill('SIGKILL'); }
}, SUMMARY_TRANSFER_MAX_BYTES));
