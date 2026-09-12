import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable, Writable } from 'node:stream';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { gzipSync } from 'node:zlib';
import { CaptureExchange } from './capture-exchange';
import { LocalJournal } from './local-journal';
import type { ProcessingCoordinator } from './processing-coordinator';
import { processingRequestHandler } from './processing-server';

const TOKEN = 'synthetic-processing-api-token-'.repeat(2), ROOT = '/internal/processing';
async function request(handler: ReturnType<typeof processingRequestHandler>, url: string, options: { method?: string; token?: string | null; body?: string } = {}) {
  const req = Readable.from(options.body === undefined ? [] : [Buffer.from(options.body)]);
  req.on('error', () => {});
  Object.assign(req, { url, method: options.method ?? 'GET', headers: { authorization: options.token === null ? undefined : 'Bearer ' + (options.token ?? TOKEN) } });
  const chunks: Buffer[] = [], headers: Record<string, string> = {};
  const res = new Writable({ write(chunk, _encoding, done) { chunks.push(Buffer.from(chunk)); done(); } });
  Object.assign(res, { statusCode: 200, headersSent: false, setHeader(name: string, value: string | number) { headers[name.toLowerCase()] = String(value); } });
  const done = new Promise<void>((resolve, reject) => { res.once('finish', resolve); res.once('error', reject); });
  const handled = await handler(req as unknown as IncomingMessage, res as unknown as ServerResponse);
  if (!handled) res.end();
  await done;
  return { handled, status: (res as unknown as ServerResponse).statusCode, headers, bytes: Buffer.concat(chunks) };
}

test('every processing route rejects missing or incorrect credentials before reading private state or performing work', async () => {
  let calls = 0;
  const denied = async () => { calls++; throw new Error('Private state must remain untouched'); };
  const handler = processingRequestHandler({ token: TOKEN, seal: denied,
    exchange: { manifest: denied, bundlePath: denied, schedulePath: denied } as unknown as CaptureExchange,
    coordinator: { status: denied, claim: denied, renew: denied, release: denied, complete: denied } as unknown as ProcessingCoordinator });
  for (const path of ['/status', '/manifest', '/bundles/000000000001', '/schedules/' + 'a'.repeat(64), '/claim', '/renew', '/release', '/complete']) {
    for (const token of [null, TOKEN.slice(0, -1) + '!']) {
      const response = await request(handler, ROOT + path, { method: ['/claim', '/renew', '/release', '/complete'].includes(path) ? 'POST' : 'GET', token, body: 'not valid JSON' });
      assert.equal(response.status, 401); assert.equal(response.headers['cache-control'], 'no-store');
      assert.deepEqual(JSON.parse(response.bytes.toString()), { error: 'Authentication required' });
    }
  }
  assert.equal(calls, 0);
});

test('processing routes reject traversal, malformed bodies and oversized control requests without invoking capture or coordinator operations', async () => {
  let calls = 0;
  const denied = async () => { calls++; throw new Error('Unexpected private operation'); };
  const handler = processingRequestHandler({ token: TOKEN, seal: denied,
    exchange: { manifest: denied, bundlePath: denied, schedulePath: denied } as unknown as CaptureExchange,
    coordinator: { status: denied, claim: denied, renew: denied, release: denied, complete: denied } as unknown as ProcessingCoordinator });
  for (const path of ['/bundles/../identity.json', '/bundles/%2e%2e', '/schedules/../../identity.json', '/bundles/000000000001?file=identity', '/claim']) {
    assert.equal((await request(handler, ROOT + path)).status, 404);
  }
  for (const body of ['{', '[]', 'null', ' '.repeat(8193)]) {
    assert.equal((await request(handler, ROOT + '/claim', { method: 'POST', body })).status, 409);
  }
  assert.equal((await request(handler, '/health')).handled, false);
  assert.equal(calls, 0);
});

test('authenticated asset transport streams exact immutable bytes and exposes no filesystem paths in its response body', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'processing-server-'));
  try {
    const journal = new LocalJournal(directory); await journal.init();
    const exchange = new CaptureExchange(directory); await exchange.init();
    await writeFile(join(journal.pending, 'fixture.json.gz'), gzipSync(JSON.stringify({ schema_version: 1, batch_id: 'original-batch', source: 'sse', received_at: '2026-09-12T15:00:00.123456Z', observations: [] })));
    const bundle = (await exchange.seal(journal))!, schedule = await exchange.recordSchedule(Buffer.from('retained schedule fixture'), 'https://example.org/GTFS.zip', '2026-09-12T14:00:00Z');
    const handler = processingRequestHandler({ token: TOKEN, exchange, seal: async () => {}, coordinator: {} as ProcessingCoordinator });
    for (const [route, path, length] of [
      ['/bundles/' + bundle.id, await exchange.bundlePath(bundle.id), bundle.bytes],
      ['/schedules/' + schedule.id, await exchange.schedulePath(schedule.id), schedule.bytes],
    ] as const) {
      const original = await readFile(path), response = await request(handler, ROOT + route);
      assert.equal(response.status, 200); assert.equal(response.headers['content-type'], 'application/octet-stream');
      assert.equal(response.headers['content-length'], String(length)); assert.equal(response.headers['cache-control'], 'no-store');
      assert.deepEqual(response.bytes, original); assert.deepEqual(await readFile(path), original);
    }
    const listed = await request(handler, ROOT + '/manifest');
    assert.equal(listed.status, 200); assert.equal(listed.bytes.toString().includes(directory), false);
    assert.equal(JSON.parse(listed.bytes.toString()).bundle_count, 1);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
