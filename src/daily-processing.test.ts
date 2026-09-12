import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ProcessingClient, localProcessingLock, runDailyProcessing } from './daily-processing';

const token = 'offline-fixture-token-'.repeat(3);
const payload = Buffer.from('small verified capture fixture');
const expected = { bytes: payload.length, sha256: createHash('sha256').update(payload).digest('hex') };
async function fixture(run: (directory: string) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'transit-daily-client-'));
  try { await run(directory); } finally { await rm(directory, { recursive: true, force: true }); }
}

test('processing client accepts HTTPS or loopback tunnels and rejects credentials, URL extras and short tokens', () => {
  for (const serverUrl of ['https://processing.example', 'http://127.0.0.1:3100', 'http://localhost:3100', 'http://[::1]:3100']) {
    assert.match(new ProcessingClient({ serverUrl, token }).base, /\/internal\/processing$/);
  }
  for (const serverUrl of ['http://processing.example', 'http://localhost.other.example', 'ftp://processing.example', 'https://name:fake-password@processing.example', 'https://processing.example?token=fixture', 'https://processing.example#fragment']) {
    assert.throws(() => new ProcessingClient({ serverUrl, token }), /HTTPS/);
  }
  assert.throws(() => new ProcessingClient({ serverUrl: 'https://processing.example', token: 'short' }), /at least 32/);
});

test('capture download verifies bytes, reuses valid archives and refuses to replace corrupt existing files', async () => fixture(async directory => {
  let calls = 0;
  const client = new ProcessingClient({ serverUrl: 'https://processing.example', token, fetcher: async (url, options) => {
    calls++;
    assert.equal(String(url), 'https://processing.example/internal/processing/bundles/fixture');
    assert.equal((options?.headers as Record<string, string>).Authorization, `Bearer ${token}`);
    assert.equal(options?.redirect, 'error');
    assert.ok(options?.signal);
    return new Response(payload);
  } });
  const file = join(directory, 'capture.gz');
  await client.download('/bundles/fixture', file, expected, 1024);
  assert.deepEqual(await readFile(file), payload);
  await client.download('/bundles/fixture', file, expected, 1024);
  assert.equal(calls, 1, 'a verified local archive does not redownload');
  await writeFile(file, 'existing corrupt data');
  await assert.rejects(client.download('/bundles/fixture', file, expected, 1024), /Existing local capture archive failed verification/);
  assert.equal(await readFile(file, 'utf8'), 'existing corrupt data');
  assert.equal(calls, 1);
  await assert.rejects(client.download('/bundles/fixture', join(directory, 'oversized'), { ...expected, bytes: 1025 }, 1024), /Invalid download size/);
  assert.equal(calls, 1);
}));

test('mismatched, oversized and interrupted downloads never publish partial capture files', async () => fixture(async directory => {
  const responses = [
    () => new Response(Buffer.from('x'.repeat(payload.length))),
    () => new Response(payload.subarray(0, payload.length - 1)),
    () => new Response(Buffer.concat([payload, Buffer.from('too long')])),
    () => new Response(new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(payload.subarray(0, 5)); },
      pull(controller) { controller.error(new Error('fixture interrupted response')); },
    })),
  ];
  for (const response of responses) {
    const client = new ProcessingClient({ serverUrl: 'https://processing.example', token, fetcher: async () => response() });
    await assert.rejects(client.download('/bundles/fixture', join(directory, 'capture.gz'), expected, 1024), /checksum mismatch|size limit|interrupted response/);
    assert.deepEqual(await readdir(directory), [], 'neither final nor temporary capture file survives a failed response');
  }
}));

test('native local locks reject overlap and release after normal completion or configuration failure', async () => fixture(async directory => {
  const unlock = await localProcessingLock(directory);
  try { await assert.rejects(localProcessingLock(directory), /already running/); } finally { await unlock(); }
  await (await localProcessingLock(directory))();
  let calls = 0;
  const fetcher: typeof fetch = async () => { calls++; throw new Error('Unexpected network request'); };
  for (const options of [{ serverUrl: 'http://non-loopback.example', token }, { serverUrl: 'https://processing.example', token: 'short' }]) {
    await assert.rejects(runDailyProcessing({ dataDirectory: directory, ...options, fetcher, log: () => {} }), /HTTPS|at least 32/);
    await (await localProcessingLock(directory))();
  }
  // A later setup error also releases the lock before an operator retries.
  await assert.rejects(runDailyProcessing({ dataDirectory: directory, serverUrl: 'https://processing.example', token, fetcher, log: () => {} }), /ENOENT/);
  await (await localProcessingLock(directory))();
  assert.equal(calls, 0);
  assert.deepEqual(await readdir(directory), ['processing'], 'configuration failures never open a research database');
}));

test('the separate native lock blocks another process and releases after that process dies', { timeout: 20000 }, async () => fixture(async directory => {
  const moduleUrl = new URL('./daily-processing.ts', import.meta.url).href;
  const source = `process.stdout.write('starting\\n'); const {localProcessingLock}=await import(${JSON.stringify(moduleUrl)}); process.stdout.write('imported\\n'); await localProcessingLock(process.argv[1]); process.stdout.write('locked\\n'); setInterval(()=>{},1000);`;
  const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', source, directory], { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, NODE_TEST_CONTEXT: undefined } });
  let stderr = '';
  child.stderr.on('data', chunk => { stderr += String(chunk); });
  const exit = new Promise<void>((resolve, reject) => { child.once('exit', () => resolve()); child.once('error', reject); });
  try {
    await new Promise<void>((resolve, reject) => {
      let output = '';
      const timer = setTimeout(() => reject(new Error('Fixture child did not acquire its lock: ' + JSON.stringify({ stdout: output.slice(0, 200), stderr: stderr.slice(0, 200) }))), 10000);
      child.stdout.on('data', chunk => { output += String(chunk); if (output.includes('locked\n')) { clearTimeout(timer); resolve(); } });
      child.once('exit', () => { clearTimeout(timer); reject(new Error('Fixture child exited before lock: ' + stderr)); });
      child.once('error', error => { clearTimeout(timer); reject(error); });
    });
    await assert.rejects(localProcessingLock(directory), /lock|conflict|process/i);
    child.kill('SIGKILL');
    await exit;
    await (await localProcessingLock(directory))();
  } finally { if (child.exitCode == null && child.signalCode == null) child.kill('SIGKILL'); await exit; }
}));
