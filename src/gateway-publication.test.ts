import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { publishGatewaySummary } from '../scripts/process-server-history';

test('a rejected gateway upload remains retryable and is never marked published', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'transit-gateway-'));
  try {
    await mkdir(join(dir, 'processing'));
    const marker = join(dir, 'processing/gateway-published.json');
    await assert.rejects(publishGatewaySummary(dir, 'job', 'url', 'token',
      async () => ({ published: false, reason: 'HTTP 503' })), /HTTP 503/);
    await assert.rejects(readFile(marker), { code: 'ENOENT' });
    await publishGatewaySummary(dir, 'job', 'url', 'token', async () => ({ published: true }));
    assert.equal(JSON.parse(await readFile(marker, 'utf8')).job_id, 'job');
    await assert.rejects(publishGatewaySummary(dir, 'next-job', 'url', 'token',
      async () => ({ published: false })), /publication failed/);
    assert.equal(JSON.parse(await readFile(marker, 'utf8')).job_id, 'job');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
