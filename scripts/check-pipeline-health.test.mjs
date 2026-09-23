import { test } from 'node:test';
import assert from 'node:assert/strict';
import { checkPipeline, clockCheck } from './check-pipeline-health.mjs';

test('fresh collection and repeated summary delivery cannot hide stale analysis', async () => {
  const now = Date.parse('2026-09-23T03:00:00Z');
  const current = new Date(now).toISOString();
  const result = await checkPipeline({ now, collectorUrl: 'collector', dashboardUrl: 'dashboard',
    fetcher: async url => Response.json(url === 'collector'
      ? { connected: true, last_received_at: current, last_persisted_at: current }
      : { summary: { generated_at: '2026-09-14T05:00:00Z', received_at: current } }) });
  assert.equal(result.status, 'degraded');
  assert.equal(result.checks.collection.status, 'ok');
  assert.equal(result.checks.summary_delivery.status, 'ok');
  assert.equal(result.checks.analysis.status, 'stale');
});
test('missing, invalid and future clocks fail closed', () => {
  for (const value of [undefined, 'invalid', '2099-01-01T00:00:00Z'])
    assert.equal(clockCheck(value, 300_000, Date.parse('2026-09-23T03:00:00Z')).status, 'stale');
});
test('live SSE cannot hide a Le Pass collector whose successful requests stopped', async () => {
  const now = Date.parse('2026-09-23T03:00:00Z'), current = new Date(now).toISOString();
  const result = await checkPipeline({ now, collectorUrl: 'collector', dashboardUrl: 'dashboard', localUrl: 'local',
    fetcher: async url => Response.json(url === 'collector'
      ? { connected: true, last_received_at: current, last_persisted_at: current }
      : url === 'dashboard' ? { summary: { generated_at: current, received_at: current } }
        : { last_persisted_at: current, lepass: { status: 'collecting', queries: [{ lastSuccess: '2026-09-23T02:00:00Z' }] } }) });
  assert.equal(result.checks.durable_capture.status, 'ok');
  assert.equal(result.checks.lepass.status, 'stale');
  assert.equal(result.status, 'degraded');
});
