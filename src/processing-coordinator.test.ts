import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { atomicFile } from './local-journal';
import { captureManifestDigest, type CaptureManifest } from './capture-exchange';
import { latestProcessingDate, ProcessingCoordinator, PROCESSING_SUMMARY_MAX_BYTES,
  type ProcessingCoordinatorOptions, type ProcessingLease } from './processing-coordinator';
import { rowData, signalData } from '../dashboard/src/study-fixtures';

const START = Date.parse('2026-09-14T11:00:00.000Z'); // 06:00 Chicago; September 13 is due.
const BASELINE = 'verified-seed', REVISION = 'analysis-v1';
function manifest(count = 1): CaptureManifest {
  const bundles = Array.from({ length: count }, (_, index) => ({ id: String(index + 1).padStart(12, '0'), sequence: index + 1,
    sha256: String(index + 1).padStart(64, '0'), bytes: 100, uncompressed_bytes: 300, frames: 1,
    first_received_at: '2026-09-13T23:00:00.000Z', last_received_at: '2026-09-13T23:00:00.000Z', sealed_at: '2026-09-14T00:00:00.000Z' }));
  const body = { schema_version: 1 as const, capture_id: '11111111-2222-3333-4444-555555555555', latest_sequence: count,
    bundles, schedules: [{ id: 'a'.repeat(64), sha256: 'a'.repeat(64), bytes: 10, source: 'https://example.org/gtfs.zip', first_seen_at: '2026-09-13T00:00:00.000Z' }],
    bundle_count: count, bundle_bytes: count * 100, schedule_bytes: 10 };
  return { ...body, manifest_sha256: captureManifestDigest(body) };
}
function redigest(value: CaptureManifest): CaptureManifest {
  const { manifest_sha256: _hash, ...body } = value;
  return { ...body, manifest_sha256: captureManifestDigest(body) };
}
const summary = (at = START, extra = {}) => JSON.stringify({ schema_version: 1, generated_at: new Date(at).toISOString(),
  row_study: rowData(), signal_study: signalData(), legacy: {}, ...extra });
const complete = (coordinator: ProcessingCoordinator, lease: ProcessingLease, body = summary()) =>
  coordinator.complete({ ...lease, summary: body });
async function fixture(run: (f: { directory: string; options: ProcessingCoordinatorOptions; coordinator: ProcessingCoordinator;
  now: () => number; setNow: (at: number) => void }) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'transit-coordinator-')); let clock = START;
  const options = { data_dir: directory, baseline_id: BASELINE, analysis_revision: REVISION,
    first_service_date: '2026-09-07', lease_seconds: 60, now: () => clock };
  try { await run({ directory, options, coordinator: new ProcessingCoordinator(options), now: () => clock, setNow: at => { clock = at; } }); }
  finally { await rm(directory, { recursive: true, force: true }); }
}

test('Chicago daily due time uses local calendar dates through spring and fall DST transitions', () => {
  assert.equal(latestProcessingDate(Date.parse('2026-09-14T10:59:59Z')), '2026-09-12');
  assert.equal(latestProcessingDate(START), '2026-09-13');
  assert.equal(latestProcessingDate(Date.parse('2026-03-08T10:59:59Z')), '2026-03-06');
  assert.equal(latestProcessingDate(Date.parse('2026-03-08T11:00:00Z')), '2026-03-07');
  assert.equal(latestProcessingDate(Date.parse('2026-11-01T11:59:59Z')), '2026-10-30');
  assert.equal(latestProcessingDate(Date.parse('2026-11-01T12:00:00Z')), '2026-10-31');
  assert.equal(latestProcessingDate(Date.parse('2026-09-14T11:29:59Z'), 6, 30), '2026-09-12');
  assert.equal(latestProcessingDate(Date.parse('2026-09-14T11:30:00Z'), 6, 30), '2026-09-13');
});

test('first online worker wins one cumulative catch-up job; unseeded or wrong-revision workers cannot claim', async () => {
  await fixture(async ({ coordinator, options }) => {
    await assert.rejects(coordinator.claim('mac', 'empty-seed', REVISION, manifest()), /baseline or analysis revision/);
    await assert.rejects(coordinator.claim('mac', BASELINE, 'old-code', manifest()), /baseline or analysis revision/);
    const contenders = [coordinator, new ProcessingCoordinator(options)];
    const leases = await Promise.all(contenders.map((c, i) => c.claim(`worker-${i}`, BASELINE, REVISION, manifest())));
    assert.equal(leases.filter(Boolean).length, 1);
    const lease = leases.find(Boolean)!;
    assert.equal(lease.fence, 1); assert.equal(lease.service_date, '2026-09-13'); assert.equal(lease.covers_from, '2026-09-07');
    assert.equal(lease.lease_seconds, 60); assert.equal(Date.parse(lease.expires_at), START + 60000);
    const status = await coordinator.status();
    assert.equal(status.active!.job_id, lease.job_id); assert.equal(status.baseline_id, BASELINE);
    assert.equal(JSON.stringify(status).includes(lease.lease_token), false);
  });
});

test('renewal extends only a live lease; expiration and release retry the immutable job with higher fences', async () => {
  await fixture(async ({ coordinator, options, setNow }) => {
    const first = (await coordinator.claim('desktop', BASELINE, REVISION, manifest()))!;
    setNow(START + 30000);
    const renewed = await coordinator.renew(first); assert.equal(Date.parse(renewed.expires_at), START + 90000);
    setNow(START + 89999); assert.equal(await coordinator.claim('mac', BASELINE, REVISION, manifest(2)), null);
    setNow(START + 90000); await assert.rejects(coordinator.renew(first), /expired/);
    const second = (await new ProcessingCoordinator(options).claim('mac', BASELINE, REVISION, manifest(2)))!;
    assert.equal(second.job_id, first.job_id); assert.deepEqual(second.manifest, first.manifest);
    assert.equal(second.fence, first.fence + 1); assert.notEqual(second.lease_token, first.lease_token);
    await assert.rejects(coordinator.release(first), /fenced/);
    await coordinator.release(second);
    const third = (await coordinator.claim('desktop', BASELINE, REVISION, manifest(3)))!;
    assert.equal(third.job_id, first.job_id); assert.equal(third.fence, second.fence + 1);
    assert.deepEqual(third.manifest, first.manifest);
  });
});

test('not-due dates do not create jobs and completed daily generations wait until the next due date', async () => {
  await fixture(async ({ directory, options, setNow }) => {
    const coordinator = new ProcessingCoordinator({ ...options, first_service_date: '2026-09-14' });
    assert.equal(await coordinator.claim('mac', BASELINE, REVISION, manifest()), null);
    setNow(START + 86400000);
    const lease = (await coordinator.claim('mac', BASELINE, REVISION, manifest()))!;
    await complete(coordinator, lease, summary(START + 86400000));
    assert.equal(await coordinator.claim('desktop', BASELINE, REVISION, manifest(2)), null);
    setNow(START + 2 * 86400000);
    const next = (await coordinator.claim('desktop', BASELINE, REVISION, manifest(2)))!;
    assert.equal(next.service_date, '2026-09-15'); assert.equal(next.covers_from, '2026-09-15');
    assert.notEqual(next.job_id, lease.job_id);
    assert.equal(JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8')).processing.service_date, '2026-09-14');
  });
});

test('new generations must retain every earlier bundle and schedule and pass manifest digest and sequence checks', async () => {
  await fixture(async ({ coordinator, directory, setNow }) => {
    const first = (await coordinator.claim('desktop', BASELINE, REVISION, manifest(2)))!; await complete(coordinator, first);
    const published = await readFile(join(directory, 'summary.json'), 'utf8'); setNow(START + 86400000);
    await assert.rejects(coordinator.claim('mac', BASELINE, REVISION, manifest(1)), /regresses/);
    const changed = manifest(3); changed.bundles[0].sha256 = 'f'.repeat(64);
    await assert.rejects(coordinator.claim('mac', BASELINE, REVISION, redigest(changed)), /changes an accepted bundle/);
    const missing = manifest(3); missing.schedules = []; missing.schedule_bytes = 0;
    await assert.rejects(coordinator.claim('mac', BASELINE, REVISION, redigest(missing)), /accepted schedule/);
    const gap = manifest(3); gap.bundles.shift();
    await assert.rejects(coordinator.claim('mac', BASELINE, REVISION, redigest(gap)), /sequence gap/);
    const badHash = manifest(3); badHash.manifest_sha256 = '0'.repeat(64);
    await assert.rejects(coordinator.claim('mac', BASELINE, REVISION, badHash), /checksum/);
    assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), published);
    const reordered = manifest(3);
    reordered.bundles = reordered.bundles.map(b => Object.fromEntries(Object.entries(b).reverse()) as unknown as typeof b);
    reordered.schedules = reordered.schedules.map(s => Object.fromEntries(Object.entries(s).reverse()) as unknown as typeof s);
    assert.equal((await coordinator.claim('mac', BASELINE, REVISION, reordered))!.manifest.latest_sequence, 3);
  });
});

test('expired or fenced owners and wrong manifests never replace the published summary', async () => {
  await fixture(async ({ coordinator, directory, setNow }) => {
    await atomicFile(join(directory, 'summary.json'), 'previous publication');
    const first = (await coordinator.claim('desktop', BASELINE, REVISION, manifest()))!;
    await assert.rejects(coordinator.complete({ ...first, manifest_sha256: 'b'.repeat(64), summary: summary() }), /fixed input manifest/);
    setNow(START + 60000);
    await assert.rejects(complete(coordinator, first), /expired/);
    const second = (await coordinator.claim('mac', BASELINE, REVISION, manifest(2)))!;
    await assert.rejects(complete(coordinator, first), /fenced/);
    assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), 'previous publication');
    await complete(coordinator, second, summary(START + 60000));
    const published = JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8'));
    assert.equal(published.processing.worker_id, 'mac'); assert.equal(published.processing.manifest_sha256, first.manifest_sha256);
    assert.equal(JSON.stringify(published).includes(second.lease_token), false);
  });
});

test('completion validates public summary and size, ignores injected metadata, and replays only identical accepted content', async () => {
  await fixture(async ({ coordinator, directory }) => {
    const lease = (await coordinator.claim('desktop', BASELINE, REVISION, manifest()))!;
    await assert.rejects(complete(coordinator, lease, 'not json'));
    await assert.rejects(complete(coordinator, lease, JSON.stringify({ schema_version: 1, generated_at: new Date(START).toISOString() })), /both studies/);
    await assert.rejects(coordinator.complete({ ...lease, summary: Buffer.alloc(PROCESSING_SUMMARY_MAX_BYTES + 1) }), /exceeds 60 MiB/);
    const body = summary(START, { token: 'must-not-publish' });
    const first = await complete(coordinator, lease, body); assert.equal(first.replayed, false);
    const bytes = await readFile(join(directory, 'summary.json'), 'utf8'); assert.equal(bytes.includes('must-not-publish'), false);
    const replay = await complete(coordinator, lease, body); assert.equal(replay.replayed, true); assert.equal(replay.summary_sha256, first.summary_sha256);
    await assert.rejects(complete(coordinator, lease, summary(START, { legacy: { errors: { changed: 'different' } } })), /cannot be changed/);
    await assert.rejects(coordinator.complete({ ...lease, fence: lease.fence + 1, summary: body }), /another owner/);
    assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), bytes);
  });
});

test('crashes immediately before and after summary replacement recover accepted publication before another lease is issued', async () => {
  for (const phase of ['before', 'after'] as const) await fixture(async ({ directory, options, setNow }) => {
    await atomicFile(join(directory, 'summary.json'), 'previous publication'); let crash = true;
    const coordinator = new ProcessingCoordinator({ ...options, write_atomic: async (path, body) => {
      if (path === join(directory, 'summary.json') && crash) {
        crash = false;
        if (phase === 'after') await atomicFile(path, body);
        throw new Error('simulated power loss');
      }
      await atomicFile(path, body);
    } });
    const lease = (await coordinator.claim('desktop', BASELINE, REVISION, manifest()))!;
    await assert.rejects(complete(coordinator, lease), /simulated power loss/);
    if (phase === 'before') assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), 'previous publication');
    setNow(START + 120000); // The intent was authorized before this lease expired.
    const restarted = new ProcessingCoordinator(options);
    assert.equal(await restarted.claim('mac', BASELINE, REVISION, manifest(2)), null);
    assert.equal((await restarted.status()).last_completed!.job_id, lease.job_id);
    assert.equal((await complete(restarted, lease)).replayed, true);
    const published = JSON.parse(await readFile(join(directory, 'summary.json'), 'utf8'));
    assert.equal(published.processing.worker_id, 'desktop');
  });
});

test('a lease expiring during durable staging cannot accept publication intent', async () => {
  await fixture(async ({ directory, options, setNow }) => {
    await atomicFile(join(directory, 'summary.json'), 'previous publication');
    const coordinator = new ProcessingCoordinator({ ...options, write_atomic: async (path, body) => {
      await atomicFile(path, body);
      if (path.includes('/publications/')) setNow(START + 60000);
    } });
    const lease = (await coordinator.claim('desktop', BASELINE, REVISION, manifest()))!;
    await assert.rejects(complete(coordinator, lease), /expired/);
    assert.equal(await readFile(join(directory, 'summary.json'), 'utf8'), 'previous publication');
    const replacement = (await new ProcessingCoordinator(options).claim('mac', BASELINE, REVISION, manifest(2)))!;
    assert.equal(replacement.job_id, lease.job_id); assert.equal(replacement.fence, lease.fence + 1);
  });
});

test('changing expected analysis revision fences pending old work without changing its immutable input', async () => {
  await fixture(async ({ coordinator, options, directory }) => {
    const first = (await coordinator.claim('desktop', BASELINE, REVISION, manifest(2)))!;
    const original = await readFile(join(directory, 'processing', 'jobs', `${first.job_id}.json`), 'utf8');
    const upgraded = new ProcessingCoordinator({ ...options, analysis_revision: 'analysis-v2' });
    await assert.rejects(upgraded.claim('mac', BASELINE, 'analysis-v2', manifest(1)), /regresses/);
    const second = (await upgraded.claim('mac', BASELINE, 'analysis-v2', manifest(3)))!;
    assert.notEqual(second.job_id, first.job_id); assert.ok(second.fence > first.fence);
    await assert.rejects(complete(upgraded, first), /fenced/);
    assert.equal(await readFile(join(directory, 'processing', 'jobs', `${first.job_id}.json`), 'utf8'), original);
    await assert.rejects(new ProcessingCoordinator({ ...options, baseline_id: 'different-seed' }).status(), /different processing baseline/);
  });
});
