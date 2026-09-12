import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, open, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync, gunzipSync } from 'node:zlib';
import { zipSync, strToU8 } from 'fflate';
import { LocalJournal } from './local-journal';
import { CAPTURE_LIMITS, CaptureExchange, captureManifestDigest, decodeCaptureBundle, validateCaptureManifest, type CaptureBundle } from './capture-exchange';
import type { CollectionBatch } from './observation-types';

const sha = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
function batch(id: string, received = '2026-09-12T14:30:00.123456Z'): CollectionBatch {
  return { schema_version: 1, batch_id: id, source: 'lepass', received_at: received, observations: [{
    observation: { source: 'lepass', observation_id: id + ':0', vehicle_id: 'lepass:00460', provider_vehicle_id: '00460',
      route_id: '12', trip_id: '000001', observed_at: 1789223380.123, received_at: Date.parse(received) / 1000,
      lat: 29.95, lon: -90.1, speed_mph: null, off_route: false, location_source: 'provider_gps', timestamp_precision_seconds: 0.001,
      direction_id: '0', pattern_id: '7', mapping_confidence: 'verified' }, raw: { provider_clock: 'original', unknown_field: ['retained'] },
  }], predictions: [{ destination_trip: '9999999999999999999', prediction_clock: 1789223391 }], provenance: { query: 12, receipt: received } };
}
async function fixture(run: (directory: string, journal: LocalJournal, exchange: CaptureExchange) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'capture-exchange-'));
  const journal = new LocalJournal(directory), exchange = new CaptureExchange(directory);
  try { await journal.init(); await exchange.init(); await run(directory, journal, exchange); }
  finally { await rm(directory, { recursive: true, force: true }); }
}
async function put(journal: LocalJournal, value: CollectionBatch): Promise<{ name: string; path: string; bytes: Buffer }> {
  const name = value.received_at.replace(/[^0-9]/g, '') + '-' + value.batch_id + '.json.gz';
  const path = join(journal.pending, name), bytes = gzipSync(JSON.stringify(value));
  await writeFile(path, bytes); return { name, path, bytes };
}

test('sealing preserves exact gzip frames, batch IDs, clocks, predictions and null speed; later arrivals remain for the next bundle', async () => {
  await fixture(async (directory, journal, exchange) => {
    const first = batch('first'), second = batch('second', '2026-09-12T14:30:01.654321Z');
    const saved = [await put(journal, first), await put(journal, second)];
    const files = journal.files.bind(journal);
    journal.files = async limit => { const names = await files(limit); await put(journal, batch('later', '2026-09-12T14:30:02Z')); return names; };
    const descriptor = (await exchange.seal(journal))!; journal.files = files;
    assert.equal(descriptor.sequence, 1); assert.equal(descriptor.frames, 2);
    assert.equal(descriptor.first_received_at, first.received_at); assert.equal(descriptor.last_received_at, second.received_at);
    assert.equal((await journal.files()).length, 1);
    const manifest = await exchange.manifest(), bytes = await readFile(await exchange.bundlePath(descriptor.id));
    const envelope: CaptureBundle = JSON.parse(gunzipSync(bytes).toString());
    assert.deepEqual(envelope.frames.map(f => f.name), saved.map(f => f.name));
    envelope.frames.forEach((frame, i) => { assert.deepEqual(Buffer.from(frame.data_base64, 'base64'), saved[i].bytes); assert.equal(frame.sha256, sha(saved[i].bytes)); });
    assert.deepEqual(decodeCaptureBundle(bytes, descriptor, manifest.capture_id), [first, second]);
    assert.equal(manifest.bundle_bytes, bytes.length); assert.equal(manifest.bundle_count, 1);
    assert.deepEqual(await new CaptureExchange(directory).manifest(), manifest);
    assert.equal((await exchange.seal(journal))!.sequence, 2);
    assert.equal(await exchange.seal(journal), null);
    assert.equal((await exchange.manifest()).bundles.length, 2, 'sealed bundles remain retained');
  });
});

test('interrupted bundle writes, manifest commits and partial cleanup recover without resequencing or losing frames', async () => {
  for (const phase of ['before_bundle', 'before_manifest', 'during_cleanup'] as const) await fixture(async (directory, journal, exchange) => {
    await put(journal, batch('first')); await put(journal, batch('second'));
    const interrupted = join(exchange.directory, 'bundles', phase === 'before_bundle' ? '000000000001.json.gz' : '000000000001.manifest.json');
    const acknowledge = journal.acknowledge.bind(journal);
    if (phase === 'during_cleanup') journal.acknowledge = async path => { await acknowledge(path); throw new Error('simulated process interruption'); };
    else await mkdir(interrupted);
    await assert.rejects(exchange.seal(journal));
    assert.equal((await journal.files()).length, phase === 'during_cleanup' ? 1 : 2);
    journal.acknowledge = acknowledge;
    if (phase !== 'during_cleanup') await rm(interrupted, { recursive: true });
    const restarted = new CaptureExchange(directory); await restarted.init();
    const recovered = (await restarted.seal(journal))!;
    assert.equal(recovered.id, '000000000001'); assert.equal(recovered.frames, 2);
    assert.equal((await journal.files()).length, 0); assert.equal(await restarted.seal(journal), null);
    const manifest = await restarted.manifest(); assert.equal(manifest.bundle_count, 1);
    assert.equal(decodeCaptureBundle(await readFile(await restarted.bundlePath(recovered.id)), recovered, manifest.capture_id).length, 2);
    assert.equal((await readdir(restarted.directory)).includes('seal.pending.json'), false);
  });
});

test('corrupt, oversized and escaping source frames fail without deleting any original evidence', async () => {
  for (const corruption of ['gzip', 'json', 'inflated', 'compressed', 'symlink', 'traversal'] as const) await fixture(async (directory, journal, exchange) => {
    const good = await put(journal, batch('a-good')), bad = join(journal.pending, '999-bad.json.gz');
    if (corruption === 'gzip') await writeFile(bad, 'broken gzip');
    if (corruption === 'json') await writeFile(bad, gzipSync('{"schema_version":1}'));
    if (corruption === 'inflated') await writeFile(bad, gzipSync(' '.repeat(CAPTURE_LIMITS.frame_json_bytes + 1)));
    if (corruption === 'compressed') { const file = await open(bad, 'w'); await file.truncate(CAPTURE_LIMITS.frame_bytes + 1); await file.close(); }
    if (corruption === 'symlink') await symlink(good.path, bad);
    if (corruption === 'traversal') { const outside = join(directory, 'outside.json.gz'); await writeFile(outside, good.bytes); journal.files = async () => [outside]; }
    await assert.rejects(exchange.seal(journal));
    assert.deepEqual(await readFile(good.path), good.bytes);
    assert.equal((await exchange.manifest()).bundle_count, 0);
  });
});

test('corrupt committed bundles block cleanup and client decoding rejects nested checksums, identity and manifest omissions', async () => {
  await fixture(async (directory, journal, exchange) => {
    const original = await put(journal, batch('protected'));
    const acknowledge = journal.acknowledge.bind(journal);
    journal.acknowledge = async () => { throw new Error('interruption before cleanup'); };
    await assert.rejects(exchange.seal(journal)); journal.acknowledge = acknowledge;
    const manifest = await exchange.manifest(), descriptor = manifest.bundles[0], path = await exchange.bundlePath(descriptor.id), bytes = await readFile(path);
    const changed = Buffer.from(bytes); changed[changed.length - 1] ^= 1; await writeFile(path, changed);
    await assert.rejects(new CaptureExchange(directory).seal(journal), /checksum/);
    assert.deepEqual(await readFile(original.path), original.bytes);
    assert.throws(() => decodeCaptureBundle(changed, descriptor, manifest.capture_id), /checksum/);
    assert.throws(() => decodeCaptureBundle(bytes, descriptor, '00000000-0000-0000-0000-000000000000'), /identity/);
    const inner: CaptureBundle = JSON.parse(gunzipSync(bytes).toString()); inner.frames[0].sha256 = '0'.repeat(64);
    const innerPlain = Buffer.from(JSON.stringify(inner)), innerBytes = gzipSync(innerPlain);
    assert.throws(() => decodeCaptureBundle(innerBytes, { ...descriptor, bytes: innerBytes.length, uncompressed_bytes: innerPlain.length, sha256: sha(innerBytes) }, manifest.capture_id), /checksum/);
    assert.throws(() => validateCaptureManifest({ ...manifest, bundle_bytes: manifest.bundle_bytes + 1 }), /totals/);
    const missing = { ...manifest, bundles: [{ ...descriptor, id: '000000000002', sequence: 2 }] };
    const { manifest_sha256: _hash, ...body } = missing;
    assert.throws(() => validateCaptureManifest({ ...missing, manifest_sha256: captureManifestDigest(body) }), /sequence gap/);
    await assert.rejects(exchange.bundlePath('../identity'), /Invalid capture bundle ID/);
    await assert.rejects(exchange.schedulePath('../identity'), /Invalid capture schedule ID/);
  });
});

test('GTFS assets preserve first-seen descriptors across versions, retries and an interrupted asset write', async () => {
  await fixture(async (directory, _journal, exchange) => {
    const zip = (value: string) => zipSync({ 'feed_info.txt': strToU8('feed_version\n' + value) });
    const first = zip('one'), second = zip('two'), firstSeen = '2026-09-07T05:30:00.123456Z';
    const descriptor = await exchange.recordSchedule(first, 'https://example.org/GTFS.zip', firstSeen);
    assert.deepEqual(await exchange.recordSchedule(first, 'another-source', '2026-09-12T05:30:00Z'), descriptor);
    assert.deepEqual(await readFile(await exchange.schedulePath(descriptor.id)), Buffer.from(first));
    const interrupted = join(exchange.directory, 'schedules', sha(second) + '.zip'); await mkdir(interrupted);
    await assert.rejects(exchange.recordSchedule(second, 'original-source', '2026-09-08T05:30:00Z'));
    await rm(interrupted, { recursive: true });
    const restarted = new CaptureExchange(directory); await restarted.init();
    const resumed = await restarted.recordSchedule(second, 'later-source', '2026-09-12T05:30:00Z');
    assert.equal(resumed.first_seen_at, '2026-09-08T05:30:00Z'); assert.equal(resumed.source, 'original-source');
    const manifest = await restarted.manifest();
    assert.equal(manifest.schedules.length, 2); assert.equal(manifest.schedule_bytes, first.length + second.length);
    assert.equal(manifest.schedules.find(s => s.id === descriptor.id)!.first_seen_at, firstSeen);
    assert.deepEqual(await readFile(await restarted.schedulePath(resumed.id)), Buffer.from(second));
  });
});

test('concurrent seal requests serialize and retain a stable capture identity with monotonic cumulative sequences', async () => {
  await fixture(async (directory, journal, exchange) => {
    const original = await exchange.manifest();
    for (let i = 0; i < 4; i++) await put(journal, batch(String(i)));
    const other = new CaptureExchange(directory);
    const sealed = await Promise.all([exchange.seal(journal, 1), other.seal(journal, 1), exchange.seal(journal, 1), other.seal(journal, 1)]);
    assert.deepEqual(sealed.map(b => b!.sequence), [1, 2, 3, 4]);
    const manifest = await new CaptureExchange(directory).manifest();
    assert.equal(manifest.capture_id, original.capture_id); assert.equal(manifest.latest_sequence, 4);
    assert.equal(manifest.bundle_count, 4); assert.equal((await journal.files()).length, 0);
    assert.notEqual(manifest.manifest_sha256, original.manifest_sha256);
    assert.deepEqual(validateCaptureManifest(manifest), manifest);
  });
});

test('sealing bounds aggregate decoded frame memory and leaves overflow frames for the next bundle', async () => {
  await fixture(async (_directory, journal, exchange) => {
    const padding = 'x'.repeat(7 * 1024 * 1024);
    for (let i = 0; i < 10; i++) await put(journal, { ...batch(String(i)), provenance: { padding } });
    const sealed = (await exchange.seal(journal))!;
    assert.equal(sealed.frames, 9); assert.equal((await journal.files()).length, 1);
    const manifest = await exchange.manifest(), bytes = await readFile(await exchange.bundlePath(sealed.id));
    assert.equal(decodeCaptureBundle(bytes, sealed, manifest.capture_id).length, 9);
    assert.equal((await exchange.seal(journal))!.frames, 1);
    assert.equal((await journal.files()).length, 0);
  });
});
