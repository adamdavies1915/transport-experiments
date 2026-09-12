import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { appendFile, lstat, mkdir, mkdtemp, readFile, readdir, rename, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative } from 'node:path';
import { archiveDay } from './cloud-archive';
import { ingestBatch, loadObservations, openLocalStore, query, setState, state } from './local-store';
import { createProcessingSeed, restoreProcessingSeed, sha256File, type ProcessingSeed } from './processing-seed';
import type { CollectionBatch } from './observation-types';

const date = '2026-09-08';
const received = Date.parse(date + 'T12:00:00Z') / 1000;
const batch: CollectionBatch = {
  schema_version: 1, batch_id: 'seed-fixture-batch', source: 'lepass', received_at: new Date(received * 1000).toISOString(),
  observations: [{
    observation: { source: 'lepass', observation_id: 'seed-fixture-observation', vehicle_id: 'lepass:fixture', provider_vehicle_id: 'fixture', route_id: '12', trip_id: 'trip', observed_at: received - 1, received_at: received, lat: 29.95, lon: -90.08, speed_mph: null, off_route: false, location_source: 'provider_gps', timestamp_precision_seconds: 0.001, direction_id: '0', pattern_id: 'pattern', mapping_confidence: 'verified' },
    raw: { fixture: true, lat: 29.95, lon: -90.08 },
  }],
};

async function fixture(run: (paths: { root: string; source: string; seed: string; restored: string }) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), 'transit-processing-seed-'));
  const source = join(root, 'server-history'), seed = join(root, 'seed'), restored = join(root, 'desktop-history');
  try {
    const store = await openLocalStore(source);
    try {
      await ingestBatch(store.c, batch);
      await archiveDay(store.c, source, date);
      // Leave evidence solely in cold storage so relocation must actually work.
      await store.c.run('DELETE FROM collection_receipts; DELETE FROM collection_payloads');
      await setState(store.c, 'seed-fixture-checkpoint', { kept: true });
    } finally { store.c.closeSync(); store.db.closeSync(); }
    await writeFile(join(source, 'private-credential.txt'), 'private-fixture-secret');
    await run({ root, source, seed, restored });
  } finally { await rm(root, { recursive: true, force: true }); }
}

test('checkpointed seeds hash every archive and restore cold evidence with relocated paths and the same baseline', async () => fixture(async ({ source, seed, restored }) => {
  const manifest = await createProcessingSeed(source, seed);
  assert.match(manifest.baseline_id, /^[a-f0-9]{64}$/);
  assert.equal(manifest.source_directory, source);
  assert.deepEqual(JSON.parse(await readFile(join(seed, 'seed.json'), 'utf8')), manifest);
  assert.equal(manifest.files.length, 3, 'database, daily Parquet and archive manifest only');
  assert.ok(manifest.files.some(file => file.path === 'transit.duckdb'));
  assert.ok(manifest.files.some(file => file.path === `archives/${date}/manifest.json`));
  assert.ok(manifest.files.every(file => !/credential|\.wal$|baseline/.test(file.path)));
  for (const file of manifest.files) {
    assert.equal((await lstat(join(seed, file.path))).size, file.bytes);
    assert.equal(await sha256File(join(seed, file.path)), file.sha256);
  }
  const sourceMarker = JSON.parse(await readFile(join(source, 'processing', 'baseline.json'), 'utf8'));
  assert.equal(sourceMarker.baseline_id, manifest.baseline_id);
  assert.deepEqual(await restoreProcessingSeed(seed, restored), manifest);
  assert.equal(JSON.parse(await readFile(join(restored, 'processing', 'baseline.json'), 'utf8')).baseline_id, manifest.baseline_id);
  const originalArchive = JSON.parse(await readFile(join(source, 'archives', date, 'manifest.json'), 'utf8'));
  const restoredArchive = JSON.parse(await readFile(join(restored, 'archives', date, 'manifest.json'), 'utf8'));
  assert.equal(restoredArchive.file, join(restored, relative(source, originalArchive.file)));
  assert.equal(restoredArchive.sha256, originalArchive.sha256);
  assert.equal(await sha256File(restoredArchive.file), originalArchive.sha256);
  const store = await openLocalStore(restored);
  try {
    const catalog = await query<{ path: string; rows: number; sha256: string }>(store.c, 'SELECT path,rows,sha256 FROM local_archive_catalog');
    assert.deepEqual(catalog, [{ path: restoredArchive.file, rows: 1, sha256: originalArchive.sha256 }]);
    assert.equal((await query<{ n: number }>(store.c, 'SELECT count(*) n FROM collection_receipts'))[0].n, 0);
    assert.deepEqual(await state(store.c, 'seed-fixture-checkpoint'), { kept: true });
    const observations = await loadObservations(store.c, date);
    assert.equal(observations.length, 1);
    assert.equal(observations[0].observation_id, batch.observations[0].observation.observation_id);
    assert.equal(observations[0].received_at, received);
    assert.equal(observations[0].speed_mph, null);
  } finally { store.c.closeSync(); store.db.closeSync(); }
  // Relocation changes only the restored copy, never the reusable seed package.
  for (const file of manifest.files) assert.equal(await sha256File(join(seed, file.path)), file.sha256);
}));

test('corrupt seed bytes fail before publication and never overwrite the live database', async () => fixture(async ({ root, source, seed, restored }) => {
  const manifest = await createProcessingSeed(source, seed);
  const liveHash = await sha256File(join(source, 'transit.duckdb'));
  const marker = await readFile(join(source, 'processing', 'baseline.json'), 'utf8');
  const archive = manifest.files.find(file => file.path.endsWith('.parquet'))!;
  await appendFile(join(seed, archive.path), 'corrupt fixture bytes');
  await assert.rejects(restoreProcessingSeed(seed, restored), /checksum mismatch/);
  await assert.rejects(lstat(restored), { code: 'ENOENT' });
  assert.ok(!(await readdir(root)).some(name => name.endsWith('.tmp')), 'failed staging directory is removed');
  await assert.rejects(restoreProcessingSeed(seed, source), /destination must not exist/);
  assert.equal(await sha256File(join(source, 'transit.duckdb')), liveHash);
  assert.equal(await readFile(join(source, 'processing', 'baseline.json'), 'utf8'), marker);
}));

test('seed creation and restoration refuse existing destinations, even empty ones', async () => fixture(async ({ source, seed, restored }) => {
  await createProcessingSeed(source, seed);
  const originalManifest = await readFile(join(seed, 'seed.json'), 'utf8');
  const originalBaseline = await readFile(join(source, 'processing', 'baseline.json'), 'utf8');
  await assert.rejects(createProcessingSeed(source, seed), /destination already exists/);
  await mkdir(restored);
  await assert.rejects(restoreProcessingSeed(seed, restored), /destination must not exist/);
  assert.deepEqual(await readdir(restored), []);
  await assert.rejects(createProcessingSeed(source, join(source, 'nested-seed')), /outside the live data directory/);
  assert.equal(await readFile(join(seed, 'seed.json'), 'utf8'), originalManifest);
  assert.equal(await readFile(join(source, 'processing', 'baseline.json'), 'utf8'), originalBaseline);
}));

test('a checksum-valid manifest cannot traverse outside the seed directory', async () => fixture(async ({ root, source, seed, restored }) => {
  const manifest = await createProcessingSeed(source, seed);
  const outside = join(root, 'outside-fixture.txt');
  await writeFile(outside, 'outside fixture');
  const body = { schema_version: 1 as const, source_directory: manifest.source_directory, files: [...manifest.files, { path: '../outside-fixture.txt', bytes: (await lstat(outside)).size, sha256: await sha256File(outside) }] };
  const changed: ProcessingSeed = { ...body, baseline_id: createHash('sha256').update(JSON.stringify(body)).digest('hex') };
  await writeFile(join(seed, 'seed.json'), JSON.stringify(changed));
  await assert.rejects(restoreProcessingSeed(seed, restored), /Unsafe seed path/);
  assert.equal(await readFile(outside, 'utf8'), 'outside fixture');
  await assert.rejects(lstat(restored), { code: 'ENOENT' });
}));

test('seed creation refuses a symlink at the archive root', async () => fixture(async ({ root, source, seed }) => {
  const outsideArchives = join(root, 'outside-archives');
  await rename(join(source, 'archives'), outsideArchives);
  await symlink(outsideArchives, join(source, 'archives'), 'dir');
  await assert.rejects(createProcessingSeed(source, seed), /symbolic|symlink/i);
  await assert.rejects(lstat(seed), { code: 'ENOENT' });
  assert.ok(!(await readdir(root)).some(name => name.endsWith('.tmp')));
}));

test('seed restore refuses archive paths with a symlink parent even when bytes match', async () => fixture(async ({ root, source, seed, restored }) => {
  await createProcessingSeed(source, seed);
  const outsideArchives = join(root, 'outside-seed-archives');
  await rename(join(seed, 'archives'), outsideArchives);
  await symlink(outsideArchives, join(seed, 'archives'), 'dir');
  await assert.rejects(restoreProcessingSeed(seed, restored), /symbolic|symlink/i);
  await assert.rejects(lstat(restored), { code: 'ENOENT' });
  assert.ok(!(await readdir(root)).some(name => name.endsWith('.tmp')));
}));

test('trusted parent directory aliases remain usable for Mac-style temporary paths', async () => fixture(async ({ root, source, seed, restored }) => {
  // macOS /var and /tmp are normal parent aliases. Internal archive symlinks
  // remain forbidden, but selecting a package through such an alias is valid.
  const alias = join(root, 'parent-alias');
  await symlink(root, alias, 'dir');
  const created = await createProcessingSeed(join(alias, relative(root, source)), seed);
  const restoredSeed = await restoreProcessingSeed(join(alias, relative(root, seed)), restored);
  assert.equal(restoredSeed.baseline_id, created.baseline_id);
  assert.equal(JSON.parse(await readFile(join(restored, 'processing', 'baseline.json'), 'utf8')).baseline_id, created.baseline_id);
}));
