import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { CaptureExchange, decodeCaptureBundle } from './capture-exchange';
import { sealCaptureFrontier } from './capture-frontier';
import { LocalJournal } from './local-journal';

async function fixture(run: (journal: LocalJournal, exchange: CaptureExchange) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'capture-frontier-'));
  const journal = new LocalJournal(directory), exchange = new CaptureExchange(directory);
  try { await journal.init(); await exchange.init(); await run(journal, exchange); }
  finally { await rm(directory, { recursive: true, force: true }); }
}
async function append(journal: LocalJournal, id: string) {
  const path = join(journal.pending, id + '.json.gz');
  await writeFile(path, gzipSync(JSON.stringify({ schema_version: 1, batch_id: id, source: 'sse', received_at: '2026-09-12T14:00:00.123456Z', observations: [] })));
  return path;
}

test('a claim seals more than 1000 original frames while an arrival during sealing remains outside its frozen frontier', async () => {
  await fixture(async (journal, exchange) => {
    for (let start = 0; start < 1005; start += 100) {
      await Promise.all(Array.from({ length: Math.min(100, 1005 - start) }, (_, offset) => append(journal, 'initial-' + String(start + offset).padStart(4, '0'))));
    }
    const acknowledge = journal.acknowledge.bind(journal); let late: string | undefined;
    journal.acknowledge = async path => {
      await acknowledge(path);
      // This new name sorts before all initial names, so a live rescan would
      // wrongly include it despite the claim's earlier snapshot.
      if (!late) late = await append(journal, '0000-later-arrival');
    };
    const last = await sealCaptureFrontier(exchange, journal); journal.acknowledge = acknowledge;
    assert.equal(last!.sequence, 2);
    const manifest = await exchange.manifest(); assert.deepEqual(manifest.bundles.map(b => b.frames), [1000, 5]);
    const ids: string[] = [];
    for (const descriptor of manifest.bundles) ids.push(...decodeCaptureBundle(await readFile(await exchange.bundlePath(descriptor.id)), descriptor, manifest.capture_id).map(b => b.batch_id));
    assert.equal(ids.length, 1005); assert.equal(new Set(ids).size, 1005); assert.ok(ids.every(id => id.startsWith('initial-')));
    assert.deepEqual(await journal.files(), [late]);
    assert.equal((await exchange.seal(journal))!.sequence, 3);
    assert.equal((await journal.files()).length, 0);
  });
});

test('a previous seal interrupted after its final unlink recovers before advancing a new claim frontier', async () => {
  await fixture(async (journal, exchange) => {
    await append(journal, 'previous');
    const acknowledge = journal.acknowledge.bind(journal);
    journal.acknowledge = async path => { await acknowledge(path); throw new Error('interrupted after final unlink'); };
    await assert.rejects(exchange.seal(journal), /interrupted/); journal.acknowledge = acknowledge;
    await append(journal, 'new-frontier');
    const last = await sealCaptureFrontier(exchange, journal);
    assert.equal(last!.sequence, 2);
    assert.equal((await journal.files()).length, 0);
    assert.deepEqual((await exchange.manifest()).bundles.map(b => b.frames), [1, 1]);
  });
});
