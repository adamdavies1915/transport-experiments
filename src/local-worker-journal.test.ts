import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';
import { atomicFile, LocalJournal } from './local-journal';
import { createWorkerJournalInput } from './local-worker-journal';
import { ingestBatch, openLocalStore, query } from './local-store';
import type { CollectionBatch } from './observation-types';

async function fixture(run: (journal: LocalJournal, append: (id: string) => Promise<void>, directory: string) => Promise<void>) {
  const directory = await mkdtemp(join(tmpdir(), 'transit-worker-journal-'));
  const journal = new LocalJournal(directory); await journal.init();
  // Exercise the durable journal format without making tests depend on the
  // production disk reserve or synthetic receipt times relative to wall time.
  const append = async (id: string) => {
    const batch: CollectionBatch = { schema_version: 1, batch_id: id, source: 'sse',
      received_at: '2026-09-08T13:00:00.000Z', observations: [] };
    await atomicFile(join(journal.pending, `${id}.json.gz`), gzipSync(JSON.stringify(batch)));
  };
  try { await run(journal, append, directory); } finally { await rm(directory, { recursive: true, force: true }); }
}

test('backfill consumes every startup file once, including beyond 500, and leaves concurrent arrivals untouched', async () => {
  await fixture(async (journal, append) => {
    for (let i = 0; i < 501; i++) await append(`initial-${String(i).padStart(3, '0')}`);
    const input = await createWorkerJournalInput(journal, true);
    assert.equal(input.snapshot_frames, 501);
    await append('arrived-after-snapshot');
    const ingested: string[] = [];
    const ingest = async (batch: CollectionBatch) => {
      ingested.push(batch.batch_id);
      if (ingested.length === 1) await append('arrived-during-ingestion');
    };
    assert.equal(await input.drain(ingest), true);
    assert.equal(await input.drain(ingest), true, 'later analysis cycles must not expand backfill input');
    assert.equal(ingested.length, 501); assert.equal(new Set(ingested).size, 501);
    assert.deepEqual(await Promise.all((await journal.files()).map(async p => (await journal.read(p)).batch_id)),
      ['arrived-after-snapshot', 'arrived-during-ingestion']);
  });
});

test('failed initial ingestion aborts backfill and retains the failed and all later files for a restart', async () => {
  await fixture(async (journal, append) => {
    for (const id of ['a', 'b', 'c']) await append(id);
    const input = await createWorkerJournalInput(journal, true), seen: string[] = [];
    await assert.rejects(input.drain(async batch => {
      if (batch.batch_id === 'b') throw new Error('test write failure');
      seen.push(batch.batch_id);
    }), /Backfill initial journal snapshot failed.*test write failure/);
    assert.deepEqual(seen, ['a']);
    assert.deepEqual(await Promise.all((await journal.files()).map(async p => (await journal.read(p)).batch_id)), ['b', 'c']);
    const restarted = await createWorkerJournalInput(journal, true);
    await restarted.drain(async batch => { seen.push(batch.batch_id); });
    assert.deepEqual(seen, ['a', 'b', 'c']); assert.equal((await journal.files()).length, 0);
  });
});

test('commit before acknowledgement failure replays idempotently from the retained journal frame', async () => {
  await fixture(async (journal, append, directory) => {
    await append('committed');
    const store = await openLocalStore(directory);
    try {
      const broken = await createWorkerJournalInput({ files: limit => journal.files(limit), read: path => journal.read(path),
        acknowledge: async () => { throw new Error('test acknowledgement failure'); } }, true);
      await assert.rejects(broken.drain(batch => ingestBatch(store.c, batch)), /Backfill initial journal snapshot failed/);
      assert.equal((await journal.files()).length, 1);
      const restarted = await createWorkerJournalInput(journal, true), inserted: boolean[] = [];
      await restarted.drain(async batch => { inserted.push(await ingestBatch(store.c, batch)); });
      assert.deepEqual(inserted, [false]);
      assert.equal((await query<{ n: number }>(store.c, 'SELECT COUNT(*) AS n FROM collection_batches'))[0].n, 1);
      assert.equal((await journal.files()).length, 0);
    } finally { store.c.closeSync(); store.db.closeSync(); }
  });
});

test('continuous mode discovers later arrivals and retries failures, while shutdown never acknowledges unprocessed input', async () => {
  await fixture(async (journal, append) => {
    const input = await createWorkerJournalInput(journal, false);
    assert.equal(input.snapshot_frames, null);
    await append('a');
    const errors: unknown[] = [], seen: string[] = [];
    assert.equal(await input.drain(async () => { throw new Error('retry'); }, { onError: e => errors.push(e) }), false);
    assert.equal(errors.length, 1); assert.equal((await journal.files()).length, 1);
    await input.drain(async b => { seen.push(b.batch_id); });
    await append('b');
    assert.equal(await input.drain(async b => { seen.push(b.batch_id); }, { stopped: () => true }), false);
    assert.equal((await journal.files()).length, 1);
    await input.drain(async b => { seen.push(b.batch_id); });
    assert.deepEqual(seen, ['a', 'b']); assert.equal((await journal.files()).length, 0);
  });
});
