import { mkdir, open, rename, readFile, readdir, statfs, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { gzipSync, gunzipSync } from 'node:zlib';
import { randomUUID } from 'node:crypto';
import type { CollectionBatch } from './observation-types';

export const DATA_DIR = process.env.TRANSIT_DATA_DIR || './runtime-data';
export async function atomicFile(path: string, data: string | Uint8Array): Promise<void> {
  const temporary = `${path}.${randomUUID()}.tmp`;
  const file = await open(temporary, 'wx', 0o600);
  try { await file.writeFile(data); await file.sync(); } finally { await file.close(); }
  await rename(temporary, path);
  const directory = await open(join(path, '..'), 'r');
  try { await directory.sync(); } finally { await directory.close(); }
}

export async function diskBudget(directory: string, reserveBytes = 10_000_000_000) {
  const s = await statfs(directory);
  const capacity = s.blocks * s.bsize, available = s.bavail * s.bsize;
  return { capacity_bytes: capacity, available_bytes: available,
    allowed: available > reserveBytes && (capacity - available) / capacity < 0.8 };
}

/** Each acknowledged frame survives process termination. Analysis runs separately. */
export class LocalJournal {
  readonly pending: string;
  private chain: Promise<void> = Promise.resolve();
  constructor(readonly directory: string, private readonly budget?: (bytes: number) => Promise<{ allowed: boolean }>) { this.pending = join(directory, 'incoming'); }
  async init() { await mkdir(this.pending, { recursive: true, mode: 0o700 }); }
  append(batch: CollectionBatch): Promise<void> {
    const operation = this.chain.then(async () => {
      const compressed = gzipSync(JSON.stringify(batch), { level: 6 });
      if (!(await (this.budget ? this.budget(compressed.length) : diskBudget(this.directory))).allowed) throw new Error('Local storage ceiling reached; collection must pause');
      const path = join(this.pending, `${batch.received_at.replace(/[^0-9]/g, '')}-${batch.batch_id}.json.gz`);
      await atomicFile(path, compressed);
    });
    this.chain = operation.catch(() => {});
    return operation;
  }
  async drain() { await this.chain; }
  async files(limit = 500): Promise<string[]> {
    return (await readdir(this.pending)).filter(n => n.endsWith('.json.gz')).sort().slice(0, limit).map(n => join(this.pending, n));
  }
  async read(path: string): Promise<CollectionBatch> {
    const value = JSON.parse(gunzipSync(await readFile(path)).toString('utf8')) as CollectionBatch;
    if (value.schema_version !== 1 || !value.batch_id || !Array.isArray(value.observations)) throw new Error('Invalid journal frame');
    return value;
  }
  async acknowledge(path: string) { await unlink(path); }
}
