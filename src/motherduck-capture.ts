import { createHash } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { readFile, stat, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { blobValue, DuckDBBlobValue, type DuckDBConnection } from '@duckdb/node-api';
import { ident, query, sql } from './local-store';
import { atomicFile } from './local-journal';
import { captureManifestDigest, validateCaptureManifest, type CaptureManifest, type CaptureBundleDescriptor, type CaptureScheduleDescriptor } from './capture-exchange';
import { assertCumulativeManifest } from './processing-coordinator';
import { STORAGE_BUDGET, storageAccountingFresh } from './cloud-archive';

type Descriptor = CaptureBundleDescriptor | CaptureScheduleDescriptor;
type Kind = 'bundle' | 'schedule';
const hash = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex');
const table = (database: string, name: string) => `${ident(database)}.main.${ident(name)}`;

export async function captureStorageGuard(c: DuckDBConnection): Promise<number> {
  const [row] = await query<{ bytes: number; measured: number }>(c,
    'SELECT SUM(active_bytes) AS bytes,MIN(epoch(computed_ts)) AS measured FROM md_information_schema.storage_info');
  if (!row || !Number.isFinite(row.bytes) || row.bytes < 0 || !Number.isFinite(row.measured) ||
      !storageAccountingFresh(new Date(row.measured * 1000).toISOString())) throw new Error('Live storage accounting unavailable or stale');
  if (row.bytes >= STORAGE_BUDGET) throw new Error('MotherDuck live-data ceiling reached');
  return row.bytes;
}

export async function initializeCaptureCloud(c: DuckDBConnection, database: string) {
  await c.run(`CREATE TABLE IF NOT EXISTS ${table(database,'transit_capture_assets')} (
    capture_id VARCHAR,kind VARCHAR,id VARCHAR,sha256 VARCHAR,bytes BIGINT,descriptor JSON,payload BLOB,
    PRIMARY KEY(capture_id,kind,id))`);
  await c.run(`CREATE TABLE IF NOT EXISTS ${table(database,'transit_capture_manifests')} (
    capture_id VARCHAR PRIMARY KEY,manifest JSON,updated_at TIMESTAMPTZ)`);
}

export async function cloudCaptureManifest(c: DuckDBConnection, database: string, captureId: string): Promise<CaptureManifest | null> {
  const [row] = await query<{ manifest: string }>(c, `SELECT manifest::VARCHAR AS manifest FROM ${table(database,'transit_capture_manifests')} WHERE capture_id=${sql(captureId)}`);
  return row ? validateCaptureManifest(JSON.parse(row.manifest)) : null;
}

/** Publishes only a contiguous prefix whose original bytes are committed and
 * verified remotely. Interrupted attempts leave immutable assets safe to retry. */
export async function uploadCapture(c: DuckDBConnection, database: string, input: CaptureManifest,
  readAsset: (kind: Kind, descriptor: Descriptor) => Promise<Uint8Array>, maximumBytes = 32 * 1024 * 1024) {
  const manifest = validateCaptureManifest(input);
  const prior = await cloudCaptureManifest(c,database,manifest.capture_id);
  if (prior) assertCumulativeManifest(prior,manifest);
  const rows = await query<{ kind: string; id: string; sha256: string; bytes: number; descriptor: string }>(c,
    `SELECT kind,id,sha256,bytes,descriptor::VARCHAR AS descriptor FROM ${table(database,'transit_capture_assets')} WHERE capture_id=${sql(manifest.capture_id)}`);
  const existing = new Map(rows.map(row => [`${row.kind}:${row.id}`,row]));
  let uploadedBytes = 0, uploadedAssets = 0;
  async function ensure(kind: Kind, descriptor: Descriptor): Promise<boolean> {
    const saved = existing.get(`${kind}:${descriptor.id}`);
    if (saved) {
      if (saved.sha256 !== descriptor.sha256 || saved.bytes !== descriptor.bytes || !isDeepStrictEqual(JSON.parse(saved.descriptor),descriptor))
        throw new Error('Existing cloud asset conflicts with capture evidence');
      return true;
    }
    if (uploadedBytes + descriptor.bytes > maximumBytes) return false;
    const bytes = await readAsset(kind,descriptor);
    if (bytes.length !== descriptor.bytes || hash(bytes) !== descriptor.sha256) throw new Error('Capture source checksum mismatch');
    await c.run(`INSERT INTO ${table(database,'transit_capture_assets')} VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`,
      [manifest.capture_id,kind,descriptor.id,descriptor.sha256,descriptor.bytes,JSON.stringify(descriptor),blobValue(bytes)]);
    const [verified] = await query<{ sha: string; bytes: number; descriptor: string }>(c,
      `SELECT sha256(payload) AS sha,octet_length(payload) AS bytes,descriptor::VARCHAR AS descriptor FROM ${table(database,'transit_capture_assets')}
       WHERE capture_id=${sql(manifest.capture_id)} AND kind=${sql(kind)} AND id=${sql(descriptor.id)}`);
    if (!verified || verified.sha !== descriptor.sha256 || verified.bytes !== descriptor.bytes || !isDeepStrictEqual(JSON.parse(verified.descriptor),descriptor))
      throw new Error('MotherDuck capture readback mismatch');
    uploadedBytes += bytes.length; uploadedAssets++;
    return true;
  }
  for (const descriptor of manifest.schedules) if (!await ensure('schedule',descriptor))
    return { manifest: prior, uploadedBytes, uploadedAssets };
  const bundles: CaptureBundleDescriptor[] = [];
  for (const descriptor of manifest.bundles) {
    if (!await ensure('bundle',descriptor)) break;
    bundles.push(descriptor);
  }
  const { manifest_sha256: _old, ...body } = manifest;
  const prefix = { ...body,bundles,latest_sequence:bundles.length,bundle_count:bundles.length,
    bundle_bytes:bundles.reduce((sum,b) => sum+b.bytes,0) };
  const published = validateCaptureManifest({ ...prefix,manifest_sha256:captureManifestDigest(prefix) });
  if (prior) assertCumulativeManifest(prior,published);
  if (published.manifest_sha256 !== prior?.manifest_sha256) {
    await c.run(`INSERT INTO ${table(database,'transit_capture_manifests')} VALUES (?,?::JSON,now())
      ON CONFLICT(capture_id) DO UPDATE SET manifest=excluded.manifest,updated_at=excluded.updated_at`,
      [manifest.capture_id,JSON.stringify(published)]);
    const readback = await cloudCaptureManifest(c,database,manifest.capture_id);
    if (readback?.manifest_sha256 !== published.manifest_sha256) throw new Error('Cloud manifest readback mismatch');
  }
  return { manifest:published,uploadedBytes,uploadedAssets };
}

export class MotherDuckCaptureReader {
  constructor(private c: DuckDBConnection, private database: string, private captureId: string) {}
  async download(path: string, file: string, expected: { bytes: number; sha256: string }, maximum: number, signal?: AbortSignal) {
    signal?.throwIfAborted();
    if (!Number.isSafeInteger(expected.bytes) || expected.bytes < 1 || expected.bytes > maximum) throw new Error('Invalid cloud capture size');
    try {
      const info = await stat(file);
      if (info.size !== expected.bytes || hash(await readFile(file)) !== expected.sha256) throw new Error('Retained local capture is corrupt');
      return;
    } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
    const match = /^\/(bundles|schedules)\/([a-z0-9]+)$/.exec(path);
    if (!match) throw new Error('Invalid cloud asset path');
    const kind = match[1] === 'bundles' ? 'bundle' : 'schedule';
    const result = await this.c.runAndReadAll(`SELECT payload FROM ${table(this.database,'transit_capture_assets')}
      WHERE capture_id=${sql(this.captureId)} AND kind=${sql(kind)} AND id=${sql(match[2])} AND octet_length(payload)<=${maximum}`);
    const payload = result.getRowObjects()[0]?.payload;
    if (!(payload instanceof DuckDBBlobValue) || payload.bytes.length !== expected.bytes || hash(payload.bytes) !== expected.sha256)
      throw new Error('MotherDuck capture download checksum mismatch');
    signal?.throwIfAborted();
    await mkdir(dirname(file),{recursive:true,mode:0o700});
    await atomicFile(file,payload.bytes);
  }
}
