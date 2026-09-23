import 'dotenv/config';
import { readFile, lstat, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { ProcessingClient } from '../src/daily-processing';
import { validateCaptureManifest } from '../src/capture-exchange';
import { captureStorageGuard, initializeCaptureCloud, uploadCapture } from '../src/motherduck-capture';
import { atomicFile } from '../src/local-journal';
import { ident, query, sql } from '../src/local-store';
import { safeError } from '../src/log-safety';

async function main() {
  if (process.env.MOTHERDUCK_CAPTURE_UPLOAD_ENABLED !== 'true' || process.env.MOTHERDUCK_BILLING_MODE !== 'free_no_card')
    throw new Error('Server capture upload requires explicit no-card enablement');
  const directory = process.env.TRANSIT_DATA_DIR;
  if (!directory) throw new Error('Capture directory is required');
  const token = (await readFile(process.env.MOTHER_DUCK_API_KEY_FILE!, 'utf8')).trim();
  const processingToken = (await readFile(process.env.TRANSIT_PROCESSING_TOKEN_FILE!, 'utf8')).trim();
  const client = new ProcessingClient({ serverUrl: 'http://127.0.0.1:3100', token: processingToken });
  // The capture process serializes manifest publication with sealing. This
  // uploader never creates a second CaptureExchange writer against its files.
  const manifest = validateCaptureManifest(await client.json('/manifest'));
  const database = process.env.MOTHERDUCK_DATABASE || 'my_db';
  const db = await DuckDBInstance.create(`md:${database}?motherduck_token=${encodeURIComponent(token)}`, { threads:'1', memory_limit:'256MB' });
  const c = await db.connect();
  try {
    const activeBytes = await captureStorageGuard(c);
    const budget = Math.min(64*1024*1024,Math.floor((8_000_000_000-activeBytes)/4));
    await initializeCaptureCloud(c,database);
    const pathFor = (kind: string,id: string) => join(directory,'capture-exchange',kind === 'bundle'?'bundles':'schedules',id+(kind === 'bundle'?'.json.gz':'.zip'));
    const result = await uploadCapture(c,database,manifest,(kind,d) => readFile(pathFor(kind,d.id)),budget);
    let releasedBytes = 0;
    // Only sealed bundles older than the snapshot's last bundle are releasable:
    // the latest can still be referenced by capture's crash-recovery seal intent.
    // Keep sidecar descriptors so capture sequences never reset or develop gaps.
    for (const d of result.manifest?.bundles ?? []) {
      if (d.sequence >= manifest.latest_sequence) continue;
      const path = pathFor('bundle',d.id);
      const info = await lstat(path).catch((e:NodeJS.ErrnoException) => { if(e.code==='ENOENT')return null;throw e; });
      if (!info) continue;
      if (!info.isFile() || info.isSymbolicLink()) throw new Error('Unexpected capture spool file');
      const [verified] = await query<{sha:string;bytes:number}>(c,`SELECT sha256(payload) AS sha,octet_length(payload) AS bytes
        FROM ${ident(database)}.main.transit_capture_assets WHERE capture_id=${sql(manifest.capture_id)} AND kind='bundle' AND id=${sql(d.id)}`);
      if (verified?.sha !== d.sha256 || verified.bytes !== d.bytes) throw new Error('Cannot release unverified server spool');
      await unlink(path); releasedBytes += info.size;
    }
    const health = { status:'ready', checked_at:new Date().toISOString(), capture_id:manifest.capture_id,
      published_sequence:result.manifest?.latest_sequence ?? 0, captured_sequence:manifest.latest_sequence,
      uploaded_bytes:result.uploadedBytes, released_server_bytes:releasedBytes, active_storage_bytes:activeBytes };
    await atomicFile(join(directory,'motherduck-capture-health.json'),JSON.stringify(health));
    console.log(JSON.stringify(health));
  } finally { c.closeSync();db.closeSync(); }
}
main().catch(async error => {
  const health = {status:'error',checked_at:new Date().toISOString(),reason:safeError(error)};
  if (process.env.TRANSIT_DATA_DIR) await atomicFile(join(process.env.TRANSIT_DATA_DIR,'motherduck-capture-health.json'),JSON.stringify(health)).catch(()=>{});
  console.error(JSON.stringify(health)); process.exitCode=1;
});
