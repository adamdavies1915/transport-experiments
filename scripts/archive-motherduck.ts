import 'dotenv/config';
import { readFile, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { localProcessingLock } from '../src/daily-processing';
import { openLocalStore } from '../src/local-store';
import { cloudUpload } from '../src/cloud-archive';
import { atomicFile } from '../src/local-journal';
import { safeError } from '../src/log-safety';

async function main() {
  const directory = process.env.TRANSIT_DATA_DIR;
  if (!directory) throw new Error('TRANSIT_DATA_DIR is required');
  await stat(join(directory, 'transit.duckdb'));
  if (process.env.MOTHER_DUCK_API_KEY_FILE)
    process.env.MOTHER_DUCK_API_KEY = (await readFile(process.env.MOTHER_DUCK_API_KEY_FILE, 'utf8')).trim();
  const unlock = await localProcessingLock(directory);
  try {
    const store = await openLocalStore(directory);
    try {
      // One bounded cycle; the existing uploader independently checks fresh
      // billing evidence, storage accounting and exact remote/local matches.
      const health = await cloudUpload(store.c, directory);
      await atomicFile(join(directory, 'processing/motherduck-archive-health.json'), JSON.stringify(health));
      console.log(JSON.stringify(health));
      if (health.status !== 'ready') process.exitCode = 2;
    } finally { store.c.closeSync(); store.db.closeSync(); }
  } finally { await unlock(); }
}
main().catch(error => { console.error(safeError(error)); process.exitCode = 1; });
