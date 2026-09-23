import { readdir, stat, statfs } from 'node:fs/promises';
import { join } from 'node:path';
import { diskBudget } from './local-journal';

const PACKAGING_RESERVE = 64 * 1024 * 1024;
export function captureBudget(directory: string, env: NodeJS.ProcessEnv = process.env) {
  if (!env.CAPTURE_FREE_RESERVE_BYTES && !env.CAPTURE_QUEUE_MAX_BYTES) return async (_bytes = 0) => diskBudget(directory);
  const reserve = Number(env.CAPTURE_FREE_RESERVE_BYTES), maximum = Number(env.CAPTURE_QUEUE_MAX_BYTES);
  if (!Number.isSafeInteger(reserve) || reserve < 1_000_000_000 || !Number.isSafeInteger(maximum) || maximum <= PACKAGING_RESERVE || maximum > reserve / 2) throw new Error('Invalid capture spool budget');
  let queued = 0, scanned = 0;
  let chain = Promise.resolve();
  return (additionalBytes = 0) => {
    const result = chain.then(async () => {
      if (Date.now() - scanned > 10_000) {
        queued = 0;
        for (const relative of ['incoming', 'capture-exchange/bundles']) {
          const path = join(directory, relative);
          const files = await readdir(path, { withFileTypes: true }).catch((error: NodeJS.ErrnoException) => { if (error.code === 'ENOENT') return []; throw error; });
          for (const file of files) if (file.isFile()) {
            queued += await stat(join(path, file.name)).then(s => s.size).catch((error: NodeJS.ErrnoException) => { if (error.code === 'ENOENT') return 0; throw error; });
          }
        }
        scanned = Date.now();
      }
      const s = await statfs(directory);
      const available = s.bavail * s.bsize;
      const allowed = available - additionalBytes > reserve && queued + additionalBytes + PACKAGING_RESERVE <= maximum;
      if (allowed) queued += additionalBytes;
      return { allowed, capacity_bytes: s.blocks * s.bsize, available_bytes: available, queued_bytes: queued, queue_budget_bytes: maximum, reserve_bytes: reserve };
    });
    chain = result.then(() => {}, () => {});
    return result;
  };
}
