import 'dotenv/config';
import { pathToFileURL } from 'node:url';
import { safeError } from './log-safety';

export type CollectorMode = 'local' | 'cloud';
export function collectorMode(value: string | undefined): CollectorMode {
  if (value === undefined || value === 'local') return 'local';
  if (value === 'cloud') return 'cloud';
  throw new Error('COLLECTOR_MODE must be local or cloud');
}

type CollectorLaunchers = Record<CollectorMode, () => Promise<unknown>>;
export async function launchCollector(value: string | undefined = process.env.COLLECTOR_MODE,
  launchers: CollectorLaunchers = {
    local: () => import('./index'),
    cloud: async () => (await import('./cloud-collector')).runCloudCollector(),
  }): Promise<void> {
  await launchers[collectorMode(value)]();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  void launchCollector().catch(error => {
    console.error('[Collector startup]', safeError(error)); process.exitCode = 1;
  });
}
