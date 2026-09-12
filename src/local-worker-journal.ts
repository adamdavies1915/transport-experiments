import type { LocalJournal } from './local-journal';
import type { CollectionBatch } from './observation-types';
import { safeError } from './log-safety';

type Journal = Pick<LocalJournal, 'files' | 'read' | 'acknowledge'>;
interface DrainOptions { stopped?: () => boolean; onError?: (error: unknown) => void }

/** A one-shot backfill uses only the files visible at creation. New arrivals stay
 * durable for the next worker, regardless of their claimed receipt timestamps.
 * Normal operation keeps the journal's existing per-cycle listing limit. */
export async function createWorkerJournalInput(journal: Journal, backfill: boolean) {
  const snapshot = backfill ? await journal.files(Number.MAX_SAFE_INTEGER) : null;
  let nextSnapshotFile = 0;
  return {
    snapshot_frames: snapshot?.length ?? null,
    async drain(ingest: (batch: CollectionBatch) => Promise<unknown>, options: DrainOptions = {}): Promise<boolean> {
      const files = snapshot ?? await journal.files();
      for (let i = snapshot ? nextSnapshotFile : 0; i < files.length; i++) {
        if (options.stopped?.()) return false;
        try {
          const batch = await journal.read(files[i]);
          await ingest(batch);
          await journal.acknowledge(files[i]);
          if (snapshot) nextSnapshotFile = i + 1;
        } catch (error) {
          if (snapshot) throw new Error(`Backfill initial journal snapshot failed; unacknowledged frames remain for retry: ${safeError(error)}`, { cause: error });
          options.onError?.(error);
          return false;
        }
      }
      return true;
    },
  };
}
