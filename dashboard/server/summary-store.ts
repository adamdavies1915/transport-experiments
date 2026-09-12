import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { SummarySnapshotStatus, TransitSummaryEnvelope } from '../src/summary-data';
import { parseSummary } from '../src/summary-validation';
export { parseSummary } from '../src/summary-validation';

const MAX_BYTES = 64 * 1024 * 1024;
export const DEFAULT_SUMMARY_STALE_MS = 35 * 60000;
const object = (value: unknown): value is Record<string, unknown> => value != null && typeof value === 'object' && !Array.isArray(value);
export function summaryStaleMs(value: string | undefined): number {
  if (value === undefined) return DEFAULT_SUMMARY_STALE_MS;
  const milliseconds = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(milliseconds) || milliseconds <= 0) throw new Error('TRANSIT_SUMMARY_STALE_MS must be a positive integer in milliseconds');
  return milliseconds;
}
export interface SummaryStoreOptions {
  url?: string; token?: string; cacheFile: string;
  refreshMs?: number; timeoutMs?: number; staleMs?: number;
  fetcher?: typeof fetch; now?: () => number;
}
/** A background collector fetch is the only upstream access. Public requests read memory. */
export class SummaryStore {
  private value: TransitSummaryEnvelope | null = null;
  private origin: SummarySnapshotStatus['origin'] = 'none';
  private receivedAt: string | null = null;
  private error: string | null = null;
  private pending: Promise<void> | null = null;
  private timer?: ReturnType<typeof setInterval>;
  constructor(private options: SummaryStoreOptions) {
    if (options.staleMs !== undefined && (!Number.isSafeInteger(options.staleMs) || options.staleMs <= 0)) throw new Error('Summary stale interval must be a positive integer in milliseconds');
  }
  get snapshot() { return this.value; }
  get status(): SummarySnapshotStatus {
    const now = (this.options.now ?? Date.now)();
    return { generated_at: this.value?.generated_at ?? null, received_at: this.receivedAt, origin: this.origin,
      stale: !this.value || now - Date.parse(this.value.generated_at) > (this.options.staleMs ?? DEFAULT_SUMMARY_STALE_MS) || this.error != null,
      refresh_error: this.error,
      ...(this.value?.processing ? { processing: this.value.processing } : {}) };
  }
  async load() {
    try {
      const text = await readFile(this.options.cacheFile, 'utf8');
      if (Buffer.byteLength(text) > MAX_BYTES) throw new Error('Summary too large');
      const saved: unknown = JSON.parse(text);
      const wrapped = object(saved) && object(saved.summary);
      this.value = parseSummary(wrapped ? saved.summary : saved);
      this.receivedAt = wrapped && typeof saved.received_at === 'string' ? saved.received_at : null;
      this.origin = 'disk';
    } catch { this.error = 'No readable saved summary is available yet.'; }
  }
  refresh(): Promise<void> {
    if (this.pending) return this.pending;
    this.pending = this.fetchSummary().finally(() => { this.pending = null; });
    return this.pending;
  }
  private async fetchSummary() {
    if (!this.options.url || !this.options.token) { this.error = 'Collector summary connection is not configured.'; return; }
    try {
      const response = await (this.options.fetcher ?? fetch)(this.options.url, {
        headers: { Authorization: `Bearer ${this.options.token}`, Accept: 'application/json' },
        redirect: 'error', signal: AbortSignal.timeout(this.options.timeoutMs ?? 10000),
      });
      if (!response.ok || !response.body) throw new Error('Collector unavailable');
      const reader = response.body.getReader(); let bytes = 0; const chunks: Uint8Array[] = [];
      try { while (true) { const item = await reader.read(); if (item.done) break; bytes += item.value.byteLength;
        if (bytes > MAX_BYTES) throw new Error('Summary too large'); chunks.push(item.value); }
      } finally { await reader.cancel(); }
      const next = parseSummary(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      if (this.value && Date.parse(next.generated_at) < Date.parse(this.value.generated_at)) throw new Error('Older summary');
      const receivedAt = new Date((this.options.now ?? Date.now)()).toISOString();
      await mkdir(dirname(this.options.cacheFile), { recursive: true, mode: 0o700 });
      const temporary = `${this.options.cacheFile}.tmp`;
      const file = await open(temporary, 'w', 0o600);
      try { await file.writeFile(JSON.stringify({ received_at: receivedAt, summary: next })); await file.sync(); }
      finally { await file.close(); }
      await rename(temporary, this.options.cacheFile);
      const directory = await open(dirname(this.options.cacheFile), 'r');
      try { await directory.sync(); } finally { await directory.close(); }
      this.value = next; this.receivedAt = receivedAt; this.origin = 'collector'; this.error = null;
    } catch { this.error = 'Collector refresh failed; showing the last saved summary when available.'; }
  }
  start() { if (this.timer) return; void this.refresh(); this.timer = setInterval(() => { void this.refresh(); }, this.options.refreshMs ?? 60000); this.timer.unref(); }
  stop() { if (this.timer) clearInterval(this.timer); this.timer = undefined; }
}
