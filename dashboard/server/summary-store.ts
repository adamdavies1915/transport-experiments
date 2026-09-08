import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { SummarySnapshotStatus, TransitSummaryEnvelope } from '../src/summary-data';

const MAX_BYTES = 64 * 1024 * 1024;
const object = (value: unknown): value is Record<string, unknown> => value != null && typeof value === 'object' && !Array.isArray(value);
/** Accept only the public envelope; tokens and collector configuration never belong here. */
export function parseSummary(value: unknown): TransitSummaryEnvelope {
  if (!object(value) || value.schema_version !== 1 || typeof value.generated_at !== 'string' || !Number.isFinite(Date.parse(value.generated_at))) throw new Error('Invalid summary envelope');
  for (const key of ['source_quality', 'row_study', 'signal_study', 'legacy']) if (value[key] != null && !object(value[key])) throw new Error('Invalid summary section');
  for (const key of ['row_study', 'signal_study']) {
    const study = value[key];
    if (object(study) && (!Array.isArray(study.cells) || !object(study.network) || !Array.isArray(study.network.paths) || !Array.isArray(study.network.sites) || !Array.isArray(study.network.row_sections) || !Array.isArray(study.network.limitations) || !Array.isArray(study.limitations) || !Array.isArray(study.quality))) throw new Error('Invalid study section');
    if (object(study) && study.coverage_cells != null && !Array.isArray(study.coverage_cells)) throw new Error('Invalid study coverage');
  }
  if (object(value.source_quality) && !Array.isArray(value.source_quality.sources)) throw new Error('Invalid source quality');
  return { schema_version: 1, generated_at: value.generated_at,
    source_quality: value.source_quality as TransitSummaryEnvelope['source_quality'],
    row_study: value.row_study as TransitSummaryEnvelope['row_study'],
    signal_study: value.signal_study as TransitSummaryEnvelope['signal_study'],
    legacy: value.legacy as TransitSummaryEnvelope['legacy'] };
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
  constructor(private options: SummaryStoreOptions) {}
  get snapshot() { return this.value; }
  get status(): SummarySnapshotStatus {
    const now = (this.options.now ?? Date.now)();
    return { generated_at: this.value?.generated_at ?? null, received_at: this.receivedAt, origin: this.origin,
      stale: !this.value || now - Date.parse(this.value.generated_at) > (this.options.staleMs ?? 35 * 60000) || this.error != null,
      refresh_error: this.error };
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
