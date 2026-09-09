import type { SourceQualityData, SourceQualityEntry } from '../dashboard/src/summary-data';
import type { CollectionBatch } from './observation-types';
import type { LePassHealth } from './lepass-collector';

type Source = CollectionBatch['source'];
export type SourceClocks = Record<Source, { received_at: number; provider_at: number }>;
export type PublicLePassHealth = Pick<LePassHealth, 'status' | 'reason'> & { queries?: Pick<LePassHealth['queries'][number], 'lastError'>[] }
  | { status: 'unavailable'; message?: string };
const instant = (value: string | null | undefined) => {
  const at = value ? Date.parse(value) : NaN;
  return Number.isFinite(at) && at > 0 ? at : 0;
};

/** These high-water marks only advance after the corresponding batch is durable.
 * A replay can advance the receipt clock, but never substitutes it for provider time. */
export class PersistedSourceClocks {
  private clocks: SourceClocks = { sse: { received_at: 0, provider_at: 0 }, lepass: { received_at: 0, provider_at: 0 } };
  get values(): SourceClocks { return structuredClone(this.clocks); }
  async persist(batch: CollectionBatch, append: (batch: CollectionBatch) => Promise<void>): Promise<void> {
    await append(batch);
    const clock = this.clocks[batch.source];
    clock.received_at = Math.max(clock.received_at, instant(batch.received_at));
    for (const { observation } of batch.observations) {
      const at = observation.observed_at == null ? NaN : observation.observed_at * 1000;
      if (observation.source === batch.source && Number.isFinite(at) && at > 0 && at <= 8.64e15) clock.provider_at = Math.max(clock.provider_at, at);
    }
  }
}

function lepassIssue(health: PublicLePassHealth): string | undefined {
  if (health.status === 'unavailable' || health.status === 'disabled' || health.status === 'authentication_required') return 'Le Pass collection requires configuration or a valid session. Previously saved observations remain available.';
  if (health.status === 'stopped') return 'Le Pass collection is stopped. Previously saved observations remain available.';
  if (health.status === 'starting') return 'Le Pass collection is starting; a successful collection has not yet been confirmed.';
  if (health.status !== 'degraded') return;
  if (health.reason === 'mapping_revision_changed' || health.queries?.some(query => query.lastError === 'mapping_revision_changed')) return 'A provider revision requires route/direction mappings to be revalidated. Raw responses are retained; affected observations await mapping validation before analysis.';
  if (health.reason === 'durable_sink_unavailable') return 'Le Pass responses cannot currently be saved; collection is retrying.';
  if (health.reason === 'some_queries_failing') return 'One or more configured Le Pass queries are failing. Saved observations remain available; current coverage may be incomplete.';
  return 'Le Pass reports a collection problem. Saved observations remain available; current coverage may be incomplete.';
}

/** Overlay live durable clocks and collection issues without changing snapshot inventory. */
export function mergeSourceQuality(saved: SourceQualityData | undefined, live: {
  clocks: SourceClocks; paused: boolean; lepass: PublicLePassHealth; stale_ms: number; now?: number;
}): SourceQualityData {
  const now = live.now ?? Date.now();
  const entries = [...(saved?.sources ?? [])];
  for (const id of ['sse', 'lepass'] as const) if (!entries.some(source => source.id === id)) entries.push({ id, label: id === 'sse' ? 'RTA relay' : 'LePass', status: 'collecting' });
  const sources = entries.map((source): SourceQualityEntry => {
    if (source.id !== 'sse' && source.id !== 'lepass') return source;
    const clock = live.clocks[source.id];
    const received = Math.max(clock.received_at, instant(source.last_received_at));
    const provider = Math.max(clock.provider_at, instant(source.last_provider_at));
    const recent = received > 0 && now >= received && now - received < live.stale_ms;
    let status: SourceQualityEntry['status'] = recent ? 'ready' : received ? 'degraded' : 'collecting';
    let message: string | undefined;
    if (live.paused) { status = 'degraded'; message = 'Collection is paused. Previously saved observations remain available.'; }
    else if (source.id === 'lepass') {
      message = lepassIssue(live.lepass);
      if (message) status = live.lepass.status === 'starting' ? 'collecting' : 'degraded';
    }
    if (!message && !recent && received) message = 'No recent durable receipt is available. Showing previously saved observations.';
    return { ...source, status, message, last_received_at: received ? new Date(received).toISOString() : null,
      last_provider_at: provider ? new Date(provider).toISOString() : null };
  });
  return { ...saved, sources, status: sources.some(source => source.status === 'degraded' || source.status === 'unavailable') ? 'degraded' : sources.some(source => source.status === 'ready') ? 'ready' : 'collecting' };
}
