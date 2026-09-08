import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { setTimeout as delay } from 'node:timers/promises';
import type { CollectionBatch, CollectedObservation } from './observation-types.js';
import { LePassAuth, LePassError, fetchLePass, type LePassAuthOptions, type LePassTransport } from './lepass-auth.js';
import { encodeThrift, field, numeric, stringId, struct, T, type ThriftStruct, type ThriftValue } from './lepass-thrift.js';

export interface LePassQuery {
  id: string;
  stopId: number;
  lineId?: number;
  patternId?: number;
  patterns?: Array<{ patternId: number; directionId: string | null; mappingConfidence: 'verified' | 'candidate' | 'unknown' }>;
  routeId: string | null;
  directionId: string | null;
  gtfsStopId?: string;
  mappingConfidence: 'verified' | 'candidate' | 'unknown';
  mappingEvidence?: string;
  metroRevision?: string;
}
export const INITIAL_LEPASS_QUERIES: LePassQuery[] = [{ id: 'rta-12-poydras-outbound', stopId: 4539336, lineId: 8697788, patternId: 17096284,
  routeId: '12', directionId: '0', gtfsStopId: '1067', mappingConfidence: 'verified', mappingEvidence: 'LIVE_FEED_COMPARISON.md; 2026-09-07 stop board and vehicle coordinate match' }];

export interface LePassQueryHealth {
  id: string; routeId: string | null; mappingConfidence: LePassQuery['mappingConfidence'];
  lastAttempt: string | null; lastSuccess: string | null; nextAttempt: string | null;
  responses: number; observations: number; providerGpsObservations: number; predictions: number; responsesWithoutLocations: number;
  consecutiveFailures: number; lastError: string | null; advertisedIntervalSeconds: number | null;
  lastResponseIntervalSeconds: number | null; lastProviderSampleAt: string | null;
}
export interface LePassHealth {
  source: 'lepass'; status: 'disabled' | 'starting' | 'collecting' | 'degraded' | 'authentication_required' | 'stopped';
  reason: string | null; updatedAt: string; queries: LePassQueryHealth[]; configuredRoutes: string[];
  coverageNote: string;
}
export interface LePassCollectorOptions extends LePassAuthOptions {
  queries: LePassQuery[];
  onBatch: (batch: CollectionBatch) => Promise<void>;
  onHealth?: (health: LePassHealth) => void | Promise<void>;
}

export function arrivalRequest(query: LePassQuery): { path: 'V4/StopsArrivals' | 'V4/LineArrivals'; body: Buffer } {
  return arrivalBatchRequest([query]);
}

export function arrivalBatchRequest(queries: LePassQuery[]): { path: 'V4/StopsArrivals' | 'V4/LineArrivals'; body: Buffer } {
  if (!queries.length || queries.length > 8 || queries.some(q => (q.lineId === undefined) !== (queries[0].lineId === undefined))) throw new LePassError('invalid_query_batch');
  const conf = [field(2, T.BOOL, true), field(3, T.BOOL, false), field(4, T.BOOL, true), field(5, T.BOOL, false), field(6, T.BOOL, false)];
  const ids = queries[0].lineId === undefined ? field(1, T.LIST, queries.map(q => q.stopId), T.I32) :
    field(1, T.LIST, queries.map(q => [field(1, T.I32, q.stopId), field(2, T.I32, q.lineId!)]) as unknown as ThriftValue[], T.STRUCT);
  return { path: queries[0].lineId === undefined ? 'V4/StopsArrivals' : 'V4/LineArrivals', body: encodeThrift([ids, field(2, T.STRUCT, conf)]) };
}

const millis = (value: ThriftValue | undefined): number | null => {
  const n = numeric(value); return n !== null && n >= 946684800000 && n <= 4102444800000 ? n : null;
};
function safeRaw(value: unknown): unknown { return JSON.parse(JSON.stringify(value, (_key, v) => typeof v === 'bigint' ? v.toString() : v)); }

/** No provider composite ID is automatically asserted to be the RTA physical vehicle ID. */
export function parseLePassArrivals(data: ThriftStruct, query: LePassQuery, receivedAtMs: number): CollectionBatch {
  if (numeric(data[1]) !== query.stopId) throw new LePassError('response_stop_mismatch');
  const incoming = Array.isArray(data[3]) ? data[3] : struct(data[3]) ? [data[3]] : null;
  if (!incoming) throw new LePassError('invalid_arrivals_response');
  const observations: CollectedObservation[] = [], predictions: Record<string, unknown>[] = [];
  const batchId = randomUUID(); let index = 0;
  for (const candidate of incoming) {
    const line = struct(candidate), lineId = numeric(line?.[1]);
    if (!line || !lineId || !Array.isArray(line[2])) throw new LePassError('invalid_line_arrivals');
    if (query.lineId !== undefined && lineId !== query.lineId) throw new LePassError('response_line_mismatch');
    for (const item of line[2]) {
      const arrival = struct(item); if (!arrival) throw new LePassError('invalid_arrival');
      const pattern = numeric(arrival[1]);
      const patternMapping = query.patterns?.find(p => p.patternId === pattern);
      const matched = query.lineId === lineId && (query.patterns ? !!patternMapping : query.patternId === undefined || query.patternId === pattern);
      const routeId = matched ? query.routeId : null;
      const directionId = matched ? patternMapping?.directionId ?? (query.patternId !== undefined ? query.directionId : null) : null;
      const confidence = matched ? patternMapping?.mappingConfidence ?? query.mappingConfidence : 'unknown';
      const location = struct(arrival[11]), coords = struct(location?.[1]);
      const providerVehicleId = stringId(location?.[3]), tripId = stringId(arrival[2]);
      const common = { source: 'lepass', query_id: query.id, received_at: receivedAtMs / 1000, provider_stop_id: query.stopId,
        provider_line_id: lineId, provider_pattern_id: pattern, provider_trip_id: tripId, route_id: routeId,
        direction_id: directionId, mapping_confidence: confidence };
      predictions.push({ ...common, prediction_id: `lepass:${batchId}:${index}`, provider_vehicle_id: providerVehicleId,
        static_departure_at: millis(arrival[3]) === null ? null : millis(arrival[3])! / 1000,
        realtime_departure_at: millis(arrival[4]) === null ? null : millis(arrival[4])! / 1000,
        statistical_departure_at: millis(arrival[17]) === null ? null : millis(arrival[17])! / 1000,
        status: numeric(arrival[5]), certainty: numeric(arrival[18]), raw: safeRaw(arrival) });
      if (location) {
        const lat = numeric(coords?.[1]), lon = numeric(coords?.[2]);
        const sample = millis(location[4]), source = numeric(location[6]);
        const observation = {
          source: 'lepass' as const, observation_id: `lepass:${batchId}:${index}`, vehicle_id: providerVehicleId ? `lepass:${providerVehicleId}` : null,
          provider_vehicle_id: providerVehicleId, route_id: routeId, trip_id: tripId ? `lepass:${tripId}` : null,
          observed_at: sample === null ? null : sample / 1000, received_at: receivedAtMs / 1000,
          lat: lat !== null && Math.abs(lat) <= 90_000_000 ? lat / 1e6 : null,
          lon: lon !== null && Math.abs(lon) <= 180_000_000 ? lon / 1e6 : null,
          speed_mph: null, off_route: numeric(location[5]) === 2,
          location_source: source === 1 ? 'provider_gps' as const : source !== null && [2, 3, 4].includes(source) ? 'estimated' as const : 'unknown' as const,
          timestamp_precision_seconds: .001, direction_id: directionId,
          pattern_id: pattern === null ? null : `lepass:${pattern}`, mapping_confidence: confidence,
        };
        observations.push({ observation, raw: { provider_stop_id: query.stopId, provider_line_id: lineId,
          provider_pattern_id: pattern, provider_trip_id: tripId, vehicle_location: safeRaw(location),
          location_source_code: source, vehicle_status_code: numeric(location[5]),
          timestamp_semantics: 'provider-reported millisecond sample; physical GPS cadence unverified' } });
      }
      index++;
    }
  }
  return { schema_version: 1, batch_id: `lepass:${batchId}`, source: 'lepass', received_at: new Date(receivedAtMs).toISOString(), observations, predictions,
    provenance: { query, provider_epoch_day: numeric(data[2]), next_poll_seconds: numeric(data[5]), response_fingerprint: createHash('sha256').update(JSON.stringify(safeRaw(data))).digest('hex') } };
}

export function pollDelayMs(advertisedSeconds: number | null, failures: number, retryAfterMs = 0): number {
  return Math.max(20_000, (advertisedSeconds ?? 20) * 1000, failures ? Math.min(15 * 60_000, 20_000 * 2 ** Math.min(6, failures - 1)) : 0, retryAfterMs);
}
function validateQueries(queries: LePassQuery[]): void {
  if (!queries.length || queries.length > 500 || new Set(queries.map(q => q.id)).size !== queries.length) throw new LePassError('invalid_query_configuration');
  for (const q of queries) {
    if (!q.id || !Number.isSafeInteger(q.stopId) || q.stopId <= 0 || (q.lineId !== undefined && (!Number.isSafeInteger(q.lineId) || q.lineId <= 0)) ||
      !['verified', 'candidate', 'unknown'].includes(q.mappingConfidence) || (q.mappingConfidence === 'verified' && !q.mappingEvidence)) throw new LePassError('invalid_query_configuration');
  }
}

export function startLePassCollector(options: LePassCollectorOptions): { stop(): void; health(): LePassHealth; done: Promise<void> } {
  validateQueries(options.queries);
  const controller = new AbortController(); const externalAbort = () => controller.abort();
  options.signal?.addEventListener('abort', externalAbort, { once: true }); if (options.signal?.aborted) controller.abort();
  const now = options.now ?? Date.now;
  const monotonic = () => performance.now();
  let lastRequest = -Infinity;
  const original = options.transport ?? fetchLePass;
  const transport: LePassTransport = async (...args) => {
    const remaining = 1000 - (monotonic() - lastRequest);
    if (remaining > 0) await delay(remaining, undefined, { signal: controller.signal });
    lastRequest = monotonic(); return original(...args);
  };
  const auth = new LePassAuth({ ...options, signal: controller.signal, transport });
  const state: LePassHealth = { source: 'lepass', status: 'starting', reason: null, updatedAt: new Date(now()).toISOString(),
    configuredRoutes: [...new Set(options.queries.flatMap(q => q.routeId ? [q.routeId] : []))],
    coverageNote: 'Only configured, mapped stop/line queries are collected. Missing vehicle locations do not mean no service; the two sources may share upstream observations.',
    queries: options.queries.map(q => ({ id: q.id, routeId: q.routeId, mappingConfidence: q.mappingConfidence,
      lastAttempt: null, lastSuccess: null, nextAttempt: null, responses: 0, observations: 0, providerGpsObservations: 0,
      predictions: 0, responsesWithoutLocations: 0, consecutiveFailures: 0, lastError: null, advertisedIntervalSeconds: null,
      lastResponseIntervalSeconds: null, lastProviderSampleAt: null })) };
  const due = options.queries.map(() => 0);
  const lastResponseMonotonic: Array<number | null> = options.queries.map(() => null);
  const health = () => structuredClone(state);
  async function report(): Promise<void> { state.updatedAt = new Date(now()).toISOString(); await options.onHealth?.(health()); }
  const done = (async () => {
    await report();
    while (!controller.signal.aborted) {
      const i = due.indexOf(Math.min(...due));
      if (due[i] > monotonic()) await delay(due[i] - monotonic(), undefined, { signal: controller.signal });
      const indexes = due.map((value, index) => ({ value, index })).filter(x => x.value <= monotonic() &&
        (options.queries[x.index].lineId === undefined) === (options.queries[i].lineId === undefined)).sort((a, b) => a.value - b.value).slice(0, 8).map(x => x.index);
      if (!indexes.length) continue;
      for (const index of indexes) state.queries[index].lastAttempt = new Date(now()).toISOString();
      try {
        const request = arrivalBatchRequest(indexes.map(index => options.queries[index])), response = await auth.requestMany(request.path, request.body);
        const receivedMonotonic = monotonic();
        for (const index of indexes) {
        const q = options.queries[index], h = state.queries[index];
        const data = response.data.find(d => numeric(d[1]) === q.stopId && (q.lineId === undefined || numeric(struct(d[3])?.[1]) === q.lineId));
        if (!data) {
          h.consecutiveFailures++; h.lastError = 'query_missing_from_response';
          due[index] = monotonic() + pollDelayMs(h.advertisedIntervalSeconds, h.consecutiveFailures);
          h.nextAttempt = new Date(now() + due[index] - monotonic()).toISOString(); continue;
        }
        const revisionChanged = q.metroRevision && q.metroRevision !== auth.currentMetroRevision;
        const mappedQuery = revisionChanged ? { ...q, mappingConfidence: 'candidate' as const, patterns: q.patterns?.map(p => ({ ...p, mappingConfidence: 'candidate' as const })) } : q;
        h.mappingConfidence = mappedQuery.mappingConfidence;
        const batch = parseLePassArrivals(data, mappedQuery, response.receivedAt);
        batch.provenance = { ...batch.provenance, current_metro_revision: auth.currentMetroRevision, query_batch_size: indexes.length, mapping_revision_changed: !!revisionChanged };
        // A durable sink failure must halt this source rather than discard the fetched batch.
        for (;;) {
          try { await options.onBatch(batch); break; }
          catch { state.status = 'degraded'; state.reason = 'durable_sink_unavailable'; await report(); await delay(20_000, undefined, { signal: controller.signal }); }
        }
        h.lastSuccess = new Date(response.receivedAt).toISOString(); h.responses++; h.observations += batch.observations.length;
        h.lastResponseIntervalSeconds = lastResponseMonotonic[index] === null ? null : (receivedMonotonic - lastResponseMonotonic[index]!) / 1000;
        lastResponseMonotonic[index] = receivedMonotonic;
        const samples = batch.observations.flatMap(o => o.observation.observed_at === null ? [] : [o.observation.observed_at]);
        if (samples.length) h.lastProviderSampleAt = new Date(Math.max(...samples) * 1000).toISOString();
        h.providerGpsObservations += batch.observations.filter(o => o.observation.location_source === 'provider_gps').length;
        h.predictions += batch.predictions?.length ?? 0; if (!batch.observations.length) h.responsesWithoutLocations++;
        h.consecutiveFailures = 0; h.lastError = revisionChanged ? 'mapping_revision_changed' : null; h.advertisedIntervalSeconds = numeric(data[5]);
        due[index] = monotonic() + pollDelayMs(h.advertisedIntervalSeconds, 0);
        h.nextAttempt = new Date(now() + due[index] - monotonic()).toISOString();
        }
        const queryFailure = state.queries.some(queryHealth => queryHealth.consecutiveFailures > 0);
        const staleMapping = state.queries.some(queryHealth => queryHealth.lastError === 'mapping_revision_changed');
        state.status = queryFailure || staleMapping ? 'degraded' : 'collecting';
        state.reason = queryFailure ? 'some_queries_failing' : staleMapping ? 'mapping_revision_changed' : null;
      } catch (error) {
        if (controller.signal.aborted) break;
        for (const index of indexes) {
        const h = state.queries[index];
        h.consecutiveFailures++; h.lastError = error instanceof LePassError ? error.code : 'protocol_error';
        const authFailed = error instanceof LePassError && (error.status === 401 || error.status === 403 || /credential|guest_bootstrap|refresh_token/.test(error.code));
        state.status = authFailed ? 'authentication_required' : 'degraded'; state.reason = h.lastError;
        due[index] = monotonic() + (authFailed ? 15 * 60_000 : pollDelayMs(h.advertisedIntervalSeconds, h.consecutiveFailures, error instanceof LePassError ? error.retryAfterMs : 0));
        if (authFailed) for (let j = 0; j < due.length; j++) due[j] = Math.max(due[j], due[index]);
        h.nextAttempt = new Date(now() + due[index] - monotonic()).toISOString();
        }
      }
      await report();
    }
  })().catch(async () => { if (!controller.signal.aborted) { state.status = 'degraded'; state.reason = 'collector_failure'; await report().catch(() => {}); } }).finally(async () => {
    if (controller.signal.aborted) { state.status = 'stopped'; state.reason = null; await report().catch(() => {}); }
    options.signal?.removeEventListener('abort', externalAbort);
  });
  return { stop: () => controller.abort(), health, done };
}

export async function startLePassFromEnvironment(options: Pick<LePassCollectorOptions, 'stateDir' | 'onBatch' | 'onHealth'>): Promise<{ stop(): void }> {
  const apiKey = process.env.LEPASS_API_KEY, encryptionKey = process.env.LEPASS_ENCRYPTION_KEY;
  const reason = process.env.LEPASS_ENABLED === 'false' ? 'disabled_by_configuration' : !apiKey || !encryptionKey ? 'credentials_not_configured' : null;
  if (reason) {
    await options.onHealth?.({ source: 'lepass', status: 'disabled', reason, updatedAt: new Date().toISOString(), queries: [], configuredRoutes: [], coverageNote: 'No Le Pass observations are being collected.' });
    return { stop() {} };
  }
  let queries = JSON.parse(await readFile(new URL('./data/lepass-queries.json', import.meta.url), 'utf8')) as LePassQuery[];
  if (process.env.LEPASS_QUERIES_FILE) queries = JSON.parse(await readFile(process.env.LEPASS_QUERIES_FILE, 'utf8')) as LePassQuery[];
  else if (process.env.LEPASS_QUERIES_JSON) queries = JSON.parse(process.env.LEPASS_QUERIES_JSON) as LePassQuery[];
  return startLePassCollector({ ...options, queries, apiKey: apiKey!, encryptionKey: encryptionKey!, clientVersion: process.env.LEPASS_CLIENT_VERSION,
    allowGuestBootstrap: process.env.LEPASS_ALLOW_GUEST_BOOTSTRAP === 'true' });
}
