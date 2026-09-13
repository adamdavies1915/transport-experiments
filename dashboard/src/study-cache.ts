/** Shared browser cache for public study responses. No credentials or raw
 * vehicle observations are stored here. Each filter combination is isolated. */
export const STUDY_CACHE_FRESH_MS = 60_000;
export interface StudyCacheState {
  data?: unknown;
  error?: string;
  refreshing: boolean;
  checkedAt?: number;
}
export const EMPTY_STUDY_CACHE: StudyCacheState = Object.freeze({ refreshing: false });

export function studyRequestKey(url: string): string {
  const parsed = new URL(url, 'http://study.local');
  if (!['/api/row-study', '/api/signal-study'].includes(parsed.pathname) || parsed.origin !== 'http://study.local') throw new Error('Invalid study endpoint.');
  const params = parsed.searchParams;
  for (const [key, value] of [['mode', 'streetcar'], ['hour_from', '0'], ['hour_to', '23'], ['day_type', 'all']]) {
    // Duplicate parameters are invalid on the server; do not normalize them
    // into a valid, cached request that would conceal the error.
    if (params.getAll(key).length === 1 && params.get(key) === value) params.delete(key);
  }
  params.sort();
  return `${parsed.pathname}${params.size ? `?${params}` : ''}`;
}

interface Entry {
  state: StudyCacheState;
  listeners: Set<() => void>;
  pending?: Promise<void>;
  bytes: number;
  touched: number;
}
interface Options {
  fetcher?: typeof fetch;
  now?: () => number;
  freshMs?: number;
  maxEntries?: number;
  maxBytes?: number;
  timeoutMs?: number;
}

export function createStudyCache(options: Options = {}) {
  const entries = new Map<string, Entry>();
  const now = options.now ?? Date.now;
  let sequence = 0;
  const entry = (key: string) => {
    let value = entries.get(key);
    if (!value) { value = { state: EMPTY_STUDY_CACHE, listeners: new Set(), bytes: 0, touched: ++sequence }; entries.set(key, value); }
    return value;
  };
  const emit = (value: Entry, state: StudyCacheState) => { value.state = state; for (const listener of value.listeners) listener(); };
  const prune = () => {
    let bytes = [...entries.values()].reduce((total, value) => total + value.bytes, 0);
    const inactive = [...entries].filter(([, value]) => !value.listeners.size && !value.pending).sort(([, a], [, b]) => a.touched - b.touched);
    for (const [key, value] of inactive) {
      if (entries.size <= (options.maxEntries ?? 6) && bytes <= (options.maxBytes ?? 64 * 1024 * 1024)) break;
      entries.delete(key); bytes -= value.bytes;
    }
    // Mounted views and in-flight requests retain their working data. Once
    // released, they are subject to the same entry/serialized-byte limits.
  };
  const generatedAt = (data: unknown): number => {
    if (!data || typeof data !== 'object' || !('snapshot' in data)) return NaN;
    const snapshot = data.snapshot;
    return snapshot && typeof snapshot === 'object' && 'generated_at' in snapshot && typeof snapshot.generated_at === 'string' ? Date.parse(snapshot.generated_at) : NaN;
  };
  return {
    read(url: string): StudyCacheState { const value = entry(studyRequestKey(url)); value.touched = ++sequence; return value.state; },
    subscribe(url: string, listener: () => void): () => void {
      const value = entry(studyRequestKey(url)); value.listeners.add(listener); value.touched = ++sequence;
      return () => { value.listeners.delete(listener); prune(); };
    },
    load(url: string, force = false): Promise<void> {
      const key = studyRequestKey(url), value = entry(key);
      value.touched = ++sequence;
      if (value.pending) return value.pending;
      if (!force && value.state.checkedAt != null && now() - value.state.checkedAt < (options.freshMs ?? STUDY_CACHE_FRESH_MS)) return Promise.resolve();
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? 20_000);
      const previous = value.state;
      let failure = 'The latest study could not be loaded.';
      // Set pending before notifying subscribers so simultaneous mounts share
      // one request, including when a subscriber immediately asks to refresh.
      const pending = Promise.resolve().then(async () => {
        try {
          const response = await (options.fetcher ?? fetch)(key, { signal: controller.signal, cache: previous.data || force ? 'no-cache' : 'default' });
          if (!response.ok) {
            if (response.status === 400) {
              const problem: unknown = await response.json().catch(() => null);
              if (problem && typeof problem === 'object' && 'error' in problem && typeof problem.error === 'string' && problem.error.length <= 500) failure = problem.error;
            }
            throw new Error('Study unavailable');
          }
          const text = await response.text();
          const data: unknown = JSON.parse(text);
          const field = key.startsWith('/api/row-study') ? 'comparisons' : 'signals';
          if (!data || typeof data !== 'object' || !('cells' in data) || !Array.isArray(data.cells) || !(field in data) || !Array.isArray((data as Record<string, unknown>)[field])) throw new Error('Invalid study');
          if (!('network' in data) || !data.network || typeof data.network !== 'object'
            || !('paths' in data.network) || !Array.isArray(data.network.paths)
            || !('sites' in data.network) || !Array.isArray(data.network.sites)
            || !('quality' in data) || !Array.isArray(data.quality)
            || !('limitations' in data) || !Array.isArray(data.limitations)
            || !('limitations' in data.network) || !Array.isArray(data.network.limitations)
            || field === 'comparisons' && (!('coverage' in data) || !data.coverage || typeof data.coverage !== 'object')) throw new Error('Invalid study');
          if (generatedAt(data) < generatedAt(previous.data)) throw new Error('Older study');
          value.bytes = new TextEncoder().encode(text).byteLength;
          emit(value, { data, refreshing: false, checkedAt: now() });
        } catch {
          emit(value, { ...previous, refreshing: false, error: failure });
        } finally {
          clearTimeout(timeout); value.pending = undefined; prune();
        }
      });
      value.pending = pending;
      emit(value, { ...previous, error: undefined, refreshing: true });
      return pending;
    },
  };
}

export const studyCache = createStudyCache();
