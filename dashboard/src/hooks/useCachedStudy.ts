import { useCallback, useEffect, useSyncExternalStore } from 'react';
import { EMPTY_STUDY_CACHE, STUDY_CACHE_FRESH_MS, studyCache, studyRequestKey } from '../study-cache';

export interface CachedStudyResult<T> {
  data?: T;
  error?: string;
  loading: boolean;
  refreshing: boolean;
  retry: () => void;
}

export function useCachedStudy<T>(url: string, supplied?: T): CachedStudyResult<T> {
  const key = studyRequestKey(url);
  const subscribe = useCallback((listener: () => void) => supplied ? () => {} : studyCache.subscribe(key, listener), [key, supplied]);
  const read = useCallback(() => supplied ? EMPTY_STUDY_CACHE : studyCache.read(key), [key, supplied]);
  const state = useSyncExternalStore(subscribe, read, () => EMPTY_STUDY_CACHE);
  useEffect(() => {
    if (supplied) return;
    const refresh = () => { if (document.visibilityState !== 'hidden') void studyCache.load(key); };
    void studyCache.load(key);
    window.addEventListener('focus', refresh);
    document.addEventListener('visibilitychange', refresh);
    return () => {
      window.removeEventListener('focus', refresh);
      document.removeEventListener('visibilitychange', refresh);
      // Requests belong to the shared cache, not to this component. Navigation
      // must not cancel a request that the next view can reuse.
    };
  }, [key, supplied]);
  useEffect(() => {
    if (supplied || state.refreshing) return;
    // Schedule from the response's completion time. A mount-based interval can
    // miss expiry by a second and accidentally leave data unchanged for twice
    // the intended TTL. Failed refreshes get a one-minute retry backoff.
    const delay = state.error || state.checkedAt == null ? STUDY_CACHE_FRESH_MS
      : Math.max(0, STUDY_CACHE_FRESH_MS - (Date.now() - state.checkedAt));
    const timeout = setTimeout(() => { if (document.visibilityState !== 'hidden') void studyCache.load(key); }, delay);
    return () => clearTimeout(timeout);
  }, [key, supplied, state.checkedAt, state.error, state.refreshing]);
  const retry = useCallback(() => { if (!supplied) void studyCache.load(key, true); }, [key, supplied]);
  const data = supplied ?? state.data as T | undefined;
  return { data, error: supplied ? undefined : state.error, loading: !data && !state.error,
    refreshing: !supplied && !!data && state.refreshing, retry };
}
