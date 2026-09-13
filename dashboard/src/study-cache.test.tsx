import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStudyCache, studyRequestKey, STUDY_CACHE_FRESH_MS } from './study-cache';
import { rowData } from './study-fixtures';

const url = '/api/row-study';
const study = (generated_at = '2026-09-13T20:00:00Z', stale = false) => ({ ...rowData(), snapshot: {
  generated_at, received_at: '2026-09-13T20:00:20Z', origin: 'collector', stale, refresh_error: null,
} });
const deferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};

test('canonical keys share only equivalent defaults and preserve every actual selection', () => {
  assert.equal(studyRequestKey('/api/row-study?mode=streetcar&hour_to=23&hour_from=0'), url);
  assert.equal(studyRequestKey('/api/row-study?day_type=all&mode=streetcar'), url);
  assert.equal(studyRequestKey('/api/row-study?route_id=12&source=sse'), studyRequestKey('/api/row-study?source=sse&route_id=12'));
  for (const query of ['mode=bus', 'source=sse', 'source=lepass', 'direction_id=0', 'hour_from=1', 'day_type=weekday', 'from=2026-09-01', 'to=2026-09-13', 'mode=streetcar&mode=streetcar']) {
    assert.notEqual(studyRequestKey(`${url}?${query}`), url);
  }
  assert.notEqual(studyRequestKey('/api/signal-study'), url);
  assert.throws(() => studyRequestKey('https://elsewhere.test/api/row-study'));
});

test('story and detail reuse completed and in-flight responses across unmounts', async () => {
  const response = deferred<Response>(); let calls = 0; let signal: AbortSignal | null | undefined;
  const cache = createStudyCache({ fetcher: async (_url, options) => { calls++; signal = options?.signal; return response.promise; } });
  const unsubscribeStory = cache.subscribe(url, () => {});
  const first = cache.load(url);
  const second = cache.load(`${url}?mode=streetcar&hour_from=0&hour_to=23`);
  assert.strictEqual(first, second);
  const unsubscribeDetail = cache.subscribe(url, () => {}); unsubscribeStory();
  await Promise.resolve(); assert.equal(calls, 1); assert.equal(signal?.aborted, false);
  response.resolve(Response.json(study())); await first;
  const saved = cache.read(url);
  assert.equal(saved.refreshing, false); assert.ok(saved.data);
  await cache.load(`${url}?mode=streetcar`);
  assert.equal(calls, 1); assert.strictEqual(cache.read(url), saved);
  unsubscribeDetail();
});

test('expiry refresh keeps visible data and replaces it with original server freshness clocks', async () => {
  let now = 1000; let calls = 0; const response = deferred<Response>(); const modes: RequestCache[] = [];
  const cache = createStudyCache({ now: () => now, fetcher: async (_url, options) => {
    modes.push(options!.cache!); calls++;
    return calls === 1 ? Response.json(study()) : response.promise;
  } });
  await cache.load(url); const before = cache.read(url).data;
  now += STUDY_CACHE_FRESH_MS;
  const refresh = cache.load(url);
  assert.strictEqual(cache.read(url).data, before); assert.equal(cache.read(url).refreshing, true);
  const newer = study('2026-09-13T20:15:00Z', true);
  response.resolve(Response.json(newer)); await refresh;
  assert.deepEqual(cache.read(url).data, newer); assert.equal(cache.read(url).checkedAt, now);
  assert.deepEqual(modes, ['default', 'no-cache']);
});

test('failed refresh keeps only matching cached data; a new bus or date query cannot show streetcars', async () => {
  let failed = false;
  const cache = createStudyCache({ fetcher: async () => failed ? new Response('Unavailable', { status: 503 }) : Response.json(study()) });
  await cache.load(url); const saved = cache.read(url).data; failed = true;
  await cache.load(url, true);
  assert.strictEqual(cache.read(url).data, saved); assert.ok(cache.read(url).error);
  for (const other of [`${url}?mode=bus`, `${url}?from=2026-09-01`, `${url}?source=lepass`]) {
    assert.equal(cache.read(other).data, undefined);
    await cache.load(other); assert.equal(cache.read(other).data, undefined); assert.ok(cache.read(other).error);
  }
  assert.strictEqual(cache.read(url).data, saved);
});

test('invalid or older responses never replace the last good analysis', async () => {
  let next: unknown = study();
  const cache = createStudyCache({ fetcher: async () => Response.json(next) });
  await cache.load(url); const saved = cache.read(url).data;
  for (const bad of [{ error: 'not a study' }, { cells: [], comparisons: [] }, { ...study(), comparisons: null }, study('2026-09-12T20:00:00Z')]) {
    next = bad; await cache.load(url, true);
    assert.strictEqual(cache.read(url).data, saved); assert.ok(cache.read(url).error);
  }
  next = study('2026-09-13T20:30:00Z'); await cache.load(url, true);
  assert.equal(cache.read(url).error, undefined); assert.deepEqual(cache.read(url).data, next);
});

test('invalid filter requests retain the actionable server message without caching their response', async () => {
  const cache = createStudyCache({ fetcher: async () => Response.json({ error: 'That range is outside the saved summary.' }, { status: 400 }) });
  await cache.load(`${url}?from=2020-01-01`);
  const result = cache.read(`${url}?from=2020-01-01`);
  assert.equal(result.data, undefined); assert.equal(result.error, 'That range is outside the saved summary.');
});

test('least-used inactive results are evicted; active views survive until released', async () => {
  const cache = createStudyCache({ maxEntries: 2, fetcher: async () => Response.json(study()) });
  const a = `${url}?route_id=12`, b = `${url}?route_id=47`, c = `${url}?route_id=49`;
  await cache.load(a); await cache.load(b);
  const unsubscribe = cache.subscribe(a, () => {});
  await cache.load(c);
  assert.ok(cache.read(a).data); assert.ok(cache.read(c).data); assert.equal(cache.read(b).data, undefined);
  unsubscribe();
  const small = createStudyCache({ maxBytes: 1, fetcher: async () => Response.json(study()) });
  const stop = small.subscribe(url, () => {}); await small.load(url);
  assert.ok(small.read(url).data); stop(); assert.equal(small.read(url).data, undefined);
});

test('a bounded request timeout keeps previous data and permits retry', async () => {
  let hang = false;
  const cache = createStudyCache({ timeoutMs: 5, fetcher: async (_url, options) => {
    if (!hang) return Response.json(study());
    return new Promise<Response>((_resolve, reject) => options!.signal!.addEventListener('abort', () => reject(new Error('Timeout')), { once: true }));
  } });
  await cache.load(url); const saved = cache.read(url).data; hang = true;
  await cache.load(url, true); assert.strictEqual(cache.read(url).data, saved); assert.ok(cache.read(url).error);
  hang = false; await cache.load(url, true); assert.equal(cache.read(url).error, undefined);
});
