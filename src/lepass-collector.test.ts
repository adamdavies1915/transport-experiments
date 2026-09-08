import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomBytes } from 'node:crypto';
import { LePassAuth, LePassCredentialStore, LePassError, tokenRefreshDue, type LePassCredentials, type LePassTransport } from './lepass-auth.js';
import { arrivalRequest, arrivalBatchRequest, INITIAL_LEPASS_QUERIES, parseLePassArrivals, pollDelayMs } from './lepass-collector.js';
import { decodeThrift, decodeThriftSequence, encodeThrift, field, T, type ThriftField } from './lepass-thrift.js';
import { buildLePassQueryCatalog } from './lepass-discovery.js';
import { zipSync, strToU8 } from 'fflate';

const now = Date.parse('2026-09-08T02:39:50Z');
const query = INITIAL_LEPASS_QUERIES[0];
const token = (text: string) => [field(1, T.I64, now), field(2, T.I64, now + 86_400_000), field(3, T.STRING, text)];
const pair = (suffix: string) => [field(1, T.STRUCT, token(`access-${suffix}`)), field(2, T.STRUCT, token(`refresh-${suffix}`))];
function fixture(location = true, pattern = 17096284, source = 1): Buffer {
  const arrival = [field(1, T.I32, pattern), field(2, T.I64, 9007199254740993n), field(3, T.I64, now + 300_000), field(4, T.I64, now + 310_000), field(5, T.I32, 2)];
  if (location) arrival.push(field(11, T.STRUCT, [field(1, T.STRUCT, [field(1, T.I32, 29953527), field(2, T.I32, -90070130)]), field(3, T.STRING, '460::1705629903'), field(4, T.I64, now - 16_437), field(5, T.I32, 1), field(6, T.I32, source)]));
  return encodeThrift([field(1, T.I32, query.stopId), field(2, T.I32, 20703), field(3, T.STRUCT, [field(1, T.I32, query.lineId!), field(2, T.LIST, [arrival] as unknown as ThriftField[], T.STRUCT)]), field(5, T.I16, 20)]);
}

test('Thrift reader bounds input and preserves large trip IDs', () => {
  const bytes = fixture(); const parsed = decodeThrift(bytes);
  for (let i = 0; i < bytes.length; i++) assert.throws(() => decodeThrift(bytes.subarray(0, i)));
  assert.throws(() => decodeThrift(Buffer.concat([bytes, Buffer.from([0])])), /Trailing/);
  assert.throws(() => decodeThrift(Buffer.from([15, 0, 1, 8, 255, 255, 255, 255, 0])), /limit/);
  assert.throws(() => decodeThrift(Buffer.from([2, 0, 1, 1, 2, 0, 1, 0, 0])), /Duplicate/);
  assert.equal(parseLePassArrivals(parsed, query, now).observations[0].observation.trip_id, 'lepass:9007199254740993');
});

test('positions and predictions preserve separate clocks, missing speed and native identities', () => {
  const result = parseLePassArrivals(decodeThrift(fixture()), query, now);
  const o = result.observations[0].observation;
  assert.equal(o.lat, 29.953527); assert.equal(o.lon, -90.07013); assert.equal(o.speed_mph, null);
  assert.equal(o.observed_at, (now - 16_437) / 1000); assert.equal(o.received_at, now / 1000);
  assert.equal(o.vehicle_id, 'lepass:460::1705629903'); assert.notEqual(o.vehicle_id, '460');
  assert.equal(o.location_source, 'provider_gps'); assert.equal(o.route_id, '12'); assert.equal(o.direction_id, '0');
  assert.equal(result.predictions?.length, 1); assert.equal(o.mapping_confidence, 'verified');
  assert.equal(parseLePassArrivals(decodeThrift(fixture(true, 17096284, 2)), query, now).observations[0].observation.location_source, 'estimated');
});

test('missing locations do not become zero speed vehicles; changed patterns do not inherit mapping', () => {
  const missing = parseLePassArrivals(decodeThrift(fixture(false)), query, now);
  assert.equal(missing.observations.length, 0); assert.equal(missing.predictions?.length, 1);
  const changed = parseLePassArrivals(decodeThrift(fixture(true, 999)), query, now).observations[0].observation;
  assert.equal(changed.route_id, null); assert.equal(changed.direction_id, null); assert.equal(changed.mapping_confidence, 'unknown');
  assert.throws(() => parseLePassArrivals(decodeThrift(fixture()), { ...query, stopId: 1 }, now), /response_stop_mismatch/);
});

test('repeat provider payloads remain identical across receipt times for lossless compaction', () => {
  const first = parseLePassArrivals(decodeThrift(fixture()), query, now);
  const repeat = parseLePassArrivals(decodeThrift(fixture()), query, now + 20_000);
  assert.deepEqual(first.observations[0].raw, repeat.observations[0].raw);
  assert.notEqual(first.observations[0].observation.received_at, repeat.observations[0].observation.received_at);
  assert.notEqual(first.batch_id, repeat.batch_id);
});

test('requests preserve stop/line namespaces and polling cannot exceed source limits', () => {
  const line = arrivalRequest(query), stop = arrivalRequest({ ...query, lineId: undefined });
  assert.equal(line.path, 'V4/LineArrivals'); assert.equal(stop.path, 'V4/StopsArrivals');
  assert.deepEqual(decodeThrift(line.body)[1], [{ 1: 4539336, 2: 8697788 }]);
  assert.deepEqual(decodeThrift(stop.body)[1], [4539336]);
  assert.equal(pollDelayMs(1, 0), 20_000); assert.equal(pollDelayMs(60, 0), 60_000);
  assert.equal(pollDelayMs(20, 3), 80_000); assert.equal(pollDelayMs(20, 0, 120_000), 120_000);
});

test('native batches preserve independent response boundaries and enforce the request-size cap', () => {
  const request = arrivalBatchRequest([query, { ...query, id: 'other', stopId: 46011071, lineId: 3237644 }]);
  assert.equal((decodeThrift(request.body)[1] as unknown[]).length, 2);
  assert.equal(decodeThriftSequence(Buffer.concat([fixture(), fixture(false)])).length, 2);
  assert.throws(() => arrivalBatchRequest(Array.from({ length: 9 }, () => query)), /invalid_query_batch/);
  assert.throws(() => decodeThriftSequence(Buffer.concat([fixture(), fixture().subarray(0, 6)])), /Truncated/);
});

test('catalog verifies RTA identity and ordered stop geography; ambiguous direction stays candidate', () => {
  const csv = {
    'routes.txt': 'route_id,route_short_name,route_long_name,route_type\n12,12,Streetcar,0\n9999,9999,TEST ROUTE,3\n',
    'stops.txt': 'stop_id,stop_code,stop_lat,stop_lon\na,1,29.95,-90.07\nb,2,29.95,-90.07\nc,3,29.95,-90.07\nd,4,29.95,-90.07\n',
    'trips.txt': 'route_id,trip_id,direction_id\n12,out,0\n12,in,1\n',
    'stop_times.txt': 'trip_id,stop_id,stop_sequence\nout,a,1\nout,b,2\nout,c,3\nout,d,4\nin,d,1\nin,c,2\nin,b,3\nin,a,4\n',
  };
  const entities = [{ 1: { 9: { 1: 7, 2: [101, 102, 103, 104] } } }, ...[1, 2, 3, 4].map(id => ({ 1: { 5: { 1: 100 + id, 3: { 1: 29950000, 2: -90070000 }, 4: String(id) } } }))];
  const input = { gtfsZip: zipSync(Object.fromEntries(Object.entries(csv).map(([k, v]) => [k, strToU8(v)]))), entities,
    lineEntities: [{ 1: { 8: { 1: 740760, 2: '12', 3: 1185063, 6: [{ 1: 8697788, 3: 'Outbound' }] } } }],
    memberships: [{ routeId: '12', lineId: 8697788, patternId: 7, groupId: 740760 }], metroRevision: '123', fetchedAt: new Date(now).toISOString() };
  const catalog = buildLePassQueryCatalog(input);
  assert.equal(catalog.queries.length, 1); assert.equal(catalog.queries[0].stopId, 103); assert.equal(catalog.queries[0].gtfsStopId, 'c');
  assert.equal(catalog.queries[0].mappingConfidence, 'verified'); assert.equal(catalog.queries[0].patterns?.[0].directionId, '0');
  const ambiguous = { ...csv, 'stop_times.txt': csv['stop_times.txt'].replace('in,d,1\nin,c,2\nin,b,3\nin,a,4', 'in,a,1\nin,b,2\nin,c,3\nin,d,4') };
  const candidate = buildLePassQueryCatalog({ ...input, gtfsZip: zipSync(Object.fromEntries(Object.entries(ambiguous).map(([k, v]) => [k, strToU8(v)]))) });
  assert.equal(candidate.queries[0].mappingConfidence, 'candidate'); assert.equal(candidate.queries[0].patterns?.[0].directionId, null);
  assert.equal(buildLePassQueryCatalog({ ...input, lineEntities: [{ 1: { 8: { 1: 740760, 2: '12', 3: 1167076, 6: [{ 1: 8697788, 3: 'Other agency' }] } } }] }).queries.length, 0);
});

test('credential encryption authenticates state, replaces atomically and reloads both rotated tokens', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lepass-auth-test-'));
  try {
    const key = randomBytes(32).toString('base64'), path = join(directory, 'state.enc'), store = new LePassCredentialStore(path, key);
    await store.save({ token: 'secret-value', version: 1 });
    assert.ok(!(await readFile(path)).includes(Buffer.from('secret-value')));
    assert.deepEqual(await store.load(), { token: 'secret-value', version: 1 });
    await store.save({ token: 'rotated-value', version: 1 });
    assert.deepEqual(await store.load(), { token: 'rotated-value', version: 1 });
    await assert.rejects(new LePassCredentialStore(path, randomBytes(32).toString('base64')).load(), /credential_decryption_failed/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('one guest, shared refresh, complete rotation and restart without registration', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lepass-client-test-'));
  try {
    let guests = 0, refreshes = 0;
    const transport: LePassTransport = async (endpoint, body, headers) => {
      if (endpoint.endsWith('CreateUser')) { guests++; assert.equal(headers.USER_KEY, undefined); return { body: encodeThrift([field(1, T.STRUCT, [field(1, T.STRING, 'test-user'), field(7, T.STRUCT, [field(1, T.STRUCT, pair('one'))])])]), receivedAt: now }; }
      if (endpoint.endsWith('RefreshTokens')) { refreshes++; assert.equal(headers['Access-Token'], undefined); assert.equal(decodeThrift(body)[1], 'refresh-one'); return { body: encodeThrift([field(1, T.STRUCT, pair('two'))]), receivedAt: now }; }
      throw new Error('Unexpected request');
    };
    const options = { stateDir: directory, encryptionKey: randomBytes(32).toString('base64'), apiKey: 'test-app-key', allowGuestBootstrap: true, now: () => now, transport };
    const auth = new LePassAuth(options), original = await auth.get();
    const [a, b] = await Promise.all([auth.get(true), auth.get(true)]);
    assert.equal(guests, 1); assert.equal(refreshes, 1); assert.equal(a.access.token, 'access-two'); assert.equal(a.refresh.token, 'refresh-two');
    assert.deepEqual(a, b); assert.notEqual(a.access.token, original.access.token);
    const restored = await new LePassAuth(options).get(); assert.deepEqual(restored, a); assert.equal(guests, 1);
    assert.equal(tokenRefreshDue(a.access, now + 86_400_000 * .9 - 1), false);
    assert.equal(tokenRefreshDue(a.access, now + 86_400_000 * .9), true);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('lost guest response stops automatic account creation on restart', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lepass-bootstrap-test-'));
  try {
    let attempts = 0;
    const options = { stateDir: directory, encryptionKey: randomBytes(32).toString('base64'), apiKey: 'test', allowGuestBootstrap: true, transport: async () => { attempts++; throw new LePassError('network_error'); } };
    await assert.rejects(new LePassAuth(options).get(), /network_error/);
    await assert.rejects(new LePassAuth(options).get(), /guest_bootstrap_requires_recovery/);
    assert.equal(attempts, 1);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('401 refresh retries exactly once and does not loop on a rejected replacement', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lepass-retry-test-'));
  try {
    let arrivals = 0, refreshes = 0;
    const options = { stateDir: directory, encryptionKey: randomBytes(32).toString('base64'), apiKey: 'test', now: () => now };
    const credentials: LePassCredentials = { version: 1, installationId: 'test', userKey: 'user', access: { issuedAt: now, expiresAt: now + 86_400_000, token: 'access-one' }, refresh: { issuedAt: now, expiresAt: now + 86_400_000, token: 'refresh-one' } };
    await new LePassCredentialStore(join(directory, 'lepass-credentials.enc'), options.encryptionKey).save(credentials);
    const transport: LePassTransport = async (endpoint) => {
      if (endpoint.includes('GetMetroData')) return { body: encodeThrift([field(1, T.I32, 1504), field(14, T.I64, now)]), receivedAt: now };
      if (endpoint.endsWith('RefreshTokens')) { refreshes++; return { body: encodeThrift([field(1, T.STRUCT, pair('two'))]), receivedAt: now }; }
      arrivals++; throw new LePassError('authentication_rejected', 401);
    };
    const auth = new LePassAuth({ ...options, transport }), request = arrivalRequest(query);
    await assert.rejects(auth.request(request.path, request.body), /authentication_rejected/);
    assert.equal(arrivals, 2); assert.equal(refreshes, 1);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('a changed metro revision refreshes metadata once without rotating a valid session', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lepass-revision-test-'));
  try {
    let arrivals = 0, metadata = 0;
    const options = { stateDir: directory, encryptionKey: randomBytes(32).toString('base64'), apiKey: 'test', now: () => now };
    const credentials: LePassCredentials = { version: 1, installationId: 'test', userKey: 'user', access: { issuedAt: now, expiresAt: now + 86_400_000, token: 'access-one' }, refresh: { issuedAt: now, expiresAt: now + 86_400_000, token: 'refresh-one' } };
    await new LePassCredentialStore(join(directory, 'lepass-credentials.enc'), options.encryptionKey).save(credentials);
    const transport: LePassTransport = async (endpoint, _body, headers) => {
      if (endpoint.includes('GetMetroData')) {
        metadata++;
        return { body: encodeThrift([field(1, T.I32, 1504), field(14, T.I64, now + metadata)]), receivedAt: now };
      }
      assert.ok(endpoint.endsWith('LineArrivals'), 'no account creation or refresh is needed');
      arrivals++;
      assert.equal(headers['Metro-Revision-Number'], String(now + arrivals));
      if (arrivals === 1) throw new LePassError('http_error', 412);
      return { body: fixture(), receivedAt: now };
    };
    const auth = new LePassAuth({ ...options, transport }), request = arrivalRequest(query);
    await auth.request(request.path, request.body);
    assert.equal(arrivals, 2); assert.equal(metadata, 2);
    assert.equal(auth.currentMetroRevision, String(now + 2));
  } finally { await rm(directory, { recursive: true, force: true }); }
});
