/** Refresh the known RTA line groups from normal read-only app endpoints. No guest creation. */
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { LePassAuth, LePassCredentialStore, fetchLePass, type LePassCredentials } from '../src/lepass-auth.js';
import { buildLePassQueryCatalog, type LePassMembership } from '../src/lepass-discovery.js';
import { decodeThrift, decodeThriftSequence, encodeThrift, field, numeric, stringId, struct, T, type ThriftStruct, type ThriftValue } from '../src/lepass-thrift.js';

const usage = 'Usage: node --import tsx scripts/lepass-discover.ts --gtfs GTFS.zip --state-dir /app/data/lepass [--service-date YYYYMMDD] [--output src/data/lepass-queries.json]\nRequires LEPASS_API_KEY and LEPASS_ENCRYPTION_KEY; reads an existing encrypted guest without refreshing or creating accounts.';
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  if (args.includes('--help')) { console.log(usage); return; }
  function option(name: string): string | undefined { const index = args.indexOf(name); return index >= 0 ? args[index + 1] : undefined; }
  const gtfs = option('--gtfs'), stateDir = option('--state-dir');
  if (!gtfs || !stateDir) throw new Error(usage);
  const apiKey = process.env.LEPASS_API_KEY, encryptionKey = process.env.LEPASS_ENCRYPTION_KEY;
  if (!apiKey || !encryptionKey) throw new Error('Le Pass credentials are not configured');
  const output = resolve(option('--output') ?? fileURLToPath(new URL('../src/data/lepass-queries.json', import.meta.url)));
  const serviceDate = option('--service-date') ?? new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Chicago', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date()).replaceAll('-', '');
  if (!/^\d{8}$/.test(serviceDate)) throw new Error('Service date must be YYYYMMDD');
  const previous = JSON.parse(await readFile(new URL('../src/data/lepass-catalog-metadata.json', import.meta.url), 'utf8'));
  const seed = [...new Map((previous.patterns as LePassMembership[]).map(p => [p.groupId, { groupId: p.groupId, routeId: p.routeId }])).values()];
  const auth = new LePassAuth({ stateDir, encryptionKey, apiKey });
  const store = new LePassCredentialStore(join(stateDir, 'lepass-credentials.enc'), encryptionKey);
  // The running collector owns token rotation. Re-read its committed pair for each request.
  let previousRequest = -Infinity;
  async function request(url: string, body: Buffer = Buffer.alloc(0), method: 'GET' | 'POST' = 'GET', revision?: string): Promise<Buffer> {
    const remaining = 1100 - (performance.now() - previousRequest);
    if (remaining > 0) await delay(remaining);
    const credentials = await store.load<LePassCredentials>();
    if (!credentials?.userKey) throw new Error('A working encrypted guest must be bootstrapped first');
    previousRequest = performance.now();
    const result = await fetchLePass(url, body, { ...auth.headers(credentials, false), ...(revision ? { 'Metro-Revision-Metro-Id': '1504', 'Metro-Revision-Number': revision } : {}) }, undefined, method);
    return result.body;
  }
  const metro = decodeThrift(await request('https://app5.moovitapp.com/services-app/services/V4/GetMetroData?metroAreaId=1504&metroRevisionNumber=0'));
  const revision = stringId(metro[14]) ?? ''; if (numeric(metro[1]) !== 1504 || !revision) throw new Error('Invalid New Orleans metro metadata');
  const memberships: LePassMembership[] = [];
  for (const group of seed) {
    const query = new URLSearchParams({ serviceDate, lineGroupId: String(group.groupId), metroAreaId: '1504', metroRevisionNumber: revision, osTypeId: '2', protocolVersionId: '4' });
    const data = decodeThrift(await request(`https://app4cdn.moovitapp.com/services-app/services/V4/GetLineGroupTrips?${query}`, undefined, 'GET', revision));
    if (!Array.isArray(data[1])) throw new Error('Invalid line group metadata');
    for (const value of data[1]) {
      const line = struct(value), id = numeric(line?.[1]);
      if (!id || !Array.isArray(line?.[3])) continue;
      for (const patternId of new Set(line[3].flatMap(interval => { const id = numeric(struct(interval)?.[2]); return id === null ? [] : [id]; }))) {
        memberships.push({ ...group, lineId: id, patternId });
      }
    }
    console.log(`Read route ${group.routeId}`);
  }
  async function entities(kind: number, ids: number[]): Promise<ThriftStruct[]> {
    const result: ThriftStruct[] = [];
    for (let start = 0; start < ids.length; start += 60) {
      const item = [field(1, T.I32, kind), field(2, T.SET, ids.slice(start, start + 60), T.I32)];
      const body = encodeThrift([field(1, T.LIST, [item] as unknown as ThriftValue[], T.STRUCT), field(2, T.BOOL, true), field(3, T.STRING, '/services-app/services/V4/GetLineGroupTrips')]);
      result.push(...decodeThriftSequence(await request('https://app5.moovitapp.com/services-app/services/Sync/Entities', body, 'POST', revision)));
    }
    return result;
  }
  const patterns = await entities(13, [...new Set(memberships.map(m => m.patternId))]);
  const lines = await entities(4, [...new Set(memberships.map(m => m.lineId))]);
  const result = buildLePassQueryCatalog({ gtfsZip: await readFile(gtfs), entities: patterns, lineEntities: lines, memberships, metroRevision: revision, fetchedAt: new Date().toISOString() });
  if (!result.queries.length) throw new Error('No usable RTA queries; existing catalog preserved');
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, JSON.stringify(result.queries, null, 2) + '\n');
  await writeFile(join(dirname(output), 'lepass-catalog-metadata.json'), JSON.stringify(result.metadata, null, 2) + '\n');
  console.log(JSON.stringify({ queries: result.queries.length, routes: result.metadata.routes, candidate_patterns: result.metadata.candidate_patterns, missing_gtfs_routes: result.metadata.missing_gtfs_routes }));
}
main().catch(error => { console.error(error instanceof Error && error.message.startsWith('Usage:') ? error.message : 'Le Pass catalog refresh failed; inspect configuration, source health and retry later. No credentials were logged.'); process.exitCode = 1; });
