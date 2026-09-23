import { readFile, mkdir, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { pathToFileURL } from 'node:url';

export function clockCheck(timestamp, maximumAgeMs, now = Date.now()) {
  const instant = typeof timestamp === 'string' ? Date.parse(timestamp) : NaN;
  const age = now - instant;
  return { status: Number.isFinite(age) && age >= -60_000 && age <= maximumAgeMs ? 'ok' : 'stale',
    timestamp: timestamp ?? null, age_seconds: Number.isFinite(age) ? Math.round(age / 1000) : null };
}
export async function checkPipeline({ collectorUrl, dashboardUrl, localUrl, localToken, fetcher = fetch, now = Date.now() }) {
  const checks = {};
  async function get(url, token) {
    const r = await fetcher(url, { headers: token ? { Authorization: `Bearer ${token}` } : {}, redirect: 'error', signal: AbortSignal.timeout(15_000) });
    if (!r.ok) throw new Error('unavailable');
    return r.json();
  }
  await Promise.all([
    (async () => {
      try {
        const d = await get(collectorUrl);
        checks.collection = clockCheck(d.last_received_at, 5 * 60_000, now);
        checks.persistence = clockCheck(d.last_persisted_at, 5 * 60_000, now);
        if (d.connected === false || d.status === 'paused') checks.collection.status = 'unavailable';
      } catch { checks.collection = { status: 'unavailable' }; checks.persistence = { status: 'unavailable' }; }
    })(),
    (async () => {
      try {
        const d = await get(dashboardUrl);
        checks.analysis = clockCheck(d.summary?.generated_at, 36 * 3600_000, now);
        // This is successful summary transport, NOT the analysis/publication clock.
        checks.summary_delivery = clockCheck(d.summary?.received_at, 5 * 60_000, now);
        if (d.summary?.refresh_error) checks.summary_delivery.status = 'unavailable';
      } catch { checks.analysis = { status: 'unavailable' }; checks.summary_delivery = { status: 'unavailable' }; }
    })(),
    ...(localUrl ? [(async () => {
      try {
        const d = await get(localUrl, localToken);
        checks.workstation_capture = clockCheck(d.last_persisted_at, 5 * 60_000, now);
        const lastSuccess = (d.lepass?.queries ?? []).map(q => q.lastSuccess).filter(t => typeof t === 'string').sort().at(-1);
        checks.lepass = { ...clockCheck(lastSuccess, 5 * 60_000, now), reason: d.lepass?.reason ?? null };
        if (d.lepass?.status !== 'collecting') checks.lepass.status = 'degraded';
        if (d.paused) checks.workstation_capture.status = 'paused';
      } catch { checks.workstation_capture = { status: 'unavailable' }; checks.lepass = { status: 'unavailable' }; }
    })()] : []),
  ]);
  return { checked_at: new Date(now).toISOString(), status: Object.values(checks).every(c => c.status === 'ok') ? 'ok' : 'degraded', checks };
}

async function main() {
  const result = await checkPipeline({
    collectorUrl: process.env.TRANSIT_COLLECTOR_HEALTH_URL || 'https://nola-transit.cargobay.dev/api/health',
    dashboardUrl: process.env.TRANSIT_DASHBOARD_HEALTH_URL || 'https://nola-transit-dashboard.cargobay.dev/api/health',
    localUrl: process.env.TRANSIT_LOCAL_HEALTH_URL,
    localToken: process.env.TRANSIT_LOCAL_TOKEN_FILE ? (await readFile(process.env.TRANSIT_LOCAL_TOKEN_FILE, 'utf8')).trim() : undefined,
  });
  if (process.env.TRANSIT_HEALTH_FILE) {
    const path = process.env.TRANSIT_HEALTH_FILE;
    await mkdir(dirname(path), { recursive: true });
    await writeFile(`${path}.tmp`, JSON.stringify(result, null, 2) + '\n', { mode: 0o600 });
    await rename(`${path}.tmp`, path);
  }
  console.log(JSON.stringify(result));
  if (result.status !== 'ok') process.exitCode = 1;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href)
  main().catch(() => { console.error('Pipeline health check failed'); process.exitCode = 1; });
