import type { LegacySummary } from './summary-data';
import type { OtpDay } from './otp-data';
import type { StreetcarData, StreetcarNetwork } from './streetcar-data';
import { otpDaysSql } from './otp-query';
import { streetcarCatalog, streetcarQuote as q, streetcarQueries, decodeStreetcarBins, decodeStreetcarSiteBins, decodeStreetcarQuality } from './streetcar-query';
import { PRIORITY_METHOD, PRIORITY_WAIT_METHOD, type PriorityData, type PriorityWaitData } from './priority-data';
import { priorityStatsSql, decodePriorityStats, priorityProfiles } from './priority-query';

export type LegacyQuery = (sql: string) => Promise<Record<string, unknown>[]>;
/** Pure summary builder: caller supplies its LOCAL database query function. */
export async function buildLegacySummary(query: LegacyQuery, database: string): Promise<LegacySummary> {
  const DATABASE_NAME = streetcarCatalog(database);
  const legacy: LegacySummary = { errors: {} };
  const queries: Record<string, string> = {
  'summary': `
      SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT route) as total_routes,
        COUNT(DISTINCT vid) as total_vehicles,
        MIN(timestamp) as first_record,
        MAX(timestamp) as last_record
      FROM ${DATABASE_NAME}.transit_data
    `,
  'segment_types': `
      SELECT
        segment_type,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY segment_type
      ORDER BY segment_type
    `,
  'segments': `
      SELECT
        segment_name,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_name IS NOT NULL
      GROUP BY segment_name, segment_type
      ORDER BY avg_speed DESC
    `,
  'routes': `
      SELECT
        route,
        COUNT(*) as readings,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route != 'U'
      GROUP BY route
      HAVING COUNT(*) > 100
      ORDER BY not_flagged_pct DESC
    `,
  'hourly': `
      SELECT
        EXTRACT(HOUR FROM timestamp) as hour,
        segment_type,
        ROUND(100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*), 2) as delay_pct,
        ROUND(AVG(speed), 1) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY EXTRACT(HOUR FROM timestamp), segment_type
      ORDER BY hour
    `,
  'daily': `
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        COUNT(*) as readings,
        COUNT(DISTINCT vid) as vehicles,
        SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      GROUP BY DATE_TRUNC('day', timestamp)
      ORDER BY date
    `,
  'daily_segments': `
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        segment_type,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route = '12' AND segment_type IS NOT NULL
      GROUP BY DATE_TRUNC('day', timestamp), segment_type
      ORDER BY date, segment_type
    `,
  'daily_routes': `
      SELECT
        DATE_TRUNC('day', timestamp) as date,
        route,
        COUNT(*) as readings,
        ROUND(100.0 - (100.0 * SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) / COUNT(*)), 2) as not_flagged_pct,
        ROUND(AVG(speed), 2) as avg_speed
      FROM ${DATABASE_NAME}.transit_data
      WHERE route != 'U'
      GROUP BY DATE_TRUNC('day', timestamp), route
      HAVING COUNT(*) > 50
      ORDER BY date, route
    `,
  };
  for (const [key, sql] of Object.entries(queries)) {
    try {
      const rows = await query(sql);
      Object.assign(legacy, { [key]: key === 'summary' ? rows[0] : rows });
    } catch { legacy.errors![key] = 'This historical summary has not been prepared yet.'; }
  }
  try {
    const days = await query(otpDaysSql(database)) as unknown as OtpDay[];
    legacy.otp = { status: days.length ? 'ready' : 'not_ready', days };
  } catch { legacy.errors!.otp = 'OTP results have not been prepared yet.'; }
  try {
    const saved = (await query(`SELECT network_json,method_json FROM ${DATABASE_NAME}.streetcar_networks ORDER BY installed_at DESC LIMIT 1`))[0];
    if (!saved) return legacy;
    const network = JSON.parse(String(saved.network_json)) as StreetcarNetwork;
    const method = JSON.parse(String(saved.method_json)) as StreetcarData['method'];
    const dates = (await query(`SELECT MIN(date)::VARCHAR AS first,MAX(date)::VARCHAR AS last FROM ${DATABASE_NAME}.streetcar_quality WHERE network_version=${q(network.version)} AND method=${q(method.name)}`))[0];
    if (!dates?.first || !dates.last) return legacy;
    const to = String(dates.last), from = [String(dates.first), new Date(Date.parse(to+'T00:00:00Z')-27*86400000).toISOString().slice(0,10)].sort().at(-1)!;
    legacy.streetcars = {}; legacy.streetcar_priority = {};
    for (const corridor of network.corridors) {
      try {
        const diagnosticFrom = [from, new Date(Date.parse(to+'T00:00:00Z')-6*86400000).toISOString().slice(0,10)].sort().at(-1)!;
        const sql = streetcarQueries(database, network.version, corridor.id, diagnosticFrom, to, method.name);
        const bins = decodeStreetcarBins(await query(sql.bins), method.timestamp_quantization_seconds ?? 60);
        const site_bins = decodeStreetcarSiteBins(await query(sql.site_bins), method.timestamp_quantization_seconds ?? 60);
        const rawQuality = await query(sql.quality), quality = decodeStreetcarQuality(rawQuality);
        const updated_at = rawQuality.map(row=>String(row.updated_at)).sort().at(-1) ?? null;
        legacy.streetcars[corridor.id] = {status: quality.length ? 'ready' : 'not_ready', network, method, bins, site_bins, quality,
          available_from:diagnosticFrom,available_to:to,selected_from:diagnosticFrom,selected_to:to,updated_at};
      } catch { legacy.errors![`streetcars:${corridor.id}`] = 'Archived speed diagnostics are not ready.'; }
      try {
        const filters = {day_type:'all' as const,hour_from:0,hour_to:23};
        const stats = decodePriorityStats(await query(priorityStatsSql(database,network.version,corridor.id,from,to,filters)));
        const waits: PriorityWaitData = {status:'collecting',snapshots:0,from:null,to:null,events:0,signal_only_seconds:0,mixed_seconds:0,stop_only_seconds:0,sites:[],clock:'collector_receipt'};
        try {
          const where = `corridor=${q(corridor.id)} AND date BETWEEN ${q(from)}::DATE AND ${q(to)}::DATE`;
          const coverage = (await query(`SELECT COALESCE(SUM(snapshots),0)::INTEGER AS snapshots,to_timestamp(MIN(first_at))::VARCHAR AS first,to_timestamp(MAX(last_at))::VARCHAR AS last FROM ${DATABASE_NAME}.streetcar_wait_quality WHERE ${where}`))[0];
          waits.snapshots=Number(coverage?.snapshots ?? 0); waits.from=coverage?.first==null?null:String(coverage.first); waits.to=coverage?.last==null?null:String(coverage.last); waits.status=waits.snapshots?'ready':'collecting';
          const rows = await query(`SELECT site_id,context,COUNT(*)::INTEGER AS events,SUM(duration_seconds) AS total_seconds,AVG(duration_seconds) AS mean_seconds FROM ${DATABASE_NAME}.streetcar_waits WHERE ${where} AND network_version=${q(network.version)} AND method=${q(PRIORITY_WAIT_METHOD)} GROUP BY site_id,context ORDER BY total_seconds DESC`);
          waits.sites=rows.map(row=>({site_id:String(row.site_id),name:network.sites.find(site=>site.id===row.site_id)?.name??String(row.site_id),context:row.context as 'signal_only'|'stop_only'|'both',events:Number(row.events),total_seconds:Number(row.total_seconds),mean_seconds:Number(row.mean_seconds)}));
          for (const row of waits.sites) { waits.events+=row.events; if(row.context==='signal_only')waits.signal_only_seconds+=row.total_seconds; else if(row.context==='both')waits.mixed_seconds+=row.total_seconds; else waits.stop_only_seconds+=row.total_seconds; }
        } catch { /* Older snapshots can legitimately predate the receipt ledger. */ }
        const updated = (await query(`SELECT MAX(updated_at)::VARCHAR AS at,COUNT(*)::INTEGER AS days FROM ${DATABASE_NAME}.streetcar_priority_days WHERE applied_version=${q(network.version+':'+PRIORITY_METHOD.name)} AND date BETWEEN ${q(from)}::DATE AND ${q(to)}::DATE`))[0];
        const payload:PriorityData = {status:stats.length?'ready':'not_ready',network,profiles:priorityProfiles(network,corridor.id,stats),waits,
          available_from:from,available_to:to,selected_from:from,selected_to:to,updated_at:updated?.at==null?null:String(updated.at),
          processed_days:Number(updated?.days??0),selected_days:1+Math.round((Date.parse(to)-Date.parse(from))/86400000),method:PRIORITY_METHOD,snapshot_only:true};
        legacy.streetcar_priority[corridor.id]=payload;
      } catch { legacy.errors![`streetcar_priority:${corridor.id}`] = 'Archived priority scenarios are not ready.'; }
    }
  } catch { legacy.errors!.streetcars = 'Archived streetcar diagnostics are not ready.'; }
  return legacy;
}
