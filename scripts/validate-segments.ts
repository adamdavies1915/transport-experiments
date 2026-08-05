import 'dotenv/config';
import { initMotherDuck, runQuery, closeMotherDuck } from '../src/motherduck';

// Read-only validation of Route-12 segment tagging over the last 7 days.
// Run with: tsx scripts/validate-segments.ts

const DB = process.env.MOTHERDUCK_DATABASE || 'my_db';

const num = (v: unknown): number => (v == null ? 0 : Number(v));

// Route-12, last 7 days. "In service" excludes the unassigned route marker
// (rt = 'U') and the no-trip marker (tatripid = 'N/A').
const RECENT_R12 = `
  route = '12'
  AND timestamp >= now() - INTERVAL 7 DAY
`;
const IN_SERVICE = `
  ${RECENT_R12}
  AND route <> 'U'
  AND trip_id IS NOT NULL
  AND trip_id <> 'N/A'
`;

async function main(): Promise<void> {
  if (!process.env.MOTHER_DUCK_API_KEY) {
    console.error('MOTHER_DUCK_API_KEY is required to run this analysis.');
    process.exit(1);
  }

  await initMotherDuck();

  // 1. Ping counts per segment_id.
  const bySegmentId = await runQuery(`
    SELECT segment_id, segment_name, COUNT(*) AS pings
    FROM ${DB}.transit_data
    WHERE ${RECENT_R12}
    GROUP BY segment_id, segment_name
    ORDER BY segment_id NULLS LAST
  `);

  console.log('\nRoute 12 pings per segment_id — last 7 days');
  console.log('==========================================');
  console.table(bySegmentId.map((r) => ({
    segment_id: r.segment_id ?? 'NULL',
    segment_name: r.segment_name ?? 'NULL',
    pings: num(r.pings),
  })));

  // 2. Ping counts per segment_type.
  const bySegmentType = await runQuery(`
    SELECT segment_type, COUNT(*) AS pings
    FROM ${DB}.transit_data
    WHERE ${RECENT_R12}
    GROUP BY segment_type
    ORDER BY pings DESC
  `);

  console.log('\nRoute 12 pings per segment_type — last 7 days');
  console.log('============================================');
  console.table(bySegmentType.map((r) => ({
    segment_type: r.segment_type ?? 'NULL',
    pings: num(r.pings),
  })));

  // 3. NULL-segment count and percentage (in-service only).
  const nullStats = await runQuery(`
    SELECT
      COUNT(*) AS total,
      COUNT(*) FILTER (WHERE segment_id IS NULL) AS null_segment
    FROM ${DB}.transit_data
    WHERE ${IN_SERVICE}
  `);
  const total = num(nullStats[0]?.total);
  const nullSeg = num(nullStats[0]?.null_segment);
  const nullPct = total > 0 ? (nullSeg / total) * 100 : 0;

  console.log('\nRoute 12 NULL-segment coverage (in-service only) — last 7 days');
  console.log('=============================================================');
  console.table([
    { metric: 'In-service pings', value: total },
    { metric: 'NULL-segment pings', value: nullSeg },
    { metric: 'NULL-segment %', value: `${nullPct.toFixed(2)}%` },
  ]);

  // 4. Coarse boundary check: where do NULL-segment pings cluster?
  const clusters = await runQuery(`
    SELECT
      ROUND(lat, 3) AS lat_bucket,
      ROUND(lon, 3) AS lon_bucket,
      COUNT(*) AS pings
    FROM ${DB}.transit_data
    WHERE ${IN_SERVICE}
      AND segment_id IS NULL
    GROUP BY lat_bucket, lon_bucket
    ORDER BY pings DESC
    LIMIT 25
  `);

  console.log('\nRoute 12 NULL-segment clusters (rounded lat/lon, top 25) — last 7 days');
  console.log('=====================================================================');
  console.table(clusters.map((r) => ({
    lat_bucket: r.lat_bucket,
    lon_bucket: r.lon_bucket,
    pings: num(r.pings),
  })));

  await closeMotherDuck();
}

main().catch((err) => {
  console.error('validate-segments failed:', (err as Error).message);
  process.exit(1);
});
