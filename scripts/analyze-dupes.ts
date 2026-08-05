import 'dotenv/config';
import { initMotherDuck, runQuery, closeMotherDuck } from '../src/motherduck';

// Read-only analysis: how many rows in the last 7 days are duplicate
// (vid, timestamp) pairs? Run with: tsx scripts/analyze-dupes.ts

const DB = process.env.MOTHERDUCK_DATABASE || 'my_db';

const num = (v: unknown): number => (v == null ? 0 : Number(v));

async function main(): Promise<void> {
  if (!process.env.MOTHER_DUCK_API_KEY) {
    console.error('MOTHER_DUCK_API_KEY is required to run this analysis.');
    process.exit(1);
  }

  await initMotherDuck();

  const rows = await runQuery(`
    WITH recent AS (
      SELECT vid, timestamp
      FROM ${DB}.transit_data
      WHERE timestamp >= now() - INTERVAL 7 DAY
    )
    SELECT
      (SELECT COUNT(*) FROM recent) AS total_rows,
      (SELECT COUNT(*) FROM (SELECT DISTINCT vid, timestamp FROM recent)) AS distinct_pairs
  `);

  const total = num(rows[0]?.total_rows);
  const distinct = num(rows[0]?.distinct_pairs);
  const dupes = total - distinct;
  const dupePct = total > 0 ? (dupes / total) * 100 : 0;

  console.log('\nDuplicate analysis — last 7 days');
  console.log('================================');
  console.table([
    { metric: 'Total rows', value: total },
    { metric: 'Distinct (vid, timestamp) pairs', value: distinct },
    { metric: 'Duplicate rows', value: dupes },
    { metric: 'Duplicate %', value: `${dupePct.toFixed(2)}%` },
  ]);

  await closeMotherDuck();
}

main().catch((err) => {
  console.error('analyze-dupes failed:', (err as Error).message);
  process.exit(1);
});
