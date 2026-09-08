import type { DuckDBConnection } from '@duckdb/node-api';
import { query, sql } from './local-store';

export async function initializeLegacyBackfillState(c: DuckDBConnection): Promise<void> {
  await c.run(`CREATE TABLE IF NOT EXISTS local_legacy_study_days (
    date DATE PRIMARY KEY,source_revision VARCHAR,method_revision VARCHAR,
    completed_at TIMESTAMPTZ,attempts INTEGER,last_error VARCHAR)`);
}
export async function pendingLegacyStudyDays(c: DuckDBConnection, method: string) {
  return query<{ date: string; revision: string }>(c, `SELECT s.date::VARCHAR AS date,s.source_revision AS revision
    FROM study_dates s LEFT JOIN local_legacy_study_days l USING(date)
    WHERE l.date IS NULL OR l.source_revision<>s.source_revision OR l.method_revision<>${sql(method)}
      OR (l.completed_at IS NULL AND l.attempts<3)
    ORDER BY s.date DESC LIMIT 2`);
}
export async function finishLegacyStudyDay(c: DuckDBConnection, date: string, revision: string, method: string, error?: string) {
  await c.run(`INSERT INTO local_legacy_study_days VALUES (${sql(date)},${sql(revision)},${sql(method)},${error ? 'NULL' : 'now()'},${error ? 1 : 0},${sql(error)})
    ON CONFLICT(date) DO UPDATE SET source_revision=excluded.source_revision,method_revision=excluded.method_revision,
    completed_at=excluded.completed_at,last_error=excluded.last_error,attempts=CASE WHEN excluded.completed_at IS NOT NULL THEN 0
      WHEN local_legacy_study_days.source_revision=excluded.source_revision AND local_legacy_study_days.method_revision=excluded.method_revision
      THEN local_legacy_study_days.attempts+1 ELSE 1 END`);
}
export async function failedLegacyStudyDays(c: DuckDBConnection, method: string): Promise<string[]> {
  return (await query<{ date: string }>(c, `SELECT s.date::VARCHAR AS date FROM study_dates s JOIN local_legacy_study_days l USING(date)
    WHERE l.source_revision=s.source_revision AND l.method_revision=${sql(method)} AND l.completed_at IS NULL AND l.attempts>=3 ORDER BY s.date`))
    .map(r => r.date);
}
