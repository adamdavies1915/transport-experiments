import type { DuckDBConnection } from '@duckdb/node-api';
import { query, ident } from './local-store';

const importedKeys: Record<string, string[]> = {
  streetcar_networks: ['version'],
  streetcar_bins: ['date', 'corridor', 'route', 'direction', 'hour', 'day_type', 'category'],
  streetcar_site_bins: ['site_id', 'date', 'corridor', 'route', 'direction', 'hour', 'day_type', 'category'],
  streetcar_quality: ['date', 'corridor'], streetcar_backfill_days: ['date'],
  streetcar_passages: ['date', 'run_id', 'window_id'], streetcar_priority_days: ['date'],
  streetcar_waits: ['id'], streetcar_wait_quality: ['date', 'corridor'],
};
/** CTAS/Parquet imports retain rows but omit keys. Restore only documented keys;
 * duplicates fail visibly rather than deleting or arbitrarily choosing history. */
export async function restoreImportedStudyKeys(c: DuckDBConnection): Promise<void> {
  const tables = new Set((await query<{ table_name: string }>(c,
    "SELECT table_name FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema='main' AND table_type='BASE TABLE'"))
    .map(r => r.table_name));
  const constraints = await query<{ table_name: string; constraint_column_names: string[] }>(c,
    "SELECT table_name,constraint_column_names FROM duckdb_constraints() WHERE database_name=current_database() AND constraint_type IN ('PRIMARY KEY','UNIQUE')");
  for (const [table, keys] of Object.entries(importedKeys)) {
    if (!tables.has(table) || constraints.some(c => c.table_name === table && JSON.stringify(c.constraint_column_names) === JSON.stringify(keys))) continue;
    await c.run(`CREATE UNIQUE INDEX IF NOT EXISTS ${ident('local_import_key_' + table)} ON ${ident(table)} (${keys.map(ident).join(',')})`);
  }
}
