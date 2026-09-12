import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import type { DuckDBConnection } from '@duckdb/node-api';
import { ident, query, sql } from './local-store';
import { LEGACY_STUDY_DATE_SQL, SNAPSHOT_STUDY_DATE_SQL } from './local-worker-logic';

export type ServerHistoryTable = 'transit_data' | 'streetcar_snapshots';
export interface ServerHistoryFiles { transitDataFile: string; streetcarSnapshotsFile: string }
export interface ServerHistoryFileAudit { path: string; sha256: string; bytes: number }
export interface ServerHistoryTableAudit {
  source_rows: number; source_distinct_rows: number; source_duplicate_rows: number;
  target_rows_before: number; inserted_rows: number; replay_rows: number; target_rows_after: number;
  study_dates: string[]; rows_without_study_date: number;
}
export interface ServerHistoryImportAudit {
  schema_version: 1; completed_at: string;
  files: Record<ServerHistoryTable, ServerHistoryFileAudit>;
  transit_data: ServerHistoryTableAudit; streetcar_snapshots: ServerHistoryTableAudit;
}
export interface ServerHistoryImportProgress {
  phase: 'staged' | 'validated' | 'inserted' | 'verified'; table: ServerHistoryTable;
  source_rows: number; inserted_rows: number;
}
const DATE_SQL: Record<ServerHistoryTable, string> = {
  transit_data: LEGACY_STUDY_DATE_SQL, streetcar_snapshots: SNAPSHOT_STUDY_DATE_SQL,
};
async function fingerprint(path: string): Promise<ServerHistoryFileAudit> {
  const resolved = await realpath(path), info = await stat(resolved);
  if (!info.isFile()) throw new Error('Server history input must be an immutable Parquet file');
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(resolved)) hash.update(chunk);
  return { path: resolved, sha256: hash.digest('hex'), bytes: info.size };
}

/** Offline catchup only: caller pauses the local writer and supplies immutable
 * exports. Native staging bounds JavaScript memory; both inserts commit together.
 * Complete-row EXCEPT ALL retains raw duplicate multiplicity and richer records.
 * Snapshot identities must be unique and match every field on overlap. */
export async function importServerHistory(
  connection: DuckDBConnection, input: ServerHistoryFiles,
  onProgress?: (progress: ServerHistoryImportProgress) => void | Promise<void>,
): Promise<ServerHistoryImportAudit> {
  const names: ServerHistoryTable[] = ['transit_data', 'streetcar_snapshots'];
  const files: Record<ServerHistoryTable, ServerHistoryFileAudit> = {
    transit_data: await fingerprint(input.transitDataFile), streetcar_snapshots: await fingerprint(input.streetcarSnapshotsFile),
  };
  const token = randomUUID().replaceAll('-', '');
  const stages = new Map<ServerHistoryTable, { rows: string; pending: string; columns: string[] }>();
  const temporary: string[] = [];
  const audits = {} as Record<ServerHistoryTable, ServerHistoryTableAudit>;
  let transaction = false;
  const count = async (statement: string): Promise<number> => {
    const [{ n }] = await query<{ n: number }>(connection, `SELECT COUNT(*) AS n FROM (${statement})`);
    if (!Number.isSafeInteger(n)) throw new Error('Server history row count exceeds safe reporting range');
    return n;
  };
  const progress = async (phase: ServerHistoryImportProgress['phase'], table: ServerHistoryTable) => {
    await onProgress?.({ phase, table, source_rows: audits[table].source_rows, inserted_rows: audits[table].inserted_rows });
  };
  try {
    for (const table of names) {
      const sourceSchema = await query<{ column_name: string; column_type: string }>(connection, `DESCRIBE SELECT * FROM read_parquet(${sql(files[table].path)})`);
      const targetSchema = await query<{ column_name: string; column_type: string }>(connection, `DESCRIBE ${ident(table)}`);
      const sourceTypes = new Map(sourceSchema.map(c => [c.column_name, c.column_type]));
      if (sourceSchema.length !== targetSchema.length || targetSchema.some(c => sourceTypes.get(c.column_name) !== c.column_type))
        throw new Error(`Incompatible server history schema for ${table}; no fields were discarded or converted`);
      const columns = targetSchema.map(c => c.column_name), fields = columns.map(ident).join(',');
      const rows = `server_history_${token}_${table}`, pending = `${rows}_pending`;
      stages.set(table, { rows, pending, columns }); temporary.push(rows);
      await connection.run(`CREATE TEMP TABLE ${ident(rows)} AS SELECT ${fields} FROM read_parquet(${sql(files[table].path)})`);
      const sourceRows = await count(`SELECT 1 FROM ${ident(rows)}`);
      const distinct = await count(`SELECT DISTINCT ${fields} FROM ${ident(rows)}`);
      audits[table] = {
        source_rows: sourceRows, source_distinct_rows: distinct, source_duplicate_rows: sourceRows - distinct,
        target_rows_before: await count(`SELECT 1 FROM ${ident(table)}`), inserted_rows: 0, replay_rows: 0, target_rows_after: 0,
        study_dates: (await query<{ date: string }>(connection, `SELECT DISTINCT (${DATE_SQL[table]})::VARCHAR AS date
          FROM ${ident(rows)} WHERE (${DATE_SQL[table]}) IS NOT NULL ORDER BY date`)).map(r => r.date),
        rows_without_study_date: await count(`SELECT 1 FROM ${ident(rows)} WHERE (${DATE_SQL[table]}) IS NULL`),
      };
      await progress('staged', table);
    }
    const snapshots = stages.get('streetcar_snapshots')!;
    if (await count(`SELECT snapshot_id FROM ${ident(snapshots.rows)} GROUP BY snapshot_id
      HAVING snapshot_id IS NULL OR snapshot_id='' OR COUNT(*)>1`))
      throw new Error('Server snapshot export contains invalid or duplicate snapshot_id values');
    const sourceFields = snapshots.columns.map(column => `s.${ident(column)}`).join(',');
    const targetFields = snapshots.columns.map(column => `t.${ident(column)}`).join(',');
    if (await count(`SELECT 1 FROM ${ident(snapshots.rows)} s JOIN streetcar_snapshots t USING(snapshot_id)
      WHERE ROW(${sourceFields}) IS DISTINCT FROM ROW(${targetFields})`))
      throw new Error('Conflicting snapshot_id in server history; existing snapshots were not overwritten');

    for (const table of names) {
      const stage = stages.get(table)!, fields = stage.columns.map(ident).join(','), date = DATE_SQL[table];
      // Equal complete rows necessarily have equal derived dates. Restrict the
      // historical scan without dropping records whose date cannot be resolved.
      const overlap = `(${date}) IN (SELECT ${date} FROM ${ident(stage.rows)}) OR
        ((${date}) IS NULL AND EXISTS (SELECT 1 FROM ${ident(stage.rows)} WHERE (${date}) IS NULL))`;
      temporary.push(stage.pending);
      await connection.run(`CREATE TEMP TABLE ${ident(stage.pending)} AS
        SELECT ${fields} FROM ${ident(stage.rows)} EXCEPT ALL SELECT ${fields} FROM ${ident(table)} WHERE ${overlap}`);
      audits[table].inserted_rows = await count(`SELECT 1 FROM ${ident(stage.pending)}`);
      audits[table].replay_rows = audits[table].source_rows - audits[table].inserted_rows;
      await progress('validated', table);
    }
    await connection.run('BEGIN TRANSACTION'); transaction = true;
    for (const table of names) {
      const stage = stages.get(table)!, fields = stage.columns.map(ident).join(',');
      if (audits[table].inserted_rows) await connection.run(`INSERT INTO ${ident(table)} (${fields}) SELECT ${fields} FROM ${ident(stage.pending)}`);
      await progress('inserted', table);
    }
    for (const table of names) {
      const stage = stages.get(table)!, fields = stage.columns.map(ident).join(',');
      audits[table].target_rows_after = await count(`SELECT 1 FROM ${ident(table)}`);
      if (audits[table].target_rows_after !== audits[table].target_rows_before + audits[table].inserted_rows ||
        await count(`SELECT ${fields} FROM ${ident(stage.rows)} EXCEPT ALL SELECT ${fields} FROM ${ident(table)}`))
        throw new Error(`Server history readback differs for ${table}; import rolled back`);
      const after = await fingerprint(files[table].path);
      if (after.sha256 !== files[table].sha256 || after.bytes !== files[table].bytes)
        throw new Error('Server history export changed during import; import rolled back');
      await progress('verified', table);
    }
    await connection.run('COMMIT'); transaction = false;
    return { schema_version: 1, completed_at: new Date().toISOString(), files,
      transit_data: audits.transit_data, streetcar_snapshots: audits.streetcar_snapshots };
  } catch (error) {
    if (transaction) await connection.run('ROLLBACK');
    throw error;
  } finally {
    for (const table of temporary.reverse()) await connection.run(`DROP TABLE IF EXISTS ${ident(table)}`).catch(() => {});
  }
}
