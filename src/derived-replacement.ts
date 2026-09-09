import type { DuckDBConnection } from '@duckdb/node-api';

const ident = (name: string) => `"${name.replaceAll('"', '""')}"`;
const literal = (value: string) => `'${value.replaceAll("'", "''")}'`;
export async function prepareDerivedStage(c: DuckDBConnection, table: string): Promise<string> {
  const stage = `derived_next_${table}`;
  await c.run(`CREATE OR REPLACE TEMP TABLE ${ident(stage)} AS SELECT * FROM ${ident(table)} WHERE false`);
  return stage;
}
/** Caller owns the transaction. Preserve reused keys, update their computed
 * values, and delete only obsolete keys. Some imported ART indexes reject a
 * delete/reinsert of a reused key even after SELECT sees the row as deleted. */
export async function replaceDerivedDay(c: DuckDBConnection, args: {
  table: string; stage: string; date_column: string; date: string; keys: string[];
}): Promise<void> {
  const { table, stage, date_column, date, keys } = args;
  const columns = (await c.runAndReadAll(`DESCRIBE ${ident(table)}`)).getRowObjectsJS().map(r => String(r.column_name));
  if (!keys.length || !keys.every(k => columns.includes(k)) || !columns.includes(date_column)) throw new Error('Invalid derived replacement keys');
  const readCount = async (sql: string) => Number((await c.runAndReadAll(sql)).getRowObjectsJS()[0].n);
  if (await readCount(`SELECT COUNT(*) AS n FROM ${ident(stage)} WHERE ${ident(date_column)} IS DISTINCT FROM ${literal(date)}::DATE OR ${keys.map(k => `${ident(k)} IS NULL`).join(' OR ')}`))
    throw new Error(`Invalid partition or null key in staged ${table}`);
  if (await readCount(`SELECT COUNT(*) AS n FROM (SELECT ${keys.map(ident).join(',')} FROM ${ident(stage)} GROUP BY ${keys.map(ident).join(',')} HAVING COUNT(*)>1)`))
    throw new Error(`Duplicate derived identity in staged ${table}; original partition retained`);
  if (await readCount(`SELECT COUNT(*) AS n FROM ${ident(table)} AS old JOIN ${ident(stage)} AS next
    ON ${keys.map(k => `old.${ident(k)}=next.${ident(k)}`).join(' AND ')} WHERE old.${ident(date_column)} IS DISTINCT FROM ${literal(date)}::DATE`))
    throw new Error(`Derived identity belongs to a different date in ${table}; original partitions retained`);
  const nonKeys = columns.filter(column => !keys.includes(column));
  if (!nonKeys.length) throw new Error('Derived replacement requires non-key fields');
  await c.run(`INSERT INTO ${ident(table)} SELECT * FROM ${ident(stage)} ON CONFLICT (${keys.map(ident).join(',')}) DO UPDATE SET ${nonKeys.map(k => `${ident(k)}=excluded.${ident(k)}`).join(',')}`);
  await c.run(`DELETE FROM ${ident(table)} AS old WHERE old.${ident(date_column)}=${literal(date)}::DATE AND NOT EXISTS
    (SELECT 1 FROM ${ident(stage)} AS next WHERE ${keys.map(k => `old.${ident(k)}=next.${ident(k)}`).join(' AND ')})`);
  const stored = `SELECT * FROM ${ident(table)} WHERE ${ident(date_column)}=${literal(date)}::DATE`, expected = `SELECT * FROM ${ident(stage)}`;
  if (await readCount(`SELECT COUNT(*) AS n FROM ((${stored} EXCEPT ALL ${expected}) UNION ALL (${expected} EXCEPT ALL ${stored}))`))
    throw new Error(`Derived replacement readback differs for ${table}`);
}
