/** Read retained daily research results without the public snapshot's size window. */
import { readFile, mkdir } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { DuckDBInstance } from '@duckdb/node-api';
import { atomicFile } from '../src/local-journal';
import { query, sql } from '../src/local-store';
import { compareRowCells } from '../src/transit-study-summary';
import { TRANSIT_STUDY_METHOD } from '../src/transit-study';
import type { StudyCatalog, StudyRowCell } from '../src/transit-study-types';

const { values } = parseArgs({ options: { 'data-dir': { type: 'string' }, from: { type: 'string' }, to: { type: 'string' }, output: { type: 'string' } }, strict: true });
for (const date of [values.from, values.to]) {
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date) || new Date(date).toISOString().slice(0, 10) !== date) throw new Error('--from and --to must be valid dates');
}
if (values.from! > values.to! || Date.parse(values.to!) - Date.parse(values.from!) > 89 * 86400000) throw new Error('Choose a range of at most 90 calendar days');
if (!values['data-dir'] || !values.output) throw new Error('--data-dir and --output are required; run after the database worker exits');
const catalog: StudyCatalog = JSON.parse(await readFile(new URL('../src/data/transit-study-network.json', import.meta.url), 'utf8'));
const db = await DuckDBInstance.create(join(resolve(values['data-dir']), 'transit.duckdb'), { access_mode: 'READ_ONLY', threads: '1', memory_limit: '1GB' });
try {
  const c = await db.connect();
  try {
    const dates = await query<{ date: string }>(c, `SELECT date::VARCHAR AS date FROM study_daily_results WHERE date BETWEEN ${sql(values.from)}::DATE AND ${sql(values.to)}::DATE AND method_revision=${sql(catalog.version + ':' + TRANSIT_STUDY_METHOD)} ORDER BY date`);
    const cells: StudyRowCell[] = [];
    for (const { date } of dates) {
      const [saved] = await query<{ body: string }>(c, `SELECT body::VARCHAR AS body FROM study_daily_results WHERE date=${sql(date)}::DATE`);
      const day = JSON.parse(saved.body) as { row_cells: StudyRowCell[] };
      cells.push(...day.row_cells.filter(cell => cell.route_id === '12' && cell.mode === 'streetcar'));
    }
    const comparisons = compareRowCells(cells);
    const report = {
      generated_at: new Date().toISOString(), from: values.from, to: values.to,
      basis: 'Full retained Route 12 daily results, independent of the bounded public snapshot.',
      catalog: catalog.version, available_dates: dates.map(d => d.date),
      coverage: ['sse', 'lepass'].map(source => ({ source,
        complete_passages: cells.filter(x => x.source === source).reduce((n, x) => n + x.passages, 0),
        reviewed_passages: cells.filter(x => x.source === source && x.row_class !== 'unknown').reduce((n, x) => n + x.passages, 0) })),
      primary: comparisons.filter(x => x.direction_id === '1' && x.day_type === 'weekday' && x.time_band === 2 && x.signal_count === 1 && x.stop_count === 2),
      comparisons,
      limitations: ['The sources remain separate and may share an upstream feed.',
        'Timing bounds and service-day bootstrap intervals measure different uncertainty; neither establishes a causal roadway effect.',
        'These are retained geographic study windows; current route changes and field validation remain under review.'],
    };
    const output = resolve(values.output); await mkdir(dirname(output), { recursive: true });
    await atomicFile(output, JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify({ output, dates: dates.length, ready_comparisons: comparisons.filter(x => x.status === 'ready').length }));
  } finally { c.closeSync(); }
} finally { db.closeSync(); }
