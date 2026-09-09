import type { RowStudyData, SignalStudyData, StudyRowCoverageCell, StudyRowCell, StudySignalCell, StudyQuality } from './transit-study-types';
import type { TransitSummaryEnvelope } from '../dashboard/src/summary-data';
import { rowStudyFromCells, signalStudyFromCells } from '../dashboard/src/transit-study-filter';
import { atomicFile } from './local-journal';

export const PUBLIC_SUMMARY_MAX_BYTES = 60 * 1024 * 1024;
const WINDOW_NOTE = 'Public study window is limited';
export interface PublicationDay { row_cells: StudyRowCell[]; signal_cells: StudySignalCell[]; quality: StudyQuality[] }

/** Consume newest dates sequentially. Full daily JSON and identity lists are
 * released after compaction; retained public cells have a separate byte ceiling.
 * One oversized newest day is passed to the exact envelope guard, which fails
 * before publication. The first older day that cannot fit ends the window. */
export async function collectCompactStudyDays(dates: string[], read: (date: string) => Promise<PublicationDay>,
  rowBase: RowStudyData, signalBase: SignalStudyData, maxBytes = PUBLIC_SUMMARY_MAX_BYTES) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) throw new Error('Invalid publication byte budget');
  const row_cells: StudyRowCell[] = [], coverage_cells: StudyRowCoverageCell[] = [], signal_cells: StudySignalCell[] = [], quality: StudyQuality[] = [];
  const retained_dates: string[] = [];
  let bytes = 0;
  for (const date of [...new Set(dates)].sort().reverse()) {
    const day = await read(date);
    const compact = compactStudyPublication({ ...rowBase, cells: day.row_cells }, { ...signalBase, cells: day.signal_cells });
    const next = { row_cells: compact.row.cells, coverage_cells: compact.row.coverage_cells ?? [], signal_cells: compact.signal.cells, quality: day.quality };
    const cost = Buffer.byteLength(JSON.stringify(next));
    if (retained_dates.length && bytes + cost > maxBytes) break;
    for (const cell of next.row_cells) row_cells.push(cell);
    for (const cell of next.coverage_cells) coverage_cells.push(cell);
    for (const cell of next.signal_cells) signal_cells.push(cell);
    for (const record of next.quality) quality.push(record);
    retained_dates.push(date); bytes += cost;
    if (bytes > maxBytes) break;
  }
  // Preserve the previous publisher's date order and therefore deterministic JSON.
  const byDate = <T extends { date: string }>(rows: T[]) => rows.sort((a, b) => a.date.localeCompare(b.date));
  return { row_cells: byDate(row_cells), coverage_cells: byDate(coverage_cells), signal_cells: byDate(signal_cells), quality: byDate(quality), retained_dates };
}

/** Exact UTF-8 envelope ceiling, including legacy metrics and catalog copies.
 * Prune only whole oldest study dates; keep all matching strata for retained
 * dates and recompute estimates/denominators. No file is touched by this guard. */
export function serializeBoundedStudyPublication(envelope: TransitSummaryEnvelope, options: {
  max_bytes?: number; max_dates?: number; available_dates?: string[]; included_dates?: string[];
} = {}): { body: string; bytes: number; retained_dates: string[]; omitted_dates: number } {
  const maxBytes = options.max_bytes ?? PUBLIC_SUMMARY_MAX_BYTES, maxDates = options.max_dates ?? 90;
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0 || !Number.isSafeInteger(maxDates) || maxDates <= 0) throw new Error('Invalid publication budget');
  const dates = [...new Set([
    ...(options.included_dates ?? []),
    ...(envelope.row_study?.cells.map(c => c.date) ?? []), ...(envelope.row_study?.coverage_cells?.map(c => c.date) ?? []),
    ...(envelope.signal_study?.cells.map(c => c.date) ?? []), ...(envelope.row_study?.quality.map(q => q.date) ?? []), ...(envelope.signal_study?.quality.map(q => q.date) ?? []),
  ])].sort();
  const available = [...new Set([...dates, ...(options.available_dates ?? [])])].sort();
  const retained = dates.slice(-maxDates);
  for (;;) {
    const omitted = available.length - retained.length;
    let candidate = envelope;
    if (omitted > 0) {
      const from = retained[0] ?? null, to = retained.at(-1) ?? null;
      const note = `${WINDOW_NOTE} to ${from ?? 'no dates'} through ${to ?? 'no dates'} (${retained.length} whole dates) by the publication budget. Stored study dates remain available from ${available[0]} through ${available.at(-1)}; ${omitted} older dates are omitted from this snapshot. Full local results are unchanged.`;
      const filters = { from: from ?? '9999-12-31', to: to ?? '9999-12-31' };
      const row = envelope.row_study ? rowStudyFromCells(envelope.row_study, filters) : undefined;
      const signal = envelope.signal_study ? signalStudyFromCells(envelope.signal_study, filters) : undefined;
      candidate = { ...envelope,
        row_study: row && { ...row, from, to, limitations: [...row.limitations.filter(n => !n.startsWith(WINDOW_NOTE)), note] },
        signal_study: signal && { ...signal, from, to, limitations: [...signal.limitations.filter(n => !n.startsWith(WINDOW_NOTE)), note] } };
    }
    const body = JSON.stringify(candidate), bytes = Buffer.byteLength(body);
    if (bytes <= maxBytes) return { body, bytes, retained_dates: [...retained], omitted_dates: omitted };
    if (retained.length <= 1) throw new Error(`Public summary exceeds ${maxBytes} bytes with ${retained.length ? 'the newest whole study date' : 'no study dates'}; previous snapshot retained`);
    retained.shift();
  }
}

export async function writeBoundedStudyPublication(path: string, envelope: TransitSummaryEnvelope,
  options: Parameters<typeof serializeBoundedStudyPublication>[1] = {}) {
  const { body, ...metadata } = serializeBoundedStudyPublication(envelope, options);
  await atomicFile(path, body);
  return metadata;
}

const NOTE = 'Public unknown-ROW coverage is grouped by source, date, mode, route, direction and hour; path and exposure details remain in local daily results. Public cells omit run/window identity lists. No unknown coverage enters roadway comparisons.';
/** Compact only information unused by public comparisons. Full local daily
 * cells and detailed events remain unchanged for future evidence review. */
export function compactStudyPublication(row: RowStudyData, signal: SignalStudyData): { row: RowStudyData; signal: SignalStudyData } {
  const coverage = new Map<string, StudyRowCoverageCell>();
  for (const cell of [...row.cells.filter(c => c.row_class === 'unknown'), ...(row.coverage_cells ?? [])]) {
    const key = JSON.stringify([cell.source, cell.date, cell.mode, cell.route_id, cell.direction_id, cell.hour, cell.day_type, cell.time_band]);
    const saved = coverage.get(key) ?? { source: cell.source, date: cell.date, mode: cell.mode, route_id: cell.route_id,
      direction_id: cell.direction_id, hour: cell.hour, day_type: cell.day_type, time_band: cell.time_band, passages: 0 };
    saved.passages += cell.passages; coverage.set(key, saved);
  }
  const coverage_cells = [...coverage].sort(([a], [b]) => a.localeCompare(b)).map(([, cell]) => cell);
  const cells = row.cells.filter(c => c.row_class !== 'unknown').map(({ run_ids: _runs, window_ids: _windows, ...cell }) => cell);
  const classified = cells.reduce((n, c) => n + c.passages, 0), unknown = coverage_cells.reduce((n, c) => n + c.passages, 0);
  return {
    row: { ...row, cells, coverage_cells, coverage: { passages: classified + unknown, classified_passages: classified, unknown_passages: unknown },
      limitations: [...new Set([...row.limitations, NOTE])] },
    signal: { ...signal, cells: signal.cells.map(({ run_ids: _runs, ...cell }) => cell) },
  };
}
