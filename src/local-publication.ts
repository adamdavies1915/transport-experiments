import type { RowStudyData, SignalStudyData, StudyRowCoverageCell } from './transit-study-types';

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
