import { TRANSIT_STUDY_METHOD, STUDY_LIMITATIONS } from './transit-study';
import type { RowStudyData, SignalStudyData, StudyCatalog, StudyDimensions, StudyEncounter, StudyFilters, StudyPassage,
  StudyQuality, StudyRowCell, StudySignalCell } from './transit-study-types';

import { compareRowCells, summarizeSignalCells } from '../dashboard/src/transit-study-filter';
export { compareRowCells, summarizeSignalCells, ROW_MIN_PASSAGES, ROW_MIN_DATES, SIGNAL_MIN_ENCOUNTERS, SIGNAL_MIN_DATES } from '../dashboard/src/transit-study-filter';
const sum = <T>(rows: T[], value: (row: T) => number) => rows.reduce((total, row) => total + value(row), 0);
function groups<T>(rows: T[], key: (row: T) => unknown[]): T[][] {
  const map = new Map<string, T[]>();
  for (const row of rows) { const id = JSON.stringify(key(row)), list = map.get(id) ?? []; list.push(row); map.set(id, list); }
  return [...map.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, rows]) => rows);
}
export function validateStudyFilters(filters: StudyFilters): void {
  const date = (value?: string) => !value || /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(`${value}T12:00:00Z`)) &&
    new Date(`${value}T12:00:00Z`).toISOString().slice(0, 10) === value;
  if (!date(filters.from) || !date(filters.to) || filters.from && filters.to && filters.from > filters.to ||
      filters.hour_from != null && (!Number.isInteger(filters.hour_from) || filters.hour_from < 0 || filters.hour_from > 23) ||
      filters.hour_to != null && (!Number.isInteger(filters.hour_to) || filters.hour_to < 0 || filters.hour_to > 23) ||
      (filters.hour_from ?? 0) > (filters.hour_to ?? 23) || filters.source && !['sse', 'lepass'].includes(filters.source) ||
      filters.mode && !['bus', 'streetcar'].includes(filters.mode) || filters.day_type && !['weekday', 'weekend'].includes(filters.day_type))
    throw new Error('Invalid transit study filters');
}
function selected(row: StudyDimensions, filters: StudyFilters): boolean {
  return (!filters.from || row.date >= filters.from) && (!filters.to || row.date <= filters.to) &&
    (!filters.source || row.source === filters.source) && (!filters.mode || row.mode === filters.mode) &&
    (!filters.route_id || row.route_id === filters.route_id) && (!filters.direction_id || row.direction_id === filters.direction_id) &&
    (!filters.day_type || row.day_type === filters.day_type) && row.hour >= (filters.hour_from ?? 0) && row.hour <= (filters.hour_to ?? 23);
}
function dimensions(row: StudyDimensions): StudyDimensions {
  return { date: row.date, source: row.source, mode: row.mode, route_id: row.route_id, direction_id: row.direction_id,
    path_id: row.path_id, hour: row.hour, day_type: row.day_type, time_band: row.time_band };
}
function dimensionKey(row: StudyDimensions): unknown[] {
  return [row.source, row.date, row.mode, row.route_id, row.direction_id, row.path_id, row.hour, row.day_type, row.time_band];
}
function unique<T extends { id: string; network_version: string; method: string }>(catalog: StudyCatalog, rows: T[]): T[] {
  return [...new Map(rows.filter(r => r.network_version === catalog.version && r.method === TRANSIT_STUDY_METHOD).map(r => [r.id, r])).values()];
}
export function rowStudyCells(catalog: StudyCatalog, passages: StudyPassage[], filters: StudyFilters = {}): StudyRowCell[] {
  validateStudyFilters(filters);
  const rows = unique(catalog, passages).filter(p => selected(p, filters));
  return groups(rows, p => [...dimensionKey(p), p.row_class, p.context, p.signal_ids.length, p.stop_ids.length]).map(group => {
    const p = group[0];
    return { ...dimensions(p), row_class: p.row_class, context: p.context, signal_count: p.signal_ids.length, stop_count: p.stop_ids.length,
      passages: group.length, distance_meters: sum(group, p => p.distance_meters), duration_seconds: sum(group, p => p.duration_seconds),
      duration_lower_seconds: sum(group, p => p.duration_lower_seconds), duration_upper_seconds: sum(group, p => p.duration_upper_seconds),
      window_ids: [...new Set(group.map(p => p.window_id))].sort(), run_ids: [...new Set(group.map(p => p.run_id))].sort() };
  });
}
export function signalStudyCells(catalog: StudyCatalog, encounters: StudyEncounter[], filters: StudyFilters = {}): StudySignalCell[] {
  validateStudyFilters(filters);
  return groups(unique(catalog, encounters).filter(e => selected(e, filters)), e => [...dimensionKey(e), e.site_id, e.approach_id, e.context, e.control_verified]).map(group => {
    const e = group[0];
    return { ...dimensions(e), site_id: e.site_id, approach_id: e.approach_id, context: e.context, control_verified: e.control_verified,
      encounters: group.length, evaluable_encounters: group.filter(e => e.wait_status !== 'insufficient_sampling').length,
      detected_wait_encounters: group.filter(e => e.wait_status === 'detected').length, wait_events: sum(group, e => e.wait_events),
      wait_seconds: sum(group, e => e.wait_seconds), wait_lower_seconds: sum(group, e => e.wait_lower_seconds), wait_upper_seconds: sum(group, e => e.wait_upper_seconds),
      run_ids: [...new Set(group.map(e => e.run_id))].sort() };
  });
}
function envelope(catalog: StudyCatalog, dates: string[], quality: StudyQuality[], filters: StudyFilters) {
  const selectedQuality = quality.filter(q => (!filters.from || q.date >= filters.from) && (!filters.to || q.date <= filters.to) && (!filters.source || q.source === filters.source));
  const sorted = [...new Set([...dates, ...selectedQuality.map(q => q.date)])].sort();
  return { from: filters.from ?? sorted[0] ?? null, to: filters.to ?? sorted.at(-1) ?? null, network: catalog,
    quality: selectedQuality, method: TRANSIT_STUDY_METHOD, limitations: [...STUDY_LIMITATIONS, ...catalog.limitations,
      'Quality counts cover whole selected source dates. They are not restricted to the chosen route, direction or hour.',
      'Where local and server SSE recordings overlap, local receipts take precedence over continuous provider-time coverage with 90-second transition margins. Server snapshots supplement longer gaps; original records remain preserved.',
      'ROW readiness requires at least 30 passages in each class on seven common dates. Signal headlines require 30 evaluable encounters on seven dates per directional site/context.',
      '95% bootstrap intervals resample whole service dates (2,000 deterministic draws), retaining within-day dependence. They describe sampled-day variability separately from GPS timing bounds; seven dates is still a small sample.',
      'Matched ROW strata hold route, direction, day type, four-hour band, stop count and signal count constant. Roadway locations still differ in geometry, operating rules and demand, so comparisons do not identify causal ROW effects.'] };
}
export function summarizeRowStudy(catalog: StudyCatalog, passages: StudyPassage[], quality: StudyQuality[] = [], filters: StudyFilters = {}): RowStudyData {
  const cells = rowStudyCells(catalog, passages, filters), comparisons = compareRowCells(cells);
  return { ...envelope(catalog, cells.map(c => c.date), quality, filters), status: comparisons.some(c => c.status === 'ready') ? 'ready' : 'collecting',
    cells, comparisons, coverage: { passages: sum(cells, c => c.passages), classified_passages: sum(cells.filter(c => c.row_class !== 'unknown'), c => c.passages),
      unknown_passages: sum(cells.filter(c => c.row_class === 'unknown'), c => c.passages) } };
}
export function summarizeSignalStudy(catalog: StudyCatalog, encounters: StudyEncounter[], quality: StudyQuality[] = [], filters: StudyFilters = {}): SignalStudyData {
  const cells = signalStudyCells(catalog, encounters, filters), signals = summarizeSignalCells(cells);
  return { ...envelope(catalog, cells.map(c => c.date), quality, filters), status: signals.some(s => s.status === 'ready') ? 'ready' : 'collecting',
    cells, signals };
}
