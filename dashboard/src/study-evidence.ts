import type { StudyRowComparison, StudySignalSummary, StudySource } from '../../src/transit-study-types';

/** One evidence view, without treating two feeds of the same vehicles as
 * independent samples. Select a whole source-specific estimate; never join
 * dates, counts, clocks or uncertainty intervals across feeds. */
export const EVIDENCE_SELECTION_METHOD = 'One source per result, chosen by readiness, observation dates and sample coverage. Delay magnitude does not affect selection.';

export const studySourceName = (source: StudySource): string => source === 'lepass' ? 'Le Pass' : 'SSE';
const sourceOrder = (source: StudySource): number => source === 'sse' ? 0 : 1;
const readiness = (row: {status: string}): number => row.status === 'ready' ? 1 : 0;

export function rowEvidenceKey(row: StudyRowComparison): string {
  return JSON.stringify([row.mode, row.route_id, row.direction_id, row.day_type,
    row.time_band, row.context, row.signal_count, row.stop_count]);
}

export function signalEvidenceKey(row: StudySignalSummary): string {
  return JSON.stringify([row.mode, row.route_id, row.direction_id, row.site_id, row.context]);
}

/** Sort by support, not by the largest observed roadway difference. */
export function rankRowEvidence(a: StudyRowComparison, b: StudyRowComparison): number {
  return readiness(b) - readiness(a)
    || b.matched_dates - a.matched_dates
    || Math.min(b.reserved_passages, b.shared_passages) - Math.min(a.reserved_passages, a.shared_passages)
    || sourceOrder(a.source) - sourceOrder(b.source)
    || rowEvidenceKey(a).localeCompare(rowEvidenceKey(b));
}

/** The evaluable fraction distinguishes dense measurements from a large
 * denominator that contains mostly unevaluable encounters. */
export function rankSignalEvidence(a: StudySignalSummary, b: StudySignalSummary): number {
  const coverage = (row: StudySignalSummary) => row.encounters > 0 ? row.evaluable_encounters / row.encounters : 0;
  return readiness(b) - readiness(a)
    || b.evaluable_dates - a.evaluable_dates
    || coverage(b) - coverage(a)
    || b.evaluable_encounters - a.evaluable_encounters
    || sourceOrder(a.source) - sourceOrder(b.source)
    || signalEvidenceKey(a).localeCompare(signalEvidenceKey(b));
}

function choose<T>(rows: T[], key: (row: T) => string, rank: (a: T, b: T) => number): T[] {
  const selected = new Map<string, T>();
  for (const row of rows) {
    const id = key(row), previous = selected.get(id);
    if (!previous || rank(row, previous) < 0) selected.set(id, row);
  }
  return [...selected.values()].sort(rank);
}

/** Route/direction/time/context stay separate. Each selected result retains
 * its original source, sample, readiness and uncertainty. */
export function selectRowEvidence(rows: StudyRowComparison[]): StudyRowComparison[] {
  return choose(rows, rowEvidenceKey, rankRowEvidence);
}

/** Sites and passenger-stop overlap stay separate. The returned encounter
 * counts must not be added into unique journeys or end-to-end delay. */
export function selectSignalEvidence(rows: StudySignalSummary[]): StudySignalSummary[] {
  return choose(rows, signalEvidenceKey, rankSignalEvidence);
}
