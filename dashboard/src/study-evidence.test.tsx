import { test } from 'node:test';
import assert from 'node:assert/strict';
import { compareRowCells, summarizeSignalCells } from './transit-study-filter';
import { selectRowEvidence, selectSignalEvidence } from './study-evidence';
import { rowCell, signalCell } from './study-fixtures';
import type { StudySource } from '../../src/transit-study-types';

const dates = ['2026-09-01', '2026-09-02', '2026-09-03', '2026-09-04', '2026-09-07', '2026-09-08', '2026-09-09'];
const rowDays = (source: StudySource, selected = dates) => selected.flatMap(date => [
  rowCell({ source, date }), rowCell({ source, date, row_class: 'shared', duration_seconds: 400 }),
]);
const signalDays = (source: StudySource, selected = dates) => selected.map(date => signalCell({ source, date }));

test('both-feed view preserves one complete estimate including its sample and interval', () => {
  const rows = compareRowCells([...rowDays('sse'), ...rowDays('lepass')]);
  const chosen = selectRowEvidence(rows);
  assert.equal(chosen.length, 1);
  assert.strictEqual(chosen[0], rows.find(row => row.source === 'sse'));
  assert.equal(chosen[0].matched_dates, 7);
  assert.equal(chosen[0].reserved_passages, 70);
  const signals = summarizeSignalCells([...signalDays('sse'), ...signalDays('lepass')]);
  assert.deepEqual(selectSignalEvidence(signals), signals.filter(row => row.source === 'sse'));
  assert.equal(selectSignalEvidence(signals)[0].encounters, 70);
});

test('partial dates or opposing roadway classes across sources cannot manufacture readiness', () => {
  const partial = compareRowCells([...rowDays('sse', dates.slice(0, 4)), ...rowDays('lepass', dates.slice(3))]);
  const chosen = selectRowEvidence(partial)[0];
  assert.equal(chosen.status, 'insufficient_data');
  assert.equal(chosen.matched_dates, 4);
  assert.equal(chosen.shared_extra_seconds_per_km, null);
  const splitClasses = compareRowCells(dates.flatMap(date => [rowCell({ date }), rowCell({ source: 'lepass', date, row_class: 'shared' })]));
  assert.equal(selectRowEvidence(splitClasses)[0].matched_dates, 0);
  assert.equal(selectRowEvidence(splitClasses)[0].status, 'insufficient_data');
  const signals = selectSignalEvidence(summarizeSignalCells([
    ...signalDays('sse', dates.slice(0, 4)), ...signalDays('lepass', dates.slice(3)),
  ]));
  assert.equal(signals[0].evaluable_dates, 4);
  assert.equal(signals[0].status, 'insufficient_data');
});

test('coverage selection is independent of delay size and source input order', () => {
  const rows = compareRowCells([...rowDays('sse'), ...rowDays('lepass')]);
  const extreme = rows.map(row => row.source === 'lepass' ? { ...row, shared_extra_seconds_per_km: 999999 } : row);
  assert.equal(selectRowEvidence(extreme)[0].source, 'sse');
  assert.deepEqual(selectRowEvidence(extreme), selectRowEvidence([...extreme].reverse()));
  const signals = summarizeSignalCells([...signalDays('sse'), ...signalDays('lepass')]);
  const changed = signals.map(row => row.source === 'lepass' ? { ...row, detected_wait_seconds_per_encounter: 999999 } : row);
  assert.equal(selectSignalEvidence(changed)[0].source, 'sse');
  assert.deepEqual(selectSignalEvidence(changed), selectSignalEvidence([...changed].reverse()));
  assert.equal(selectRowEvidence(rows.filter(row => row.source === 'lepass'))[0].source, 'lepass');
});

test('Le Pass supplies a better supported result without replacing stronger SSE units', () => {
  const rows = compareRowCells([...rowDays('sse', dates.slice(0, 4)), ...rowDays('lepass')]);
  assert.equal(selectRowEvidence(rows)[0].source, 'lepass');
  const signals = summarizeSignalCells([...signalDays('sse', dates.slice(0, 4)), ...signalDays('lepass')]);
  assert.equal(selectSignalEvidence(signals)[0].source, 'lepass');
  const lepass = signals.find(row => row.source === 'lepass')!;
  const lowCoverage = { ...lepass, source: 'sse' as const, encounters: 10000 };
  assert.equal(selectSignalEvidence([lowCoverage, lepass])[0].source, 'lepass');
});

test('route, direction, hour context, mode and stop overlaps retain separate evidence', () => {
  const base = compareRowCells(rowDays('sse'))[0];
  const rows = [base, { ...base, source: 'lepass' as const }, { ...base, route_id: '47' },
    { ...base, direction_id: '1' }, { ...base, time_band: 3 }, { ...base, context: 'both' as const },
    { ...base, mode: 'bus' as const }, { ...base, signal_count: 1 }];
  assert.equal(selectRowEvidence(rows).length, 7);
  const signals = summarizeSignalCells([
    ...signalDays('sse'), ...signalDays('lepass'),
    ...signalDays('sse').map(cell => ({ ...cell, context: 'both' as const })),
    ...signalDays('sse').map(cell => ({ ...cell, direction_id: '1' })),
  ]);
  const selected = selectSignalEvidence(signals);
  assert.equal(selected.length, 3);
  assert.ok(selected.find(row => row.context === 'both')!.recovery_seconds_per_encounter.every(row => row.seconds === null));
});

test('unevaluable samples cannot replace evaluated evidence or turn missing waits into zero', () => {
  const missing = summarizeSignalCells(signalDays('sse').map(cell => ({ ...cell, evaluable_encounters: 0,
    detected_wait_encounters: 0, wait_events: 0, wait_seconds: 0 })));
  assert.equal(selectSignalEvidence(missing)[0].detected_wait_seconds_per_encounter, null);
  const evaluated = summarizeSignalCells(signalDays('lepass', dates.slice(0, 1)));
  assert.equal(selectSignalEvidence([...missing, ...evaluated])[0].source, 'lepass');
});
