import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import StoryPage from './StoryPage';
import { rowCell, rowData, signalCell, signalData, studyNetwork } from './study-fixtures';
import type { StudyRowCell, StudySignalCell } from '../../src/transit-study-types';

const dates = Array.from({ length: 7 }, (_, index) => `2026-09-${String(index + 1).padStart(2, '0')}`);
const readyRows = (source: 'sse' | 'lepass', datesToUse = dates): StudyRowCell[] => datesToUse.flatMap(date => [rowCell({ date, source, row_class: 'reserved' }), rowCell({ date, source, row_class: 'shared', duration_seconds: 300 })]);
const readySignals = (patch: Partial<StudySignalCell> = {}): StudySignalCell[] => dates.map(date => signalCell({ date, ...patch }));

test('unready roadway samples remain evidence progress without a delay headline', () => {
  const html = renderToStaticMarkup(<StoryPage rowData={rowData(readyRows('sse', dates.slice(0, 4)))} signalData={signalData()} />);
  assert.match(html, /We cannot put a reliable number on it yet/);
  assert.match(html, /4 <span class="text-slate-400">\/ 7 needed/);
  assert.match(html, /Unreviewed roadway is excluded/);
  assert.doesNotMatch(html, /Shared roadway took|sec \/ pass|0 seconds more/);
  assert.match(html, /No isolated signal has a usable sample yet/);
  assert.match(html, /Both feeds inform the story/);
});

test('one source supplies the whole roadway finding without pooling partial dates', () => {
  const cells = [...readyRows('sse', dates.slice(0, 4)), ...readyRows('lepass', dates.slice(3))];
  const html = renderToStaticMarkup(<StoryPage rowData={rowData(cells)} signalData={signalData()} />);
  assert.match(html, /We cannot put a reliable number on it yet/);
  assert.doesNotMatch(html, /Shared roadway took/);
  assert.match(html, /40 reserved and 40 shared passages/);
  assert.doesNotMatch(html, /80 reserved/);
});

test('a ready result uses the stronger source and identifies the limited comparison', () => {
  const html = renderToStaticMarkup(<StoryPage rowData={rowData([...readyRows('sse', dates.slice(0, 4)), ...readyRows('lepass')])} signalData={signalData()} />);
  assert.match(html, /Shared roadway took/);
  assert.match(html, /Le Pass observations/);
  assert.match(html, /Route 12 · St. Charles outbound/);
  assert.match(html, /One matched comparison, not a network average/);
  assert.match(html, /does not measure what changing their roadway would save/);
  assert.doesNotMatch(html, /citywide savings|110 reserved/);
});

test('isolated streetcar and bus signal findings remain separate and cannot use boarding overlaps', () => {
  const signals = signalData([
    ...readySignals(),
    ...readySignals({ source: 'lepass', mode: 'bus', route_id: '5', wait_seconds: 180 }),
    ...readySignals({ site_id: 'boarding', context: 'both', encounters: 1000, evaluable_encounters: 1000, wait_seconds: 900000 }),
  ]);
  signals.network = { ...studyNetwork, sites: [...studyNetwork.sites, { ...studyNetwork.sites[0], id: 'boarding', name: 'Boarding overlap' }] };
  const html = renderToStaticMarkup(<StoryPage rowData={rowData()} signalData={signals} />);
  assert.match(html, />Streetcars<\/h4>/);
  assert.match(html, />Buses<\/h4>/);
  assert.match(html, /9 <span>sec \/ pass/);
  assert.match(html, /18 <span>sec \/ pass/);
  assert.doesNotMatch(html, /Boarding overlap|900000/);
  assert.match(html, /cannot yet say how much priority would recover/);
  assert.match(html, /SSE observations/);
  assert.match(html, /Le Pass observations/);
  assert.match(html, /42 of 70 complete passes had enough sampling/);
});

test('saved-analysis caution stays visible rather than hidden inside a disclosure', () => {
  const rows = { ...rowData(), snapshot: { generated_at: '2026-09-07T12:00:00Z', received_at: null, origin: 'disk' as const, stale: true, refresh_error: 'Unavailable' } };
  const html = renderToStaticMarkup(<StoryPage rowData={rows} signalData={signalData()} />);
  assert.match(html, /role="status"[^>]*>This is a saved analysis\. The next update is pending/);
  assert.ok(html.indexOf('This is a saved analysis') < html.indexOf('<details'));
});
