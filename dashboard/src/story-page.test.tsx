import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import StoryPage from './StoryPage';
import { rowCell, rowData, signalCell, signalData, studyNetwork } from './study-fixtures';
import type { StudyRowCell, StudySignalCell } from '../../src/transit-study-types';

const dates = Array.from({ length: 7 }, (_, index) => `2026-09-${String(index + 1).padStart(2, '0')}`);
const readyRows = (source: 'sse' | 'lepass', datesToUse = dates): StudyRowCell[] => datesToUse.flatMap(date => [rowCell({ date, source, row_class: 'reserved' }), rowCell({ date, source, row_class: 'shared', duration_seconds: 300 })]);
const readySignals = (patch: Partial<StudySignalCell> = {}): StudySignalCell[] => dates.map(date => signalCell({ date, ...patch }));

test('paired preliminary roadway figures are visible with uncertainty before readiness', () => {
  const html = renderToStaticMarkup(<StoryPage rowData={rowData(readyRows('sse', dates.slice(0, 4)))} signalData={signalData()} />);
  assert.match(html, /Preliminary figures/);
  assert.match(html, /1\.7 <span>min \/ km/);
  assert.match(html, /2\.5 <span>min \/ km/);
  assert.match(html, /Timing uncertainty is too wide to tell which roadway is quicker/);
  assert.ok(html.indexOf('Timing uncertainty is too wide') < html.indexOf('<details'));
  assert.match(html, /4 <span class="text-slate-400">\/ 7 needed/);
  assert.match(html, /A day-to-day confidence interval is not available/);
  assert.doesNotMatch(html, /Shared roadway took|0 seconds more/);
  assert.match(html, /No isolated signal has a usable sample yet/);
  assert.match(html, /Both feeds inform the story/);
});

test('one source supplies the whole roadway finding without pooling partial dates', () => {
  const cells = [...readyRows('sse', dates.slice(0, 4)), ...readyRows('lepass', dates.slice(3))];
  const html = renderToStaticMarkup(<StoryPage rowData={rowData(cells)} signalData={signalData()} />);
  assert.match(html, /Preliminary figures/);
  assert.doesNotMatch(html, /Shared roadway took/);
  assert.match(html, /40 reserved and 40 shared passages/);
  assert.doesNotMatch(html, /80 reserved/);
});

test('a ready result uses the stronger source and identifies the limited comparison', () => {
  const html = renderToStaticMarkup(<StoryPage rowData={rowData([...readyRows('sse', dates.slice(0, 4)), ...readyRows('lepass')])} signalData={signalData()} />);
  assert.match(html, /Observed time to travel one kilometre/);
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
  assert.match(html, /30 <span>sec \/ detected wait/);
  assert.match(html, /60 <span>sec \/ detected wait/);
  assert.match(html, /9 sec \/ pass/);
  assert.match(html, /18 sec \/ pass/);
  assert.doesNotMatch(html, /Boarding overlap|900000/);
  assert.match(html, /cannot yet say how much priority would recover/);
  assert.match(html, /SSE observations/);
  assert.match(html, /Le Pass observations/);
  assert.match(html, /42 of 70 complete passes had enough sampling/);
});

test('unmatched roadway and unevaluable signals remain missing instead of becoming zero estimates', () => {
  const html = renderToStaticMarkup(<StoryPage rowData={rowData([rowCell()])} signalData={signalData([signalCell({ evaluable_encounters: 0, wait_events: 0, wait_seconds: 0, detected_wait_encounters: 0 })])} />);
  assert.match(html, /No matched roadway sample yet/);
  assert.match(html, /none had enough sampling to evaluate waiting/);
  assert.doesNotMatch(html, /min \/ km|sec \/ detected wait|0 <span>waits detected/);
});

test('several support-ranked bus sites expose preliminary waits and preserve genuine zero detections', () => {
  const cells = dates.slice(0, 3).flatMap(date => [
    signalCell({ date, mode: 'bus', site_id: 'first', encounters: 10, evaluable_encounters: 10, wait_events: 0, wait_seconds: 0, detected_wait_encounters: 0 }),
    signalCell({ date, mode: 'bus', site_id: 'second', encounters: 8, evaluable_encounters: 8, wait_events: 2, wait_seconds: 100, detected_wait_encounters: 2 }),
    signalCell({ date, mode: 'bus', site_id: 'third', evaluable_encounters: 0, wait_events: 0, wait_seconds: 0, detected_wait_encounters: 0 }),
  ]);
  const signals = signalData(cells);
  signals.network = { ...studyNetwork, sites: ['first', 'second', 'third'].map(id => ({ ...studyNetwork.sites[0], id, name: `Signal ${id}` })) };
  const html = renderToStaticMarkup(<StoryPage rowData={rowData()} signalData={signals} />);
  assert.match(html, /0 <span>waits detected/);
  assert.match(html, /In 30 complete passes/);
  assert.match(html, /More sampled signals/);
  assert.match(html, /Signal second/); assert.match(html, /50 sec \/ detected wait/);
  assert.match(html, /Signal third/); assert.match(html, /Not measurable/);
  assert.match(html, /Preliminary/); assert.match(html, /Sep 1–Sep 3/);
  assert.doesNotMatch(html, /25% recovered|citywide savings/);
});

test('roadway figures attribute matched dates rather than the full archive window', () => {
  const data = rowData(readyRows('sse', dates.slice(0, 4)));
  data.from = '2026-08-01'; data.to = '2026-09-13';
  const html = renderToStaticMarkup(<StoryPage rowData={data} signalData={signalData()} />);
  const roadway = html.slice(html.indexOf('Observed time to travel'), html.indexOf('The intersections'));
  assert.match(roadway, /4 matched dates · Sep 1–Sep 4/);
  assert.doesNotMatch(roadway, /Aug 1/);
});

test('saved-analysis caution stays visible rather than hidden inside a disclosure', () => {
  const rows = { ...rowData(), snapshot: { generated_at: '2026-09-07T12:00:00Z', received_at: null, origin: 'disk' as const, stale: true, refresh_error: 'Unavailable' } };
  const html = renderToStaticMarkup(<StoryPage rowData={rows} signalData={signalData()} />);
  assert.match(html, /role="status"[^>]*>This is a saved analysis\. The next update is pending/);
  assert.ok(html.indexOf('This is a saved analysis') < html.indexOf('<details'));
});
