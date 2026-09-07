import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import OtpPanel from './OtpPanel';
import type { OtpDay } from './otp-data';

const row: OtpDay = { date: '2026-09-07', route: '11', scheduled: 100,
  observed: 10, classified: 10, early: 2, on_time: 7, late: 1, uncertain: 0,
  block_events: 0, crosswalk_events: 0, observed_trips: 3, matched_trips: 2, block_matched_trips: 0,
  updated_at: '2026-09-07 18:00:00+00' };
test('missing schedule data never renders an invented OTP', () => {
  const html = renderToStaticMarkup(<OtpPanel data={{ status: 'not_ready', days: [] }} />);
  assert.match(html, /Schedule-based OTP is not available yet/);
  assert.doesNotMatch(html, /100\.0%/);
});
test('shows our result and coverage, without comparing to an unrelated RTA month', () => {
  const html = renderToStaticMarkup(<OtpPanel data={{ status: 'ready', days: [row] }} />);
  assert.match(html, /70\.0%/);
  assert.match(html, /10\.0%/);
  assert.match(html, /Low coverage/);
  assert.doesNotMatch(html, /80\.5%/);
});
test('zero measured events remain unavailable, while a measured zero OTP is preserved', () => {
  const empty = renderToStaticMarkup(<OtpPanel data={{ status: 'ready', days: [{ ...row, observed: 0, classified: 0, early: 0, on_time: 0, late: 0 }] }} />);
  assert.match(empty, /No reliably classified stop events/);
  assert.match(empty, /Unavailable/);
  const late = renderToStaticMarkup(<OtpPanel data={{ status: 'ready', days: [{ ...row, early: 0, on_time: 0, late: 10 }] }} />);
  assert.match(late, /0\.0%/);
  assert.doesNotMatch(late, /No reliably classified stop events/);
});
test('RTA benchmark appears only alongside the matching route and month', () => {
  const html = renderToStaticMarkup(<OtpPanel data={{ status: 'ready', days: [{ ...row, date: '2026-04-01' }] }} />);
  assert.match(html, /80\.5%/);
  assert.match(html, /-10\.5 pp/);
});
