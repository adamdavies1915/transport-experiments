import { test } from 'node:test';
import assert from 'node:assert/strict';
import { catchupFrom } from '../scripts/process-cloud-history';

test('cloud catchup overlaps the prior Chicago day across DST and retries from the initial date', () => {
  assert.equal(catchupFrom(undefined, '2026-09-12'), '2026-09-12');
  assert.equal(catchupFrom('2026-09-23T01:00:00Z', ''), '2026-09-21');
  assert.equal(catchupFrom('2026-11-02T05:30:00Z', ''), '2026-10-31');
  assert.throws(() => catchupFrom(undefined, '2026-02-30'));
  assert.throws(() => catchupFrom('invalid', '2026-09-12'));
});
