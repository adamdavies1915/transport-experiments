import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, truncate, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { captureBudget } from './capture-budget';
import { diskBudget } from './local-journal';

test('capture budget leaves default workstation guard unchanged', async () => {
  assert.deepEqual(await captureBudget(tmpdir(), {})(), await diskBudget(tmpdir()));
});
test('capture budget validates explicit reserve and queue together', () => {
  assert.throws(() => captureBudget(tmpdir(), { CAPTURE_QUEUE_MAX_BYTES: '536870912' }));
  assert.throws(() => captureBudget(tmpdir(), { CAPTURE_QUEUE_MAX_BYTES: '536870912', CAPTURE_FREE_RESERVE_BYTES: '100' }));
});
test('capture budget counts sealed spool and reserves concurrent appends', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'capture-budget-'));
  const env = { CAPTURE_QUEUE_MAX_BYTES: '536870912', CAPTURE_FREE_RESERVE_BYTES: '2000000000' };
  try {
    await mkdir(join(dir, 'capture-exchange/bundles'), { recursive: true });
    const file = join(dir, 'capture-exchange/bundles/test');
    await writeFile(file, '');
    await truncate(file, 470 * 1024 * 1024);
    assert.equal((await captureBudget(dir, env)()).allowed, false);
    await truncate(file, 0);
    const budget = captureBudget(dir, env);
    const result = await Promise.all([budget(300 * 1024 * 1024), budget(300 * 1024 * 1024)]);
    assert.equal(result[1].allowed, false);
    assert.equal((await captureBudget(dir, { ...env, CAPTURE_FREE_RESERVE_BYTES: '9000000000000000' })()).allowed, false);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
