import { createHash, randomUUID } from 'node:crypto';
import type { DuckDBConnection } from '@duckdb/node-api';
import type { CollectionBatch, StudyObservation } from './observation-types';
import { ident, ingestBatch, query, sql } from './local-store';

export interface CollectionMergeProgress {
  phase: 'preflight' | 'merge' | 'verify';
  processed_batches: number; total_batches: number; inserted_batches: number;
}
export interface CollectionMergeResult {
  source_batches: number; source_payloads: number; source_receipts: number;
  inserted_batches: number; skipped_batches: number;
}
const TABLES = {
  collection_payloads: { key: 'payload_id', columns: ['payload_id', 'source', 'vehicle_id', 'route_id', 'observed_at', 'body', 'raw'] },
  collection_receipts: { key: 'observation_id', columns: ['observation_id', 'payload_id', 'received_at', 'service_date', 'batch_id'] },
  collection_batches: { key: 'batch_id', columns: ['batch_id', 'source', 'received_at', 'observations', 'predictions', 'provenance'] },
};
interface SavedBatch {
  batch_id: string; source: StudyObservation['source']; received_at: string;
  observations: number; predictions: string; provenance: string;
}
interface SavedObservation {
  batch_id: string; observation_id: string; payload_id: string; received_at: number; body: string; raw: string;
}
function object(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** Offline only: the caller must stop both database writers before calling.
 * Each batch commits through ingestBatch, making interrupted merges resumable.
 * Compacted sources require a separate archive migration and are rejected. */
export async function mergeCollectedHistory(
  target: DuckDBConnection, sourceDatabaseFile: string,
  onProgress?: (progress: CollectionMergeProgress) => void | Promise<void>,
): Promise<CollectionMergeResult> {
  const alias = `collected_merge_${randomUUID().replaceAll('-', '')}`;
  const source = (table: string) => `${ident(alias)}.main.${ident(table)}`;
  const [{ database }] = await query<{ database: string }>(target, 'SELECT current_database() AS database');
  const destination = (table: string) => `${ident(database)}.main.${ident(table)}`;
  const count = async (statement: string) => (await query<{ n: number }>(target, `SELECT COUNT(*) AS n FROM (${statement})`))[0].n;
  const rejectRows = async (statement: string, reason: string) => { if (await count(statement)) throw new Error(reason); };
  let attached = false;
  try {
    await target.run(`ATTACH ${sql(sourceDatabaseFile)} AS ${ident(alias)} (READ_ONLY)`); attached = true;
    if (await count(`SELECT 1 FROM ${source('local_archive_catalog')}`))
      throw new Error('Cannot merge a source with archived or compacted collection data; archive migration is required');
    const result: CollectionMergeResult = {
      source_batches: await count(`SELECT 1 FROM ${source('collection_batches')}`),
      source_payloads: await count(`SELECT 1 FROM ${source('collection_payloads')}`),
      source_receipts: await count(`SELECT 1 FROM ${source('collection_receipts')}`), inserted_batches: 0, skipped_batches: 0,
    };
    const batches = source('collection_batches'), receipts = source('collection_receipts'), payloads = source('collection_payloads');
    await rejectRows(`SELECT 1 WHERE (SELECT COALESCE(SUM(observations),0) FROM ${batches}) <> (SELECT COUNT(*) FROM ${receipts})`,
      'Source collection is incomplete: batch observation totals differ from receipt count');
    for (const [table, { key, columns }] of Object.entries(TABLES)) {
      await rejectRows(`SELECT ${ident(key)} FROM ${source(table)} GROUP BY ${ident(key)} HAVING ${ident(key)} IS NULL OR ${ident(key)}='' OR COUNT(*)<>1`,
        `Source collection has invalid or duplicate identities in ${table}`);
      const fields = (prefix: string) => columns.map(column => `${prefix}.${ident(column)}`).join(',');
      await rejectRows(`SELECT ${fields('s')} FROM ${source(table)} s JOIN ${destination(table)} t USING(${ident(key)})
        EXCEPT ALL SELECT ${fields('t')} FROM ${source(table)} s JOIN ${destination(table)} t USING(${ident(key)})`,
      `Conflicting existing collection identity in ${table}; target records were not overwritten`);
    }
    await rejectRows(`SELECT 1 FROM ${batches} b LEFT JOIN (SELECT batch_id,COUNT(*) AS n FROM ${receipts} GROUP BY batch_id) r USING(batch_id)
      WHERE b.observations IS NULL OR b.observations<0 OR b.observations<>COALESCE(r.n,0)
        OR b.source IS NULL OR b.source NOT IN ('sse','lepass') OR b.received_at IS NULL OR NOT isfinite(b.received_at)
        OR json_type(b.predictions) IS DISTINCT FROM 'ARRAY' OR json_type(b.provenance) IS DISTINCT FROM 'OBJECT'`,
    'Source collection contains an incomplete or invalid batch');
    await rejectRows(`SELECT 1 FROM ${receipts} r LEFT JOIN ${payloads} p USING(payload_id) LEFT JOIN ${batches} b USING(batch_id)
      WHERE p.payload_id IS NULL OR b.batch_id IS NULL OR p.source IS DISTINCT FROM b.source
        OR r.received_at IS NULL OR NOT isfinite(r.received_at)
        OR r.service_date IS DISTINCT FROM timezone('America/Chicago',to_timestamp(r.received_at))::DATE`,
    'Source collection contains orphaned or invalid receipts');
    await rejectRows(`SELECT 1 FROM ${payloads} p WHERE NOT EXISTS (SELECT 1 FROM ${receipts} r WHERE r.payload_id=p.payload_id)
      OR json_type(p.body) IS DISTINCT FROM 'OBJECT' OR json_type(p.raw) IS DISTINCT FROM 'OBJECT'
      OR p.payload_id IS DISTINCT FROM sha256(p.body::VARCHAR || chr(0) || p.raw::VARCHAR)
      OR p.source IS DISTINCT FROM json_extract_string(p.body,'$.source')
      OR p.vehicle_id IS DISTINCT FROM json_extract_string(p.body,'$.vehicle_id')
      OR p.route_id IS DISTINCT FROM json_extract_string(p.body,'$.route_id')
      OR p.observed_at IS DISTINCT FROM TRY_CAST(json_extract(p.body,'$.observed_at') AS DOUBLE)
      OR json_exists(p.body,'$.observation_id') OR json_exists(p.body,'$.received_at')`,
    'Source collection contains orphaned, corrupt or inconsistent payloads');
    const observationFields = 'observation_id,payload_id,received_at,service_date,batch_id,source,vehicle_id,route_id,observed_at,body,raw';
    const joined = `SELECT ${observationFields} FROM ${receipts} JOIN ${payloads} USING(payload_id)`;
    const view = `SELECT ${observationFields} FROM ${source('collected_observations')}`;
    await rejectRows(`(${joined} EXCEPT ALL ${view}) UNION ALL (${view} EXCEPT ALL ${joined})`,
      'Source observation view differs from its receipt and payload ledger');
    await rejectRows(`SELECT 1 FROM ${receipts} s JOIN ${destination('collection_batches')} b USING(batch_id)
      LEFT JOIN ${destination('collection_receipts')} r USING(observation_id)
      LEFT JOIN ${destination('collection_payloads')} p ON p.payload_id=s.payload_id
      WHERE r.observation_id IS NULL OR p.payload_id IS NULL`,
    'Existing destination batch is incomplete; refusing to skip missing evidence');

    async function* pages(): AsyncGenerator<CollectionBatch[]> {
      let last: SavedBatch | undefined;
      for (;;) {
        const after = last ? `WHERE received_at>${sql(last.received_at)}::TIMESTAMPTZ OR
          (received_at=${sql(last.received_at)}::TIMESTAMPTZ AND batch_id>${sql(last.batch_id)})` : '';
        const saved = await query<SavedBatch>(target, `SELECT batch_id,source,
          strftime(timezone('UTC',received_at),'%Y-%m-%dT%H:%M:%S.%fZ') AS received_at,
          observations,predictions::VARCHAR AS predictions,provenance::VARCHAR AS provenance
          FROM ${batches} ${after} ORDER BY ${batches}.received_at,batch_id LIMIT 200`);
        if (!saved.length) return;
        const items = await query<SavedObservation>(target, `SELECT batch_id,observation_id,payload_id,received_at,
          body::VARCHAR AS body,raw::VARCHAR AS raw FROM ${source('collected_observations')}
          WHERE batch_id IN (${saved.map(b => sql(b.batch_id)).join(',')}) ORDER BY received_at,observation_id`);
        const byBatch = new Map<string, SavedObservation[]>();
        for (const item of items) { const values = byBatch.get(item.batch_id) ?? []; values.push(item); byBatch.set(item.batch_id, values); }
        yield saved.map(batch => {
          const entries = byBatch.get(batch.batch_id) ?? [];
          if (entries.length !== batch.observations) throw new Error('Source observation view is incomplete');
          const observations = entries.map(entry => {
            const body: unknown = JSON.parse(entry.body), raw: unknown = JSON.parse(entry.raw);
            if (!object(body) || !object(raw) || body.source !== batch.source || !Number.isFinite(entry.received_at))
              throw new Error('Source observation cannot be reconstructed');
            const hash = createHash('sha256').update(JSON.stringify(body)).update('\0').update(JSON.stringify(raw)).digest('hex');
            if (hash !== entry.payload_id) throw new Error('Source payload cannot be reconstructed exactly');
            return { observation: { ...body, observation_id: entry.observation_id, received_at: entry.received_at } as unknown as StudyObservation, raw };
          });
          const predictions: unknown = JSON.parse(batch.predictions), provenance: unknown = JSON.parse(batch.provenance);
          if (!Array.isArray(predictions) || !object(provenance) || JSON.stringify(predictions) !== batch.predictions || JSON.stringify(provenance) !== batch.provenance)
            throw new Error('Source batch metadata cannot be reconstructed exactly');
          return { schema_version: 1, batch_id: batch.batch_id, source: batch.source, received_at: batch.received_at,
            observations, predictions, provenance };
        });
        last = saved.at(-1);
      }
    }
    // Validate reconstruction of every page before any destination mutation.
    let processed = 0;
    for await (const page of pages()) {
      processed += page.length;
      await onProgress?.({ phase: 'preflight', processed_batches: processed, total_batches: result.source_batches, inserted_batches: 0 });
    }
    if (processed !== result.source_batches) throw new Error('Source batch pagination is incomplete');
    processed = 0;
    for await (const page of pages()) {
      for (const batch of page) {
        if (await ingestBatch(target, batch)) result.inserted_batches++; else result.skipped_batches++;
        processed++;
      }
      await onProgress?.({ phase: 'merge', processed_batches: processed, total_batches: result.source_batches, inserted_batches: result.inserted_batches });
    }
    if (processed !== result.source_batches) throw new Error('Source batch merge is incomplete');
    for (const [table, { columns }] of Object.entries(TABLES)) {
      const fields = columns.map(ident).join(',');
      await rejectRows(`SELECT ${fields} FROM ${source(table)} EXCEPT ALL SELECT ${fields} FROM ${destination(table)}`,
        `Merged collection readback differs from source ${table}`);
    }
    await onProgress?.({ phase: 'verify', processed_batches: processed, total_batches: result.source_batches, inserted_batches: result.inserted_batches });
    return result;
  } finally { if (attached) await target.run(`DETACH ${ident(alias)}`); }
}
