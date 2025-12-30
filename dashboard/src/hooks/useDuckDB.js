import { useState, useEffect, useCallback } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';

const R2_ACCOUNT_ID = import.meta.env.VITE_R2_ACCOUNT_ID;
const R2_ACCESS_KEY_ID = import.meta.env.VITE_R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = import.meta.env.VITE_R2_SECRET_ACCESS_KEY;
const R2_BUCKET = import.meta.env.VITE_R2_BUCKET || 'nola-transit';

export function useDuckDB() {
  const [db, setDb] = useState(null);
  const [conn, setConn] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function initDuckDB() {
      try {
        const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
        const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

        const worker_url = URL.createObjectURL(
          new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
        );

        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger();
        const database = new duckdb.AsyncDuckDB(logger, worker);

        await database.instantiate(bundle.mainModule, bundle.pthreadWorker);
        URL.revokeObjectURL(worker_url);

        const connection = await database.connect();

        // Configure S3/R2
        await connection.query(`INSTALL httpfs`);
        await connection.query(`LOAD httpfs`);
        await connection.query(`SET s3_region='auto'`);
        await connection.query(`SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com'`);
        await connection.query(`SET s3_access_key_id='${R2_ACCESS_KEY_ID}'`);
        await connection.query(`SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}'`);
        await connection.query(`SET s3_url_style='path'`);

        setDb(database);
        setConn(connection);
        setLoading(false);
      } catch (err) {
        console.error('DuckDB init error:', err);
        setError(err.message);
        setLoading(false);
      }
    }

    initDuckDB();
  }, []);

  const query = useCallback(async (sql) => {
    if (!conn) throw new Error('Database not initialized');
    const result = await conn.query(sql);
    return result.toArray().map(row => row.toJSON());
  }, [conn]);

  const getDataPath = useCallback(() => {
    return `s3://${R2_BUCKET}/**/*.parquet`;
  }, []);

  return { db, conn, loading, error, query, getDataPath };
}
