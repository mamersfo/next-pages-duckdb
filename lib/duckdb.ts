/* Adapted from https://github.com/ryan-williams/next-duckdb-parquet-demo */

import * as duckdb from '@duckdb/duckdb-wasm'
import { AsyncDuckDB, DuckDBBundle } from '@duckdb/duckdb-wasm'
import Worker from 'web-worker'
import path from 'path'

const ENABLE_DUCK_LOGGING = false
const SilentLogger = { log: () => {} }

type WorkerBundle = { bundle: DuckDBBundle; worker: Worker }

export async function nodeWorkerBundle(): Promise<WorkerBundle> {
  const DUCKDB_DIST = `node_modules/@duckdb/duckdb-wasm/dist`
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: path.resolve(DUCKDB_DIST, './duckdb-mvp.wasm'),
      mainWorker: path.resolve(DUCKDB_DIST, './duckdb-node-mvp.worker.cjs'),
    },
    eh: {
      mainModule: path.resolve(DUCKDB_DIST, './duckdb-eh.wasm'),
      mainWorker: path.resolve(DUCKDB_DIST, './duckdb-node-eh.worker.cjs'),
    },
  })
  const mainWorker = bundle.mainWorker
  if (mainWorker) {
    const worker = new Worker(mainWorker)
    return { bundle, worker }
  } else {
    throw Error(`No mainWorker: ${mainWorker}`)
  }
}

export async function browserWorkerBundle(): Promise<WorkerBundle> {
  const allBundles = duckdb.getJsDelivrBundles()
  const bundle = await duckdb.selectBundle(allBundles)
  const mainWorker = bundle.mainWorker
  if (mainWorker) {
    const worker = await duckdb.createWorker(mainWorker)
    return { bundle, worker }
  } else {
    throw Error(`No mainWorker: ${mainWorker}`)
  }
}

// Global AsyncDuckDB instance
let dbPromise: Promise<AsyncDuckDB> | null = null

/**
 * Fetch global AsyncDuckDB instance; initialize if necessary
 */
export function getDuckDb(): Promise<AsyncDuckDB> {
  if (!dbPromise) {
    dbPromise = initDuckDb()
  }
  return dbPromise
}

/**
 * Initialize global AsyncDuckDB instance
 */
export async function initDuckDb(): Promise<AsyncDuckDB> {
  console.time('duckdb-wasm fetch')
  const { worker, bundle } = await (typeof window === 'undefined'
    ? nodeWorkerBundle()
    : browserWorkerBundle())
  console.timeEnd('duckdb-wasm fetch')
  console.log('bestBundle:', bundle)

  console.time('DB instantiation')
  const logger = ENABLE_DUCK_LOGGING ? new duckdb.ConsoleLogger() : SilentLogger
  const db = new AsyncDuckDB(logger, worker)
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
  await db.open({
    path: ':memory:',
    query: {
      castBigIntToDouble: true,
    },
  })
  console.timeEnd('DB instantiation')

  return db
}

export async function runQuery<T>(
  db: AsyncDuckDB,
  query: string
): Promise<T[]> {
  let returnValue: T[] = []
  const conn = await db.connect()
  const result = await conn.query(query)
  const proxies = result.toArray()
  returnValue = JSON.parse(JSON.stringify(proxies)) as T[]
  conn.close()
  return returnValue
}
