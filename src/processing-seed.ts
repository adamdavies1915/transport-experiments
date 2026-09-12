import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, constants } from 'node:fs';
import { copyFile, mkdir, readdir, readFile, lstat, rename, rm, open, realpath } from 'node:fs/promises';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { DuckDBInstance } from '@duckdb/node-api';
import { atomicFile } from './local-journal';
import { query, sql } from './local-store';

export interface ProcessingSeed {
  schema_version: 1;
  baseline_id: string;
  source_directory: string;
  files: { path: string; bytes: number; sha256: string }[];
}
export async function sha256File(path: string): Promise<string> {
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}
const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const seedId = (seed: Omit<ProcessingSeed, 'baseline_id'>) => digest(seed);
async function syncTree(directory:string):Promise<void>{
  for(const entry of await readdir(directory,{withFileTypes:true}))if(entry.isDirectory())await syncTree(join(directory,entry.name));
  const file=await open(directory,'r');try{await file.sync();}finally{await file.close();}
}
function safePath(root: string, path: string): string {
  if (!path || path.includes('\\') || path.split('/').some(p => !p || p === '.' || p === '..')) throw new Error('Unsafe seed path');
  const target = resolve(root, path);
  if (!target.startsWith(resolve(root) + sep)) throw new Error('Seed file is outside its directory');
  return target;
}
async function regularFiles(root: string, subdirectory: string): Promise<string[]> {
  const result: string[] = [];
  try { if (!(await lstat(join(root, subdirectory))).isDirectory()) throw new Error('Seed archives must be real directories, not symbolic links'); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []; throw error; }
  for (const entry of await readdir(join(root, subdirectory), { withFileTypes: true }).catch(error => {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return []; throw error;
  })) {
    const name = subdirectory ? `${subdirectory}/${entry.name}` : entry.name;
    if (entry.isSymbolicLink()) throw new Error('Seed archives must not contain symbolic links');
    if (entry.isDirectory()) result.push(...await regularFiles(root, name));
    else if (entry.isFile()) result.push(name);
    else throw new Error('Unsupported seed archive entry');
  }
  return result;
}
async function verifiedCopy(source: string, target: string, expected: ProcessingSeed['files'][number]|undefined, sourceRoot: string) {
  for (let parent=dirname(source);parent!==sourceRoot;parent=dirname(parent)) {
    if(!parent.startsWith(sourceRoot+sep))throw new Error('Seed path escaped its source directory');
    if ((await lstat(parent)).isSymbolicLink()) throw new Error('Seed paths must not contain symbolic links');
  }
  const before = await lstat(source);
  if (!before.isFile() || before.isSymbolicLink()) throw new Error('Seed source must be a regular file');
  await mkdir(dirname(target), { recursive: true, mode: 0o700 });
  await copyFile(source, target, constants.COPYFILE_EXCL);
  const file = await open(target, 'r+'); try { await file.sync(); } finally { await file.close(); }
  const sha256 = await sha256File(target);
  if (sha256 !== await sha256File(source) || (expected && (before.size !== expected.bytes || sha256 !== expected.sha256)))
    throw new Error('Seed checksum mismatch');
  return { bytes: before.size, sha256 };
}
/** Holds the DuckDB writer lock while checkpointing/copying. No live DB is ever shared. */
export async function createProcessingSeed(dataDirectory: string, destination: string): Promise<ProcessingSeed> {
  const source = await realpath(dataDirectory), target = resolve(destination), staging = `${target}.${randomUUID()}.tmp`;
  if (target === source || target.startsWith(source + sep)) throw new Error('Seed destination must be outside the live data directory');
  if (!(await lstat(join(source, 'transit.duckdb'))).isFile()) throw new Error('An existing historical database is required');
  try { await lstat(target); throw new Error('Seed destination already exists'); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  const db = await DuckDBInstance.create(join(source, 'transit.duckdb'), { threads: '1', memory_limit: '256MB' });
  const c = await db.connect();
  try {
    await c.run('CHECKPOINT');
    await mkdir(staging, { recursive: true, mode: 0o700 });
    const paths = ['transit.duckdb', ...await regularFiles(source, 'archives')].sort();
    const files: ProcessingSeed['files'] = [];
    const catalog = await query<{ path: string; sha256: string }>(c, 'SELECT path,sha256 FROM local_archive_catalog');
    for (const archive of catalog) {
      const path = relative(source, archive.path).split(sep).join('/');
      if (!paths.includes(path) || await sha256File(safePath(source, path)) !== archive.sha256) throw new Error('Historical archive is missing or outside the seed');
    }
    for (const path of paths) files.push({ path, ...await verifiedCopy(safePath(source, path), safePath(staging, path),undefined,source) });
    const body = { schema_version: 1 as const, source_directory: source, files };
    const seed: ProcessingSeed = { ...body, baseline_id: seedId(body) };
    await atomicFile(join(staging, 'seed.json'), JSON.stringify(seed, null, 2));
    await syncTree(staging);
    await rename(staging, target);
    const parent=await open(dirname(target),'r');try{await parent.sync();}finally{await parent.close();}
    await mkdir(join(source, 'processing'), { recursive: true, mode: 0o700 });
    await atomicFile(join(source, 'processing', 'baseline.json'), JSON.stringify({ baseline_id: seed.baseline_id, initialized_at: new Date().toISOString() }));
    const sourceParent=await open(source,'r');try{await sourceParent.sync();}finally{await sourceParent.close();}
    return seed;
  } catch (error) { await rm(staging, { recursive: true, force: true }); throw error; }
  finally { c.closeSync(); db.closeSync(); }
}
/** Restore into an empty directory and relocate verified archive paths for this OS. */
export async function restoreProcessingSeed(seedDirectory: string, destination: string): Promise<ProcessingSeed> {
  const source = await realpath(seedDirectory), target = resolve(destination), staging = `${target}.${randomUUID()}.tmp`;
  const seed = JSON.parse(await readFile(join(source, 'seed.json'), 'utf8')) as ProcessingSeed;
  if (seed.schema_version !== 1 || !Array.isArray(seed.files) || !seed.files.some(f => f.path === 'transit.duckdb') ||
      new Set(seed.files.map(f => f.path)).size !== seed.files.length ||
      seed.baseline_id !== seedId({ schema_version: seed.schema_version, source_directory: seed.source_directory, files: seed.files })) throw new Error('Invalid seed manifest');
  try { await lstat(target); throw new Error('Restore destination must not exist'); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  try {
    for (const file of seed.files) {
      if (!Number.isSafeInteger(file.bytes) || file.bytes < 0 || !/^[a-f0-9]{64}$/.test(file.sha256)) throw new Error('Invalid seed file metadata');
      await verifiedCopy(safePath(source, file.path), safePath(staging, file.path), file,source);
    }
    const relocated = (path: string) => {
      // The seed was produced on Linux/macOS; paths use forward slashes there.
      const root = seed.source_directory.replaceAll('\\', '/').replace(/\/$/, '');
      const normalized = path.replaceAll('\\', '/');
      if (!normalized.startsWith(root + '/')) throw new Error('Archive path is outside the seed');
      const rel = normalized.slice(root.length + 1);
      if (!seed.files.some(f => f.path === rel)) throw new Error('Archive not present in seed');
      return safePath(target, rel);
    };
    const db = await DuckDBInstance.create(join(staging, 'transit.duckdb'), { threads: '1', memory_limit: '256MB' });
    const c = await db.connect();
    try {
      const rows = await query<{ date: string; path: string }>(c, 'SELECT date::VARCHAR AS date,path FROM local_archive_catalog');
      for (const row of rows) await c.run(`UPDATE local_archive_catalog SET path=${sql(relocated(row.path))} WHERE date=${sql(row.date)}::DATE`);
      await c.run('CHECKPOINT');
    } finally { c.closeSync(); db.closeSync(); }
    for (const file of seed.files.filter(f => f.path.startsWith('archives/') && f.path.endsWith('/manifest.json'))) {
      const path = safePath(staging, file.path), manifest = JSON.parse(await readFile(path, 'utf8'));
      if (typeof manifest.file !== 'string') throw new Error('Invalid historical archive manifest');
      manifest.file = relocated(manifest.file);
      await atomicFile(path, JSON.stringify(manifest));
    }
    await mkdir(join(staging, 'processing'), { recursive: true, mode: 0o700 });
    await atomicFile(join(staging, 'processing', 'baseline.json'), JSON.stringify({ baseline_id: seed.baseline_id, initialized_at: new Date().toISOString() }));
    await syncTree(staging);
    await rename(staging, target);
    const parent=await open(dirname(target),'r');try{await parent.sync();}finally{await parent.close();}
    return seed;
  } catch (error) { await rm(staging, { recursive: true, force: true }); throw error; }
}
