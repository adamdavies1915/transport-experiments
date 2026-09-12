import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Works in source checkouts and images without .git; pins the code and bundled evidence. */
export async function processingAnalysisRevision(root = dirname(dirname(fileURLToPath(import.meta.url)))): Promise<string> {
  const paths: string[] = [];
  async function visit(folder: string) {
    for (const entry of await readdir(folder, { withFileTypes: true })) {
      const path = join(folder, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile() && /\.(ts|tsx|json|geojson)$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name)) paths.push(path);
    }
  }
  await visit(join(root, 'src'));
  await visit(join(root,'dashboard/src'));
  paths.push(join(root, 'scripts/import-server-history.ts'), join(root, 'package-lock.json'));
  const hash = createHash('sha256');
  for (const path of paths.sort()) {
    const bytes = await readFile(path);
    hash.update(relative(root, path).replaceAll('\\', '/') + '\0' + bytes.length + '\0'); hash.update(bytes);
  }
  return hash.digest('hex');
}
