/**
 * Characterization tests for src/lib/hash-calculator.ts
 *
 * These tests lock in the exact error semantics and pipeline behavior before any refactor:
 * - Per-file read errors: hashSingleFile returns null → filtered out → file silently skipped
 * - Batch DB update errors: catchError → of(0) → pipeline continues
 * - stats.errors is never incremented (always 0 in progress callback)
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import * as path from 'node:path';
import * as os from 'node:os';
import { promises as fs } from 'node:fs';
import { lastValueFrom, toArray } from 'rxjs';
import { eq, sql } from 'drizzle-orm';
import { calculateHashes, getHashingProgress, performHashing } from '@lib/hash-calculator.ts';
import {
  createScanDatabase,
  closeScanDatabase,
  insertFileBatch,
  files,
  type DatabaseConnection,
  type FileRow,
} from '@lib/database.ts';
import { getDatabasePath } from '@lib/platform-paths.ts';

// ---- Helpers ----

async function createTestDb(name: string): Promise<{
  db: DatabaseConnection;
  cleanup: () => Promise<void>;
}> {
  const db = await createScanDatabase(name);
  const dbDir = path.resolve(getDatabasePath(name));
  return {
    db,
    async cleanup() {
      await closeScanDatabase(db);
      await fs.rm(dbDir, { recursive: true, force: true }).catch(() => {});
    },
  };
}

function makeFileRow(
  runId: string,
  filePath: string,
  physicalSize: number,
  contentSize: number | null
): FileRow {
  return {
    run_id: runId,
    path: filePath,
    physical_size: physicalSize,
    content_size: contentSize,
    mtime: 0,
    is_directory: false,
    is_hidden: false,
    is_system: false,
    hash: null,
    is_archive_container: false,
    archive_parent_path: null,
    archive_depth: 0,
    archive_format: null,
    extraction_error: null,
  };
}

// ---- Test state ----

let tempDir: string;
const SAME_CONTENT = 'identical content for hashing test';
const UNIQUE_CONTENT = 'unique content 9a8b7c6d5e4f3g2h1i that appears only once';

beforeAll(async () => {
  tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'archifiltre-hash-'));
  await fs.writeFile(path.join(tempDir, 'file1.txt'), SAME_CONTENT);
  await fs.writeFile(path.join(tempDir, 'file2.txt'), SAME_CONTENT);
  await fs.writeFile(path.join(tempDir, 'unique.txt'), UNIQUE_CONTENT);
});

afterAll(async () => {
  await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
});

describe('performHashing', () => {
  let dbCtx: Awaited<ReturnType<typeof createTestDb>>;
  const RUN_ID = 'hash-test-run';

  beforeEach(async () => {
    const name = `hashtest${Date.now()}${Math.random().toString(36).slice(2, 6)}`;
    dbCtx = await createTestDb(name);
  });

  afterEach(async () => {
    await dbCtx.cleanup();
  });

  it('is a no-op when no files have duplicate content sizes', async () => {
    // file1 and unique have different sizes → findDuplicateSizes returns [] → nothing hashed
    const file1Size = Buffer.byteLength(SAME_CONTENT);
    const uniqueSize = Buffer.byteLength(UNIQUE_CONTENT);

    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', file1Size, file1Size),
        makeFileRow(RUN_ID, 'unique.txt', uniqueSize, uniqueSize),
      ])
    );

    await lastValueFrom(performHashing({ database: dbCtx.db, runId: RUN_ID, rootPath: tempDir }));

    const rows = await dbCtx.db.db
      .select({ hash: files.hash })
      .from(files)
      .where(eq(files.run_id, RUN_ID));

    expect(rows.every(r => r.hash === null)).toBe(true);
  });

  it('hashes all files that share a content size', async () => {
    const size = Buffer.byteLength(SAME_CONTENT);
    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', size, size),
        makeFileRow(RUN_ID, 'file2.txt', size, size),
      ])
    );

    await lastValueFrom(performHashing({ database: dbCtx.db, runId: RUN_ID, rootPath: tempDir }));

    const rows = await dbCtx.db.db
      .select({ path: files.path, hash: files.hash })
      .from(files)
      .where(eq(files.run_id, RUN_ID));

    expect(rows.every(r => r.hash !== null)).toBe(true);
  });

  it('assigns the same hash to files with identical content', async () => {
    const size = Buffer.byteLength(SAME_CONTENT);
    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', size, size),
        makeFileRow(RUN_ID, 'file2.txt', size, size),
      ])
    );

    await lastValueFrom(performHashing({ database: dbCtx.db, runId: RUN_ID, rootPath: tempDir }));

    const rows = await dbCtx.db.db
      .select({ hash: files.hash })
      .from(files)
      .where(eq(files.run_id, RUN_ID));

    const hashes = rows.map(r => r.hash);
    expect(hashes[0]).toBe(hashes[1]);
    expect(hashes[0]).not.toBeNull();
  });

  it('calls progress callback with (processed, total, errors) at batch boundaries', async () => {
    const size = Buffer.byteLength(SAME_CONTENT);
    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', size, size),
        makeFileRow(RUN_ID, 'file2.txt', size, size),
      ])
    );

    const calls: Array<{ processed: number; total: number; errors: number }> = [];
    await lastValueFrom(
      performHashing({ database: dbCtx.db, runId: RUN_ID, rootPath: tempDir }, (processed, total, errors) => {
        calls.push({ processed, total, errors });
      })
    );

    expect(calls.length).toBeGreaterThan(0);
    // errors is always 0 — hash-calculator never increments stats.errors
    expect(calls.every(c => c.errors === 0)).toBe(true);
    expect(calls[calls.length - 1].processed).toBeGreaterThan(0);
  });

  it('skips a file with an unreadable path and continues hashing the others', async () => {
    // 'ghost.txt' does not exist on disk — hashSingleFile returns null → filtered → skipped
    const size = Buffer.byteLength(SAME_CONTENT);
    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', size, size),
        makeFileRow(RUN_ID, 'ghost.txt', size, size),
      ])
    );

    // Should not throw
    await expect(lastValueFrom(performHashing({ database: dbCtx.db, runId: RUN_ID, rootPath: tempDir }))).resolves.toBeUndefined();

    const rows = await dbCtx.db.db
      .select({ path: files.path, hash: files.hash })
      .from(files)
      .where(eq(files.run_id, RUN_ID));

    const file1 = rows.find(r => r.path === 'file1.txt');
    const ghost = rows.find(r => r.path === 'ghost.txt');

    expect(file1?.hash).not.toBeNull();
    expect(ghost?.hash).toBeNull();
  });

  it('calls context.onProgress with phase "hashing" at each batch boundary', async () => {
    const size = Buffer.byteLength(SAME_CONTENT);
    await lastValueFrom(
      insertFileBatch(dbCtx.db, [
        makeFileRow(RUN_ID, 'file1.txt', size, size),
        makeFileRow(RUN_ID, 'file2.txt', size, size),
      ])
    );

    const calls: Array<{ phase: string; processed: number; total: number | null }> = [];
    await lastValueFrom(
      performHashing({
        database: dbCtx.db,
        runId: RUN_ID,
        rootPath: tempDir,
        onProgress: (phase, processed, total) => calls.push({ phase, processed, total }),
      })
    );

    expect(calls.length).toBeGreaterThan(0);
    expect(calls.every(c => c.phase === 'hashing')).toBe(true);
    expect(calls[calls.length - 1].processed).toBeGreaterThan(0);
  });

  it('hashes every candidate and emits once when they span several batches', async () => {
    // Regressions: OFFSET pagination over `hash IS NULL` skipped files, because each hashed
    // batch shrinks the result set while the offset keeps advancing; and the pipeline emitted
    // once per batch, so the scanner re-ran duplicate detection after every batch.
    const size = Buffer.byteLength(SAME_CONTENT);
    const names = Array.from({ length: 7 }, (_, i) => `batch-${i}.txt`);
    await Promise.all(names.map(n => fs.writeFile(path.join(tempDir, n), SAME_CONTENT)));
    await lastValueFrom(
      insertFileBatch(
        dbCtx.db,
        names.map(n => makeFileRow(RUN_ID, n, size, size))
      )
    );

    const emissions = await lastValueFrom(
      calculateHashes(dbCtx.db, RUN_ID, tempDir, { batchSize: 2 }).pipe(toArray())
    );
    expect(emissions).toHaveLength(1);

    const rows = await dbCtx.db.db
      .select({ path: files.path, hash: files.hash })
      .from(files)
      .where(eq(files.run_id, RUN_ID));

    expect(rows).toHaveLength(names.length);
    expect(rows.filter(r => r.hash === null).map(r => r.path)).toEqual([]);
  });

  it('counts candidates when there are more duplicate sizes than Postgres bind parameters', async () => {
    // Regression: the duplicate sizes were bound one parameter each, and Postgres caps a
    // statement at 65,535 — past that every candidate query failed and hashing was skipped.
    const sizes = 70_000;
    await dbCtx.db.db.execute(sql`
      INSERT INTO files (run_id, path, physical_size, content_size, mtime, is_directory, is_hidden, is_system)
      SELECT ${RUN_ID}, 'f' || g, (g + 1) / 2, (g + 1) / 2, 0, false, false, false
      FROM generate_series(1, ${sizes * 2}) AS g
    `);

    const progress = await lastValueFrom(getHashingProgress(dbCtx.db, RUN_ID));

    expect(progress).toEqual({ total: sizes * 2, completed: 0, remaining: sizes * 2 });
  }, 30_000);
});
