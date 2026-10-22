/**
 * Tests for the folder statistics of src/extensions/summary.ts
 *
 * A folder is empty when no row of the run lies under 'folder/'. Pinned edge cases:
 * - a name containing '_' or '%' is matched literally, not as a LIKE wildcard
 * - a sibling sharing the folder's name as a prefix ('dir2', 'dir-x') does not count as content
 * - a folder holding only an empty subfolder is not empty itself
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import * as path from 'node:path';
import { promises as fs } from 'node:fs';
import { lastValueFrom } from 'rxjs';
import type { Command } from '@oclif/core';
import { summary } from '@extensions/summary.ts';
import {
  createScanDatabase,
  closeScanDatabase,
  insertFileBatch,
  type DatabaseConnection,
  type FileRow,
} from '@lib/database.ts';
import { getDatabasePath } from '@lib/platform-paths.ts';

// ---- Helpers ----

function makeRow(runId: string, rowPath: string, isDirectory: boolean): FileRow {
  return {
    run_id: runId,
    path: rowPath,
    physical_size: isDirectory ? 0 : 1,
    content_size: isDirectory ? null : 1,
    mtime: 0,
    is_directory: isDirectory,
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

/** Run summary() and return the number printed on its "Empty folders:" line. */
async function emptyFoldersReported(db: DatabaseConnection, runId: string): Promise<number> {
  const lines: string[] = [];
  const cli = { log: (line = '') => lines.push(line) } as unknown as Command;
  await summary(db, runId, cli);
  const line = lines.find(l => l.includes('Empty folders:'));
  expect(line).toBeDefined();
  return Number(line!.split(':')[1].replace(/\D/g, ''));
}

// ---- Tests ----

describe('summary folder statistics', () => {
  const RUN_ID = 'summary-test-run';
  let db: DatabaseConnection;
  let dbName: string;

  beforeEach(async () => {
    dbName = `summarytest${Date.now()}${Math.random().toString(36).slice(2, 6)}`;
    db = await createScanDatabase(dbName);
  });

  afterEach(async () => {
    await closeScanDatabase(db);
    await fs.rm(path.resolve(getDatabasePath(dbName)), { recursive: true, force: true });
  });

  it('counts a folder as empty only when nothing lies under it', async () => {
    await lastValueFrom(
      insertFileBatch(db, [
        makeRow(RUN_ID, 'docs', true),
        makeRow(RUN_ID, 'docs/a.txt', false),
        makeRow(RUN_ID, 'empty', true), // empty
        makeRow(RUN_ID, 'a_b', true), // empty: '_' must not match the 'x' of 'axb'
        makeRow(RUN_ID, 'axb', true),
        makeRow(RUN_ID, 'axb/f.txt', false),
        makeRow(RUN_ID, 'parent', true),
        makeRow(RUN_ID, 'parent/child', true), // empty
        makeRow(RUN_ID, 'dir', true), // empty: 'dir2/' and 'dir-x/' are siblings, not content
        makeRow(RUN_ID, 'dir2', true),
        makeRow(RUN_ID, 'dir2/f.txt', false),
        makeRow(RUN_ID, 'dir-x', true),
        makeRow(RUN_ID, 'dir-x/f.txt', false),
      ])
    );

    expect(await emptyFoldersReported(db, RUN_ID)).toBe(4);
  });
});
