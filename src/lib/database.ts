/**
 * Database Operations - PGlite + Drizzle + RxJS
 *
 * Embedded PostgreSQL via PGlite, with Drizzle ORM and RxJS for reactive operations.
 */

import * as path from 'node:path';
import { promises as fs } from 'node:fs';
import { createHash } from 'node:crypto';
import { file } from 'bun';
import { getDatabasePath } from './platform-paths.ts';
import { ensureDirectory } from './helpers.ts';
import type { ScanCounts } from '@lib/job-context.ts';
import type { Observable } from 'rxjs';
import { from, of, defer, EMPTY, concat } from 'rxjs';
import { map, catchError, tap, switchMap } from 'rxjs/operators';
import { gt } from 'drizzle-orm';
import { drizzle } from 'drizzle-orm/pglite';
import { PGlite } from '@electric-sql/pglite';
import {
  pgTable,
  text,
  integer,
  bigint,
  index,
  primaryKey,
  boolean,
  alias,
} from 'drizzle-orm/pg-core';
import { eq, ne, and, count, sum, exists, inArray, isNull, isNotNull, sql } from 'drizzle-orm';
import { logger } from '@lib/logging.ts';
import { isStandalone } from './platform-paths.ts';

// Import PGlite WASM files for bundled executable (embedded at build time)
import wasmPath from '../../wasm_binaries/pglite.wasm' with { type: 'file' };
import initdbWasmPath from '../../wasm_binaries/initdb.wasm' with { type: 'file' };
import dataPath from '../../wasm_binaries/pglite.data' with { type: 'file' };

// Cached compiled WASM modules to avoid recompilation on subsequent PGlite instances
let _pgliteWasmModule: WebAssembly.Module | undefined;
let _initdbWasmModule: WebAssembly.Module | undefined;

async function getStandalonePGliteOptions() {
  if (!isStandalone()) return {};
  _pgliteWasmModule ??= await WebAssembly.compile(await file(wasmPath).arrayBuffer());
  _initdbWasmModule ??= await WebAssembly.compile(await file(initdbWasmPath).arrayBuffer());
  return {
    pgliteWasmModule: _pgliteWasmModule,
    initdbWasmModule: _initdbWasmModule,
    fsBundle: file(dataPath),
  };
}

// === Schema Definition ===

/**
 * Files table schema - includes ALL discovered items (files + directories)
 * with physical_size (actual disk usage) and content_size (logical content for comparison)
 */
export const files = pgTable(
  'files',
  {
    run_id: text('run_id').notNull(),
    path: text('path').notNull(),
    physical_size: bigint('physical_size', { mode: 'number' }).notNull(), // Actual bytes on disk
    content_size: bigint('content_size', { mode: 'number' }), // Logical content size for comparison
    mtime: bigint('mtime', { mode: 'number' }).notNull(),
    is_directory: boolean('is_directory').notNull(),
    is_hidden: boolean('is_hidden').notNull(),
    is_system: boolean('is_system').notNull(),
    hash: text('hash'), // Only files get hashed, directories remain NULL
    // Resumable-discovery frontier (Phase 7 increment 2): NULL = this enumeration unit
    // (a directory, or an archive container) has NOT had all its children durably
    // committed yet, so it's still on the frontier and must be (re-)enumerated on resume.
    // Stamped (epoch seconds) only after the LAST child of the unit commits, in the same
    // transaction (see frontier.ts / scanner.ts).
    enumerated_at: integer('enumerated_at'),
    // Archive preprocessing fields
    is_archive_container: boolean('is_archive_container').default(false),
    archive_parent_path: text('archive_parent_path'), // Path to parent archive if nested
    archive_depth: integer('archive_depth').default(0), // Nesting level (0 = root level)
    archive_format: text('archive_format'), // 'zip', 'tar', '7z', etc.
    extraction_error: text('extraction_error'), // Error message if archive processing failed
  },
  table => ({
    pk: primaryKey({ columns: [table.run_id, table.path] }),
    runIdIdx: index('idx_files_run_id').on(table.run_id),
    // Prefix-LIKE on path ('folder/%') drives the LOD/aggregate queries; the
    // runtime CREATE INDEX below uses text_pattern_ops (authoritative).
    pathPatternIdx: index('idx_files_path_pattern').on(table.run_id, table.path),
    physicalSizeIdx: index('idx_files_physical_size').on(table.physical_size),
    contentSizeIdx: index('idx_files_content_size').on(table.content_size),
    hashIdx: index('idx_files_hash').on(table.hash),
    archiveContainerIdx: index('idx_files_archive_container').on(table.is_archive_container),
    archiveParentIdx: index('idx_files_archive_parent').on(table.archive_parent_path),
    archiveDepthIdx: index('idx_files_archive_depth').on(table.archive_depth),
  })
);

// === Scan Metadata Table ===

/**
 * Scan metadata table - stores information about each scan run
 * Includes root_path for resolving relative file paths
 */
export const scanMetadata = pgTable('scan_metadata', {
  run_id: text('run_id').primaryKey(),
  root_path: text('root_path').notNull(),
  started_at: integer('started_at').notNull(),
  completed_at: integer('completed_at'),
  file_count: integer('file_count'),
  // running | paused | complete | cancelled — drives the UI's "Continue" vs live vs
  // done state without inferring from completed_at. NULL on legacy rows.
  status: text('status'),
});

// === Types ===

export type FileRow = typeof files.$inferInsert;
export type FileSelect = typeof files.$inferSelect;
export type ScanMetadataRow = typeof scanMetadata.$inferInsert;
export type ScanMetadataSelect = typeof scanMetadata.$inferSelect;

export interface DatabaseConnection {
  name: string;
  pg: PGlite;
  db: ReturnType<typeof drizzle>;
}

export interface ScanStats {
  // Canonical counts (see ScanCounts / project-canonical-count-model): `totalFiles` is
  // non-directory rows (real files + archive entries), `totalFolders` is directory rows
  // (incl. dirs inside archives). NOT COUNT(*) — every surface shows these.
  totalFiles: number;
  totalFolders: number;
  totalPhysicalSize: number; // Renamed from totalSize
  totalContentSize: number; // New - sum of content sizes
  duplicateGroups: number;
  duplicateFiles: number;
  // Archive statistics
  totalArchives: number; // Number of archive containers
  totalArchiveEntries: number; // Number of files within archives (sub-count of totalFiles)
  archiveFormats: string[]; // List of archive formats found
}

// === Database Management ===

/**
 * Create a safe database path for all platforms
 */
function createDatabasePath(name: string): string {
  return getDatabasePath(name);
}

/**
 * Initialize database tables and indexes
 */
async function initializeSchema(db: ReturnType<typeof drizzle>): Promise<void> {
  try {
    // Create files table - stores ALL discovered items (files + directories)
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS files (
        run_id TEXT NOT NULL,
        path TEXT NOT NULL,
        physical_size BIGINT NOT NULL,
        content_size BIGINT,
        mtime BIGINT NOT NULL,
        is_directory BOOLEAN NOT NULL,
        is_hidden BOOLEAN NOT NULL,
        is_system BOOLEAN NOT NULL,
        hash TEXT,
        enumerated_at INTEGER,
        is_archive_container BOOLEAN DEFAULT FALSE,
        archive_parent_path TEXT,
        archive_depth INTEGER DEFAULT 0,
        archive_format TEXT,
        extraction_error TEXT,
        CONSTRAINT files_pkey PRIMARY KEY (run_id, path)
      )
    `);

    // Migration for datadirs created before the frontier column existed (the CREATE
    // TABLE above is IF NOT EXISTS, so an existing table keeps its old shape). Also
    // covers the warm-start template once it's copied. Idempotent.
    await db.execute(sql`ALTER TABLE files ADD COLUMN IF NOT EXISTS enumerated_at INTEGER`);

    // Create indexes for performance
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_files_run_id ON files (run_id)`);
    // Prefix-LIKE on path ('folder/%') drives the LOD/aggregate descendant joins;
    // without text_pattern_ops those scans are O(n²) on large scans.
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_path_pattern ON files (run_id, path text_pattern_ops)`
    );
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_physical_size ON files (physical_size)`
    );
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_content_size ON files (content_size) WHERE content_size IS NOT NULL`
    );
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_hash ON files (hash) WHERE hash IS NOT NULL`
    );
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_files_hidden ON files (is_hidden)`);
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_files_system ON files (is_system)`);
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_archive_container ON files (is_archive_container)`
    );
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_archive_parent ON files (archive_parent_path) WHERE archive_parent_path IS NOT NULL`
    );
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_archive_depth ON files (archive_depth)`
    );
    // Frontier lookup on resume: the set of un-enumerated units per run. Partial index —
    // only frontier rows are indexed, so it stays tiny (empties out as the scan completes)
    // and the resume seed query is a cheap index scan, not a table scan.
    await db.execute(
      sql`CREATE INDEX IF NOT EXISTS idx_files_frontier ON files (run_id) WHERE enumerated_at IS NULL`
    );

    // Create scan_metadata table - stores information about each scan run
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS scan_metadata (
        run_id TEXT PRIMARY KEY,
        root_path TEXT NOT NULL,
        started_at INTEGER NOT NULL,
        completed_at INTEGER,
        file_count INTEGER,
        status TEXT
      )
    `);
    // Migration for pre-existing scan_metadata tables (CREATE above is IF NOT EXISTS).
    // status ∈ running|paused|complete|cancelled; NULL on old rows is treated as
    // 'complete' if completed_at is set, else 'running'.
    await db.execute(sql`ALTER TABLE scan_metadata ADD COLUMN IF NOT EXISTS status TEXT`);

    // Materialized per-directory aggregates (size/counts + max subtree depth).
    // Computed once per run (see populateDirStats) so get_tree is O(dirs) instead
    // of re-running the O(dirs×files) descendant join on every query. total_size
    // is BIGINT — a directory subtree easily exceeds the 2 GB INTEGER ceiling.
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS dir_stats (
        run_id TEXT NOT NULL,
        path TEXT NOT NULL,
        total_size BIGINT NOT NULL DEFAULT 0,
        file_count BIGINT NOT NULL DEFAULT 0,
        dir_count BIGINT NOT NULL DEFAULT 0,
        max_depth INTEGER NOT NULL DEFAULT 0,
        deepest_path TEXT,
        CONSTRAINT dir_stats_pkey PRIMARY KEY (run_id, path)
      )
    `);

    // Skip ledger: units the scan could not fully process (unreadable/timed-out archive, dead
    // share, permission, truncation), grouped by reason for the UI and drillable to path. fs skips
    // have no files row, so extraction_error alone can't carry them; this table is the unified
    // record.
    await db.execute(sql`
      CREATE TABLE IF NOT EXISTS scan_skips (
        run_id TEXT NOT NULL,
        path TEXT NOT NULL,
        kind TEXT NOT NULL,
        reason TEXT NOT NULL,
        at INTEGER NOT NULL,
        CONSTRAINT scan_skips_pkey PRIMARY KEY (run_id, path)
      )
    `);
    await db.execute(sql`CREATE INDEX IF NOT EXISTS idx_scan_skips_reason ON scan_skips (run_id, reason)`);

    logger.debug(
      'Database schema initialized with physical_size, content_size, archive preprocessing fields, and scan_metadata'
    );
  } catch (error) {
    logger.error('Failed to initialize database schema', error as Error);
    throw error;
  }
}

/**
 * Materialize per-directory aggregates into dir_stats for one run: size + file/
 * dir counts, plus the depth metric the icicle leans on — max_depth (how many
 * levels the deepest descendant sits below this directory) and deepest_path (the
 * descendant achieving it). Runs once; get_tree then just reads it.
 *
 * Crucially this does NOT join directories to descendants by prefix LIKE: a
 * `f.path LIKE d.path || '/%'` join uses a *column-derived* pattern, which the
 * planner cannot turn into an index range-scan, so it degenerates to a nested
 * loop — O(dirs × files), ~633M row-touches on a 10k-dir / 58k-file scan (minutes,
 * 1 GB+ RAM). Instead we ATTRIBUTE each item to each of its ancestor directories
 * via generate_series over its path segments, then GROUP BY the ancestor. That is
 * O(Σ depth) — a few hundred K attribution rows — and finishes in seconds.
 *
 * Idempotent (ON CONFLICT DO NOTHING), so a re-call — or a lazy first-read of a
 * scan made before dir_stats existed — is a safe no-op. deepest_path uses an
 * argmax trick: MAX of (zero-padded depth || path) orders by depth first, then
 * the 6-char prefix is stripped so the stored value is the clean path.
 */
export async function populateDirStats(
  connection: DatabaseConnection,
  runId: string
): Promise<void> {
  // Idempotent: clear this run's rows first so it can run more than once (a frontier
  // resume populates from the PARTIAL files at the start so the icicle isn't blank, then
  // from the COMPLETE tree at the end). Without the clear, the second INSERT collides on
  // the (run_id, path) PK and ON CONFLICT DO NOTHING keeps the stale partial. Wrapped in a
  // txn so a concurrent get_tree never sees a half-cleared table.
  await connection.db.transaction(async tx => {
    await tx.execute(sql`DELETE FROM dir_stats WHERE run_id = ${runId}`);
    await tx.execute(sql`
    INSERT INTO dir_stats (run_id, path, total_size, file_count, dir_count, max_depth, deepest_path)
    SELECT
      ${runId} AS run_id,
      anc.anc_path AS path,
      COALESCE(SUM(CASE WHEN anc.kind = 'file' THEN anc.sz ELSE 0 END), 0) AS total_size,
      COALESCE(SUM(CASE WHEN anc.kind = 'file' THEN 1 ELSE 0 END), 0) AS file_count,
      COALESCE(SUM(CASE WHEN anc.kind = 'dir' THEN 1 ELSE 0 END), 0) AS dir_count,
      COALESCE(MAX(anc.rel_depth), 0) AS max_depth,
      substring(MAX(anc.depth_key) FROM 7) AS deepest_path
    FROM (
      -- Attribute every item to each ancestor directory (a strict path prefix).
      SELECT
        array_to_string((string_to_array(f.path, '/'))[1:i.i], '/') AS anc_path,
        (f.segs - i.i) AS rel_depth,
        (CASE WHEN f.is_directory THEN 'dir' ELSE 'file' END)::text AS kind,
        -- Weight the icicle by CONTENT size, not the on-disk footprint. For a normal
        -- file content_size == physical_size, so nothing changes; the point is archives:
        -- their entries carry physical_size 0 but a real content_size (uncompressed), so
        -- weighting by content is what gives an archive width instead of collapsing to a
        -- zero-size ghost. An archive CONTAINER contributes 0 (its entries already carry
        -- its content — counting the container's own compressed size on top would
        -- double-count it at every ancestor), UNLESS it was never expanded (too large /
        -- extraction_error), in which case there are no entries and we fall back to its
        -- compressed footprint so it still shows.
        (CASE
          WHEN f.is_directory THEN 0
          WHEN f.is_archive_container THEN (CASE WHEN f.extraction_error IS NOT NULL THEN f.physical_size ELSE 0 END)
          ELSE COALESCE(f.content_size, f.physical_size)
        END) AS sz,
        (lpad(f.segs::text, 6, '0') || f.path)::text AS depth_key
      FROM (
        SELECT
          path,
          physical_size,
          content_size,
          is_directory,
          is_archive_container,
          extraction_error,
          (length(path) - length(replace(path, '/', '')) + 1) AS segs
        FROM files
        WHERE run_id = ${runId}
      ) f
      CROSS JOIN LATERAL generate_series(1, f.segs - 1) AS i(i)

      UNION ALL

      -- Ensure every directory has a row even if it has no descendants (empty dir).
      SELECT
        d.path AS anc_path,
        NULL::int AS rel_depth,
        NULL::text AS kind,
        0 AS sz,
        NULL::text AS depth_key
      FROM files d
      WHERE d.run_id = ${runId} AND d.is_directory = true
    ) anc
    GROUP BY anc.anc_path
    ON CONFLICT (run_id, path) DO NOTHING
  `);
  });
}

/**
 * Incrementally fold ONE ingestion batch into dir_stats — the live, DB-central
 * maintenance that lets the icicle read the tree straight from the DB at any moment
 * (and leaves dir_stats fully materialized by completion, so there's no one-shot
 * populateDirStats recompute spike, and nothing is retained in JS across batches).
 *
 * Computes the batch's ancestor deltas in a transient Map (discarded after the
 * UPSERT) and folds them in additively, producing EXACTLY what populateDirStats
 * would: size/counts are additive; max_depth is a running GREATEST; deepest_path is
 * argmax by depth then path. O(batch × depth) — independent of total table size.
 */
export async function rollupDirStatsBatch(
  connection: DatabaseConnection,
  runId: string,
  batch: FileRow[]
): Promise<void> {
  type Delta = {
    size: number;
    files: number;
    dirs: number;
    maxDepth: number;
    deepest: string | null;
  };
  const deltas = new Map<string, Delta>();
  const ensure = (p: string): Delta => {
    let d = deltas.get(p);
    if (!d) {
      d = { size: 0, files: 0, dirs: 0, maxDepth: 0, deepest: null };
      deltas.set(p, d);
    }
    return d;
  };

  for (const row of batch) {
    const parts = row.path.split('/').filter(Boolean);
    const segs = parts.length;
    if (segs === 0) continue;
    // Icicle weight = CONTENT size (see the matching CASE in populateDirStats): archives
    // get width from their entries' uncompressed content; the container itself contributes
    // 0 (its entries carry it) unless it was never expanded, then its compressed footprint.
    const size = row.is_directory
      ? 0
      : row.is_archive_container
        ? row.extraction_error != null
          ? (row.physical_size ?? 0)
          : 0
        : (row.content_size ?? row.physical_size ?? 0);
    // Attribute the item to each ancestor directory (strict path prefix), exactly
    // like populateDirStats's generate_series(1, segs - 1).
    for (let i = 1; i < segs; i++) {
      const anc = ensure(parts.slice(0, i).join('/'));
      const relDepth = segs - i; // how many levels this item sits below the ancestor
      if (row.is_directory) anc.dirs += 1;
      else {
        anc.size += size;
        anc.files += 1;
      }
      // deepest = argmax by depth, tie-broken by the lexically-larger path (matches
      // populateDirStats's MAX(lpad(segs) || path)).
      if (
        relDepth > anc.maxDepth ||
        (relDepth === anc.maxDepth && (anc.deepest === null || row.path > anc.deepest))
      ) {
        anc.maxDepth = relDepth;
        anc.deepest = row.path;
      }
    }
    // Every directory gets its own row even with no descendants (the UNION ALL in
    // populateDirStats); folds in as a 0-delta and merges with descendant deltas.
    if (row.is_directory) ensure(row.path);
  }

  if (deltas.size === 0) return;

  const values = [...deltas.entries()].map(
    ([path, d]) =>
      sql`(${runId}, ${path}, ${d.size}, ${d.files}, ${d.dirs}, ${d.maxDepth}, ${d.deepest})`
  );
  await connection.db.execute(sql`
    INSERT INTO dir_stats (run_id, path, total_size, file_count, dir_count, max_depth, deepest_path)
    VALUES ${sql.join(values, sql`, `)}
    ON CONFLICT (run_id, path) DO UPDATE SET
      total_size = dir_stats.total_size + EXCLUDED.total_size,
      file_count = dir_stats.file_count + EXCLUDED.file_count,
      dir_count  = dir_stats.dir_count  + EXCLUDED.dir_count,
      deepest_path = CASE
        WHEN EXCLUDED.max_depth > dir_stats.max_depth THEN EXCLUDED.deepest_path
        WHEN EXCLUDED.max_depth = dir_stats.max_depth AND EXCLUDED.deepest_path > dir_stats.deepest_path THEN EXCLUDED.deepest_path
        ELSE dir_stats.deepest_path
      END,
      max_depth = GREATEST(dir_stats.max_depth, EXCLUDED.max_depth)
  `);
}

/**
 * Frontier (Phase 7 increment 2): stamp `enumerated_at` on a set of enumeration units
 * (directory or archive-container paths) whose children are now ALL durably committed.
 * Runs in its OWN transaction, AFTER the batch that committed those children — never
 * before. That ordering is the safety property: a crash (or a failed mark) between the
 * insert and the mark leaves the unit un-stamped, so resume re-enumerates it (idempotent
 * re-insert + re-stamp). We therefore never need insert+mark in one txn for correctness;
 * the only cost is one small extra UPDATE per batch. Chunked to keep the IN-list bounded.
 */
export async function markEnumerated(
  connection: DatabaseConnection,
  runId: string,
  unitPaths: string[]
): Promise<void> {
  if (!unitPaths.length) return;
  const at = Math.floor(Date.now() / 1000);
  for (let i = 0; i < unitPaths.length; i += 1000) {
    const chunk = unitPaths.slice(i, i + 1000);
    await connection.db
      .update(files)
      .set({ enumerated_at: at })
      .where(and(eq(files.run_id, runId), inArray(files.path, chunk)));
  }
}

/**
 * Resume seed — directories still on the frontier (children not all committed). The
 * walker re-readdirs exactly these (plus root); enumerated subtrees are read from the DB,
 * never re-listed. Backed by the partial idx_files_frontier index.
 */
export function getFrontierDirs(
  connection: DatabaseConnection,
  runId: string
): Observable<string[]> {
  return defer(() =>
    from(
      connection.db
        .select({ path: files.path })
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            eq(files.is_directory, true),
            // Real filesystem directories only. Directory ENTRIES inside an archive carry
            // is_directory=true but are produced by expanding their container (never
            // readdir'd), so they're not frontier units and stay enumerated_at=NULL — must
            // not be re-listed on resume.
            isNull(files.archive_parent_path),
            isNull(files.enumerated_at)
          )
        )
    ).pipe(
      map(rows => rows.map(r => r.path)),
      catchError(error => {
        logger.error('Failed to load frontier dirs', error as Error, { runId });
        return of([]);
      })
    )
  );
}

/**
 * Resume seed — the set of ALREADY-enumerated directory paths. During the resume walk,
 * a subdirectory found inside a frontier dir is re-pushed ONLY if it is not in this set,
 * so completed subtrees are never re-walked even though their parent was on the frontier.
 */
export function getEnumeratedDirs(
  connection: DatabaseConnection,
  runId: string
): Observable<string[]> {
  return defer(() =>
    from(
      connection.db
        .select({ path: files.path })
        .from(files)
        .where(
          and(eq(files.run_id, runId), eq(files.is_directory, true), isNotNull(files.enumerated_at))
        )
    ).pipe(
      map(rows => rows.map(r => r.path)),
      catchError(error => {
        logger.error('Failed to load enumerated dirs', error as Error, { runId });
        return of([]);
      })
    )
  );
}

/**
 * Resume seed — archive containers still on the frontier (their inner entries weren't all
 * committed). These re-expand by re-reading the archive file directly, NOT by re-walking,
 * because an archive can be un-enumerated even under a fully-enumerated parent directory
 * (the container row is a child of its parent dir; its inner entries are a separate unit).
 */
export function getFrontierArchives(
  connection: DatabaseConnection,
  runId: string
): Observable<FileSelect[]> {
  return defer(() =>
    from(
      connection.db
        .select()
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            eq(files.is_archive_container, true),
            isNull(files.enumerated_at)
          )
        )
    ).pipe(
      catchError(error => {
        logger.error('Failed to load frontier archives', error as Error, { runId });
        return of([]);
      })
    )
  );
}

/**
 * Resume seed for the canonical committed counts: the already-committed rows the resumed
 * walk will NOT re-emit, split by type, so the live count continues exactly where the
 * paused run left off (seed + re-walked remainder = an uninterrupted run's counts).
 *
 * A row will be RE-EMITTED on resume (and re-counted by the walk) when:
 *   - filesystem row: it's top-level (root is always re-listed), OR its parent directory
 *     is un-stamped (that dir will be re-listed), OR it's an un-stamped archive container
 *     (re-expanded via a synthetic entry);
 *   - archive entry: its container is un-stamped (the container is re-expanded).
 * Everything else is durable and un-re-emitted → seeded. Typed by the canonical
 * definitions (files = non-dir incl. archive entries; folders = dir incl. archive dirs).
 */
/** Raw count row from a PGlite aggregate (int → number, bigint → string). */
type ScanCountsRow = { files: unknown; folders: unknown; archiveEntries: unknown; bytes: unknown } | undefined;

/** Normalize a raw count row to canonical ScanCounts (numbers). */
function rowToScanCounts(row: ScanCountsRow): ScanCounts {
  return {
    files: Number(row?.files ?? 0),
    folders: Number(row?.folders ?? 0),
    archiveEntries: Number(row?.archiveEntries ?? 0),
    bytes: Number(row?.bytes ?? 0),
  };
}

export function getResumeSeedCounts(
  connection: DatabaseConnection,
  runId: string
): Observable<ScanCounts> {
  return defer(() =>
    from(
      connection.db.execute(sql`
        SELECT
          count(*) FILTER (WHERE f.is_directory = false)::int AS files,
          count(*) FILTER (WHERE f.is_directory = true)::int AS folders,
          count(*) FILTER (WHERE f.is_directory = false AND f.archive_parent_path IS NOT NULL)::int AS "archiveEntries",
          COALESCE(sum(f.physical_size) FILTER (WHERE f.is_directory = false), 0)::bigint AS bytes
        FROM files f
        WHERE f.run_id = ${runId}
          AND CASE
            WHEN f.archive_parent_path IS NULL THEN
              position('/' in f.path) > 0
              AND NOT EXISTS (
                SELECT 1 FROM files u
                WHERE u.run_id = ${runId}
                  AND u.is_directory = true
                  AND u.archive_parent_path IS NULL
                  AND u.enumerated_at IS NULL
                  AND u.path = left(f.path, length(f.path) - position('/' in reverse(f.path)))
              )
              AND NOT (f.is_archive_container = true AND f.enumerated_at IS NULL)
            ELSE
              -- An archive entry is re-emitted iff its CONTAINER is re-emitted (the walk
              -- re-lists the container's dir → re-emits the container → re-expands all its
              -- entries). So an entry won't re-emit iff its container won't: container not
              -- top-level AND the container's parent directory is stamped.
              position('/' in f.archive_parent_path) > 0
              AND NOT EXISTS (
                SELECT 1 FROM files u
                WHERE u.run_id = ${runId}
                  AND u.is_directory = true
                  AND u.archive_parent_path IS NULL
                  AND u.enumerated_at IS NULL
                  AND u.path = left(f.archive_parent_path, length(f.archive_parent_path) - position('/' in reverse(f.archive_parent_path)))
              )
          END
      `)
    ).pipe(
      map(result => rowToScanCounts(result.rows[0])),
      catchError(error => {
        logger.error('Failed to compute resume seed counts', error as Error, { runId });
        return of({ files: 0, folders: 0, archiveEntries: 0, bytes: 0 }); // never block resume
      })
    )
  );
}

/** Total rows committed for a run (files + directories + archive entries) — the exact
 *  figure for scan_metadata.file_count at completion, correct for resumed runs too
 *  (the in-memory session counter only sees the resumed remainder). */
export function countRunRows(
  connection: DatabaseConnection,
  runId: string
): Observable<number> {
  return defer(() =>
    from(
      connection.db.execute(
        sql`SELECT count(*)::int AS n FROM files WHERE run_id = ${runId}`
      )
    ).pipe(
      map(result => Number((result.rows[0] as { n: number | string } | undefined)?.n ?? 0)),
      catchError(error => {
        logger.error('Failed to count run rows', error as Error, { runId });
        return of(-1); // sentinel: caller falls back to its session counter
      })
    )
  );
}

/** Bulk-persist the scan's skip ledger (units that couldn't be fully processed). Called ONCE
 *  at finalize on the owner connection (the pipeline has completed → no interleave with inserts).
 *  ON CONFLICT DO NOTHING keeps it idempotent across resumes; paths are sanitized like file rows. */
export async function insertScanSkips(
  connection: DatabaseConnection,
  runId: string,
  skips: Array<{ kind: string; path: string; reason: string }>
): Promise<void> {
  if (skips.length === 0) return;
  const at = Math.floor(Date.now() / 1000);
  const CHUNK = 500;
  for (let i = 0; i < skips.length; i += CHUNK) {
    const chunk = skips.slice(i, i + CHUNK);
    const values: string[] = [];
    const params: unknown[] = [];
    chunk.forEach((s, j) => {
      const b = j * 4;
      values.push(`($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}, ${at})`);
      params.push(runId, sanitizeDbText(s.path), s.kind, s.reason);
    });
    await connection.pg.query(
      `INSERT INTO scan_skips (run_id, path, kind, reason, at) VALUES ${values.join(', ')} ON CONFLICT (run_id, path) DO NOTHING`,
      params
    );
  }
}

// ── Warm-start template ──────────────────────────────────────────────────────
// Opening a fresh datadir runs PGlite's initdb (~2.5–3 s — the bulk of the DB cold
// start). Instead we keep ONE pre-initialized template PGDATA (clean: schema only,
// no rows) and a new db just COPIES it (~125 ms) then opens it with initdb already
// done. The template is built lazily in the BACKGROUND so the very first scan isn't
// penalised (it inits normally and seeds the template for next time). Fully optional:
// if the template isn't ready yet, createDatabase falls back to a normal init.
let _templateBuilding = false;

function templateDir(): string {
  return path.join(path.dirname(path.resolve(createDatabasePath('_t'))), '.dbtemplate');
}

// Fingerprint of the schema shape a template was built with. A template whose shape differs from
// the current code's is NOT copied into new scans — it's ignored (fresh init instead) and rebuilt
// in the background. Derived from the descriptor below rather than a hand-bumped v1/v2 counter: edit
// the descriptor to match any DDL change (the size columns are BIGINT here — an earlier build made
// them int4, which overflowed on files/archives >2GB and stranded the scan) and the marker follows
// automatically. Pre-release, so no migration of existing datadirs — there's nothing to keep.
const TEMPLATE_SCHEMA_DESCRIPTOR = [
  'files(run_id:text,path:text,physical_size:bigint,content_size:bigint,mtime:bigint,is_directory:bool,is_hidden:bool,is_system:bool,hash:text,enumerated_at:int,is_archive_container:bool,archive_parent_path:text,archive_depth:int,archive_format:text,extraction_error:text,pk[run_id,path])',
  'scan_metadata(run_id:text,root_path:text,started_at:int,completed_at:int,file_count:int,status:text,pk[run_id])',
  'dir_stats(run_id:text,path:text,total_size:bigint,file_count:bigint,dir_count:bigint,max_depth:int,deepest_path:text,pk[run_id,path])',
  'scan_skips(run_id:text,path:text,kind:text,reason:text,at:int,pk[run_id,path])',
].join(';');
const TEMPLATE_SCHEMA_VERSION = createHash('sha1').update(TEMPLATE_SCHEMA_DESCRIPTOR).digest('hex').slice(0, 12);

/** True iff a warm-start template exists AND matches the current schema version. A template from
 *  an older schema is treated as absent (so its stale shape is never copied into a new scan). */
async function isTemplateReady(): Promise<boolean> {
  try {
    return (await fs.readFile(`${templateDir()}.ready`, 'utf8')).trim() === TEMPLATE_SCHEMA_VERSION;
  } catch {
    return false;
  }
}

async function pathExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

/** True only for a complete PGDATA. A `.ready` marker can outlive a half-written template dir, so
 *  verify the three artifacts initdb always produces before trusting a datadir: a non-empty
 *  global/pg_control, PG_VERSION, and base/. */
async function isValidPgdata(dir: string): Promise<boolean> {
  try {
    const [ctrl] = await Promise.all([
      fs.stat(path.join(dir, 'global', 'pg_control')),
      fs.access(path.join(dir, 'PG_VERSION')),
      fs.access(path.join(dir, 'base')),
    ]);
    return ctrl.size > 0;
  } catch {
    return false;
  }
}

/** Build the clean template PGDATA once (background, fire-and-forget). No-op if ready.
 *  Call this when the process is IDLE (e.g. right after a scan completes) — its initdb
 *  blocks the single JS thread, so it must NOT overlap an active scan. */
export async function ensureTemplateInBackground(): Promise<void> {
  const dir = templateDir();
  const ready = `${dir}.ready`;
  if (_templateBuilding || (await isTemplateReady())) return; // current-version template present
  _templateBuilding = true;
  const build = `${dir}.building-${process.pid}`;
  try {
    // The `.ready` marker is a COMMIT record, written last. Remove it up front: from now until a
    // fully-built, verified template is in place, isTemplateReady() is false and nothing copies a
    // half-written template. If the build crashes anywhere below, we rebuild next idle — never poison.
    await fs.rm(ready, { force: true });
    await fs.rm(build, { recursive: true, force: true });
    await ensureDirectory(build);
    const pg = new PGlite({ dataDir: build, ...(await getStandalonePGliteOptions()) });
    await pg.waitReady;
    await initializeSchema(drizzle(pg, { schema: { files } }));
    await pg.close(); // flushes global/pg_control
    // Only publish a template that verifies as a complete PGDATA: a partial one would copy itself
    // into every new scan.
    if (!(await isValidPgdata(build))) {
      throw new Error('built template is not a complete PGDATA (missing pg_control/PG_VERSION/base)');
    }
    await fs.rm(dir, { recursive: true, force: true });
    await fs.rename(build, dir); // atomic swap into place
    await fs.writeFile(ready, TEMPLATE_SCHEMA_VERSION); // commit AFTER the dir is valid + in place
    logger.debug('Warm-start template built', { dir });
  } catch (error) {
    // Leave nothing behind that could later look ready; `.ready` was already removed above.
    await fs.rm(build, { recursive: true, force: true }).catch(() => {});
    logger.warn('Warm-start template build failed; using normal init', {
      error: (error as Error).message,
    });
  } finally {
    _templateBuilding = false;
  }
}

/**
 * Create database connection. A fresh datadir is seeded from the warm-start template
 * (copy, no initdb) when one is ready; otherwise it inits normally and triggers a
 * background template build for next time.
 */
export async function createDatabase(name: string): Promise<DatabaseConnection> {
  try {
    const dbPath = createDatabasePath(name);
    const resolvedPath = path.resolve(dbPath);

    logger.debug('Creating database', { name, dbPath, resolvedPath });

    const fresh = !(await pathExists(resolvedPath));
    // Warm path only if the template is BOTH marked ready AND structurally a complete PGDATA — the
    // second check is belt-and-suspenders against a stale-but-matching marker over a corrupt dir.
    const warm = fresh && (await isTemplateReady()) && (await isValidPgdata(templateDir()));
    if (warm) {
      // Copy the pre-initialized template instead of running initdb. A stale-schema template (older
      // version marker) is ignored, so its old column shapes never seed a new scan — we init fresh.
      await fs.cp(templateDir(), resolvedPath, { recursive: true });
    } else {
      await ensureDirectory(resolvedPath);
    }

    // Initialize PGlite with explicit dataDir for standalone compatibility. Opening a
    // copied (already-initialized) datadir skips initdb; a fresh one inits as before.
    const openOptions = await getStandalonePGliteOptions();
    let pg = new PGlite({ dataDir: resolvedPath, ...openOptions });
    try {
      await pg.waitReady;
    } catch (openError) {
      // A fresh init failing is a genuine problem, so surface it. A warm copy that won't open means
      // the template itself is corrupt: discard the copy, delete the template so it can't affect the
      // next scan, and retry with a clean init.
      if (!warm) throw openError;
      logger.warn('Warm template copy failed to open; discarding template and initializing fresh', {
        error: (openError as Error).message,
      });
      await pg.close().catch(() => {});
      await fs.rm(resolvedPath, { recursive: true, force: true });
      await fs.rm(`${templateDir()}.ready`, { force: true });
      await fs.rm(templateDir(), { recursive: true, force: true });
      await ensureDirectory(resolvedPath);
      pg = new PGlite({ dataDir: resolvedPath, ...openOptions });
      await pg.waitReady;
    }

    const db = drizzle(pg, { schema: { files } });

    // Idempotent — confirms schema on the copied template and catches any drift.
    await initializeSchema(db);

    logger.info('Database created', { name, path: resolvedPath });

    return { name, pg, db };
  } catch (error) {
    logger.error('Failed to create database', error as Error, {
      name,
      dbPath: createDatabasePath(name),
    });
    throw error;
  }
}

/**
 * Close database connection
 */
export async function closeDatabase(connection: DatabaseConnection): Promise<void> {
  try {
    await connection.pg.close();
    logger.debug('Database connection closed');
  } catch (error) {
    logger.warn('Warning during database close', { error: (error as Error).message });
  }
}

// === Reactive Database Operations ===

/**
 * Clean database by recreating it with fresh schema
 * This ensures schema compatibility and removes all old data
 */
export function cleanDatabase(
  connection: DatabaseConnection,
  runId: string,
  preserveConnection = false,
  resume = false
): Observable<void> {
  return defer(async () => {
    // Single-owner session: the connection is shared with live readers, so it must
    // NOT be closed and its datadir must NOT be deleted (that would pull the
    // connection out from under in-flight reads AND drop the enrichment tables the
    // queries join). Clear the scan-data tables in place instead; the schema +
    // enrichment tables created at startup stay intact.
    if (preserveConnection) {
      logger.debug('Clearing scan data in place (preserveConnection)', {
        runId,
        resume,
        dbName: connection.name,
      });
      // dir_stats is always cleared: the re-walk re-emits every file, so the
      // incremental rollup rebuilds it from scratch with no double-count.
      await connection.db.execute(sql`DELETE FROM dir_stats`);
      // Keep THIS run's scan_metadata (written at scan start so the root path — and thus
      // the live tree's identity — is available DURING the scan); clear only stale runs.
      await connection.db.execute(sql`DELETE FROM scan_metadata WHERE run_id <> ${runId}`);
      // Resume: KEEP the partial `files` rows so discovery skips them (onConflictDoNothing)
      // and hashing continues from `hash IS NULL`. Fresh scan: wipe them.
      if (!resume) {
        await connection.db.execute(sql`DELETE FROM files`);
      }
      return void 0;
    }

    logger.debug('Recreating database for fresh schema', { runId, dbName: connection.name });

    try {
      // Close current connection
      await connection.pg.close();
      logger.debug('Closed existing database connection');

      // Recreate database with fresh schema
      const dbPath = createDatabasePath(connection.name);

      // Delete existing database directory
      try {
        await fs.rm(path.resolve(dbPath), { recursive: true, force: true });
        logger.debug('Removed old database files', { path: dbPath });
      } catch (_error) {
        // Ignore if directory doesn't exist
        logger.debug('No existing database to remove', { path: dbPath });
      }

      // Create fresh database
      const resolvedPath = path.resolve(dbPath);
      await ensureDirectory(resolvedPath);

      const newPg = new PGlite({
        dataDir: resolvedPath,
        ...(await getStandalonePGliteOptions()),
      });
      await newPg.waitReady;
      const newDb = drizzle(newPg, { schema: { files } });

      // Initialize fresh schema
      await initializeSchema(newDb);

      // Update connection object in place to maintain API compatibility
      connection.pg = newPg;
      connection.db = newDb;

      logger.debug('Database recreated with fresh schema', { runId, dbName: connection.name });
      return void 0;
    } catch (error) {
      logger.error('Failed to recreate database', error as Error, { runId });
      throw error;
    }
  }).pipe(
    catchError(error => {
      logger.error('Database recreation failed', error as Error, { runId });
      return EMPTY;
    })
  );
}

/**
 * Make a string safe for a Postgres `text` column: Postgres text cannot hold a NUL byte, and
 * a lone/unpaired UTF-16 surrogate encodes to invalid UTF-8 and is rejected on the wire. Both
 * occur in the wild in archive-entry names read from truncated/corrupt archives — and a single
 * such value would otherwise reject the whole multi-row insert. Replace with U+FFFD rather than
 * drop, so the row still lands (a mangled name beats a stranded scan). No-op for clean strings.
 */
export function sanitizeDbText<T extends string | null | undefined>(s: T): T {
  if (s == null || (typeof s === 'string' && s.length === 0)) return s;
  let out = s as string;
  if (out.indexOf('\x00') >= 0) out = out.replace(/\x00/g, '�');
  // Unpaired high or low surrogate → invalid UTF-8 on encode.
  if (/[\uD800-\uDFFF]/.test(out)) {
    out = out.replace(
      /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g,
      '�'
    );
  }
  return out as T;
}

/** Pull the REAL cause out of a Drizzle/PGlite error (the underlying Postgres message —
 *  `invalid byte sequence…`, `value too long…`), not Drizzle's giant `Failed query: <SQL>`
 *  wrapper. Trimmed so a 2 MB SQL dump never lands in the logs. */
function pgCause(err: unknown): string {
  const e = err as { cause?: unknown; message?: string } | undefined;
  const c = e?.cause as { message?: string } | string | undefined;
  const msg = (typeof c === 'string' ? c : c?.message) ?? e?.message ?? String(err);
  return msg.length > 300 ? msg.slice(0, 300) + '…' : msg;
}

/** Outcome of a (possibly-salvaged) batch insert: which rows durably committed, and which were
 *  quarantined (a poison row we chose to skip rather than let it strand the whole batch). */
export interface BatchInsertResult {
  committed: FileRow[];
  skipped: Array<{ row: FileRow; cause: string }>;
}

/**
 * Insert `rows` in ONE transaction; on a deterministic failure, bisect and retry each half so a
 * single poison row can't reject hundreds of good ones. Each level retries once first to ride out
 * a transient blip (PGlite is in-process, so failures are almost always deterministic data). A
 * single row that still fails is quarantined (recorded, not written). Recursion depth is O(log n).
 */
async function insertRowsSalvaging(
  connection: DatabaseConnection,
  rows: FileRow[]
): Promise<BatchInsertResult> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await connection.db.transaction(async tx => {
        await tx.insert(files).values(rows).onConflictDoNothing();
      });
      return { committed: rows, skipped: [] };
    } catch (err) {
      lastErr = err;
    }
  }
  const cause = pgCause(lastErr);
  if (rows.length === 1) {
    logger.warn('row quarantined: insert failed (unsalvageable single row)', {
      cause,
      path: rows[0].path,
      isDir: rows[0].is_directory,
      isArchiveContainer: rows[0].is_archive_container ?? false,
    });
    return { committed: [], skipped: [{ row: rows[0], cause }] };
  }
  logger.warn('batch insert failed; bisecting to salvage good rows', {
    cause,
    size: rows.length,
    firstPath: rows[0].path,
    lastPath: rows[rows.length - 1].path,
  });
  const mid = rows.length >> 1;
  const a = await insertRowsSalvaging(connection, rows.slice(0, mid));
  const b = await insertRowsSalvaging(connection, rows.slice(mid));
  return { committed: [...a.committed, ...b.committed], skipped: [...a.skipped, ...b.skipped] };
}

/**
 * Insert a batch of files, salvaging good rows around any poison ones (see
 * {@link insertRowsSalvaging}). Returns which rows committed vs. were skipped so the caller can
 * keep its counts + frontier accounting exact — a failed row is skipped, never a stranded scan.
 */
export function insertFileBatch(
  connection: DatabaseConnection,
  fileRows: FileRow[]
): Observable<BatchInsertResult> {
  if (!fileRows.length) return of({ committed: [], skipped: [] });

  return defer(() => {
    logger.debug('Inserting file batch', { count: fileRows.length });
    return from(insertRowsSalvaging(connection, fileRows)).pipe(
      tap(r => {
        if (r.skipped.length) {
          logger.warn('batch salvage: rows quarantined', {
            committed: r.committed.length,
            skipped: r.skipped.length,
          });
        } else {
          logger.debug('Files inserted', { inserted: r.committed.length });
        }
      }),
      catchError(error => {
        // insertRowsSalvaging catches per-row; reaching here means the recursion itself threw
        // unexpectedly. Treat the whole batch as skipped so the pipeline continues, never crashes.
        logger.error('Failed to insert file batch (salvage threw)', error as Error, {
          count: fileRows.length,
          cause: pgCause(error),
        });
        return of<BatchInsertResult>({
          committed: [],
          skipped: fileRows.map(row => ({ row, cause: pgCause(error) })),
        });
      })
    );
  });
}

/**
 * Update hash for files
 */
export function updateFileHash(
  connection: DatabaseConnection,
  runId: string,
  filePath: string,
  hash: string
): Observable<void> {
  return defer(() => {
    return from(
      connection.db
        .update(files)
        .set({ hash })
        .where(and(eq(files.run_id, runId), eq(files.path, filePath)))
    ).pipe(
      map(() => void 0),
      catchError(error => {
        logger.error('Failed to update file hash', error as Error, { runId, filePath });
        return EMPTY;
      })
    );
  });
}

/**
 * Content sizes shared by 2+ files of a run, as a SQL subquery to pass to `inArray`.
 * Never pass the sizes as a JS array instead: drizzle binds one parameter per element and
 * Postgres caps a statement at 65,535 parameters, which a multi-million-file scan exceeds.
 */
export function duplicateSizesSubquery(connection: DatabaseConnection, runId: string) {
  return connection.db
    .select({ size: files.content_size })
    .from(files)
    .where(
      and(
        eq(files.run_id, runId),
        isNotNull(files.content_size) // Only files with content_size (excludes archives/directories)
      )
    )
    .groupBy(files.content_size)
    .having(sql`count(*) > 1`);
}

/**
 * Row-level form of {@link duplicateSizesSubquery}: true when another file of the same run
 * shares this row's content_size. Correlated on the content_size index, so a path-ordered
 * page stops after LIMIT rows instead of re-aggregating the whole run on every page.
 */
export function hasDuplicateContentSize(connection: DatabaseConnection) {
  const other = alias(files, 'other');
  return exists(
    connection.db
      .select({ one: sql`1` })
      .from(other)
      .where(
        and(
          eq(other.run_id, files.run_id),
          eq(other.content_size, files.content_size),
          ne(other.path, files.path)
        )
      )
  );
}

/**
 * Find files with duplicate content sizes (candidates for hash calculation)
 * Uses content_size for deduplication as per specification
 */
export function findDuplicateSizes(
  connection: DatabaseConnection,
  runId: string
): Observable<number[]> {
  return defer(() => {
    logger.debug('Finding duplicate content sizes', { runId });

    return from(duplicateSizesSubquery(connection, runId)).pipe(
      map(results => results.map(r => r.size!).filter(s => s !== null)), // Filter out nulls and ensure number[]
      tap(sizes => logger.debug('Found duplicate content sizes', { count: sizes.length })),
      catchError(error => {
        logger.error('Failed to find duplicate content sizes', error as Error, { runId });
        return of([]);
      })
    );
  });
}

/**
 * Get files that need hashing (based on content_size duplicates)
 */
export function getFilesNeedingHash(
  connection: DatabaseConnection,
  runId: string
): Observable<string[]> {
  return defer(() => {
    return from(
      connection.db
        .select({ path: files.path })
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            isNull(files.hash),
            inArray(files.content_size, duplicateSizesSubquery(connection, runId))
          )
        )
    ).pipe(
      map(results => results.map(r => r.path)),
      catchError(error => {
        logger.error('Failed to get files needing hash', error as Error, { runId });
        return of([]);
      })
    );
  });
}

/**
 * Count real duplicate groups by hash
 * Returns the number of hash groups that have duplicates (2+ files with same hash)
 */
export function countRealDuplicateGroups(
  connection: DatabaseConnection,
  runId: string
): Observable<number> {
  return defer(() => {
    logger.debug('Counting real duplicate groups by hash', { runId });

    return from(
      connection.db
        .select({
          hash: files.hash,
          fileCount: count(),
        })
        .from(files)
        .where(and(eq(files.run_id, runId), isNotNull(files.hash)))
        .groupBy(files.hash)
        .having(sql`count(*) > 1`)
    ).pipe(
      map(results => results.length), // Count of hash groups with duplicates
      tap(duplicateGroups =>
        logger.debug('Real duplicate groups counted', { runId, duplicateGroups })
      ),
      catchError(error => {
        logger.error('Failed to count real duplicate groups', error as Error, { runId });
        return of(0);
      })
    );
  });
}

/**
 * Get scan statistics
 */
export function getScanStats(connection: DatabaseConnection, runId: string): Observable<ScanStats> {
  return defer(() => {
    logger.debug('Getting scan statistics', { runId });

    // Get basic stats. Canonical split (NOT COUNT(*)): files = non-directory rows
    // (real files + archive entries), folders = directory rows — the numbers every
    // surface shows, so the panel/status bar/tree can never disagree.
    return from(
      connection.db
        .select({
          totalFiles: sql<number>`count(*) FILTER (WHERE ${files.is_directory} = false)`,
          totalFolders: sql<number>`count(*) FILTER (WHERE ${files.is_directory} = true)`,
          totalPhysicalSize: sum(files.physical_size),
          totalContentSize: sum(files.content_size),
        })
        .from(files)
        .where(eq(files.run_id, runId))
    ).pipe(
      switchMap(basicStats => {
        const totalFiles = Number(basicStats[0]?.totalFiles) || 0;
        const totalFolders = Number(basicStats[0]?.totalFolders) || 0;
        const totalPhysicalSize = Number(basicStats[0]?.totalPhysicalSize) || 0;
        const totalContentSize = Number(basicStats[0]?.totalContentSize) || 0;

        // Get archive stats
        const archiveStatsQuery = connection.db
          .select({
            totalArchives: count(),
            archiveFormats: sql<
              string[]
            >`array_agg(DISTINCT archive_format) FILTER (WHERE archive_format IS NOT NULL)`,
          })
          .from(files)
          .where(and(eq(files.run_id, runId), eq(files.is_archive_container, true)));

        const archiveEntriesQuery = connection.db
          .select({
            totalArchiveEntries: count(),
          })
          .from(files)
          // Sub-count of `totalFiles`: archive entries that are FILES (non-dir), matching
          // the canonical definition — directories inside archives count as folders, not
          // archive entries. (See project-canonical-count-model.)
          .where(
            and(
              eq(files.run_id, runId),
              isNotNull(files.archive_parent_path),
              eq(files.is_directory, false)
            )
          );

        // Get duplicate stats
        const duplicateStatsQuery = connection.db
          .select({
            hash: files.hash,
            fileCount: count(),
          })
          .from(files)
          .where(and(eq(files.run_id, runId), isNotNull(files.hash)))
          .groupBy(files.hash)
          .having(sql`count(*) > 1`);

        return from(
          Promise.all([archiveStatsQuery, archiveEntriesQuery, duplicateStatsQuery])
        ).pipe(
          map(([archiveStats, archiveEntriesStats, duplicateGroups]) => {
            const duplicateGroupsCount = duplicateGroups.length;
            const duplicateFilesCount = duplicateGroups.reduce(
              (sum, group) => sum + (group.fileCount || 0),
              0
            );

            const totalArchives = archiveStats[0]?.totalArchives || 0;
            const totalArchiveEntries = archiveEntriesStats[0]?.totalArchiveEntries || 0;
            const archiveFormats = archiveStats[0]?.archiveFormats || [];

            return {
              totalFiles,
              totalFolders,
              totalPhysicalSize,
              totalContentSize,
              duplicateGroups: duplicateGroupsCount,
              duplicateFiles: duplicateFilesCount,
              totalArchives,
              totalArchiveEntries,
              archiveFormats: archiveFormats.filter(f => f !== null),
            };
          })
        );
      }),
      catchError(error => {
        logger.error('Failed to get scan statistics', error as Error, { runId });
        return of({
          totalFiles: 0,
          totalFolders: 0,
          totalPhysicalSize: 0,
          totalContentSize: 0,
          duplicateGroups: 0,
          duplicateFiles: 0,
          totalArchives: 0,
          totalArchiveEntries: 0,
          archiveFormats: [],
        });
      })
    );
  });
}

/**
 * Get all files for a run (for export/analysis)
 */
export function getAllFiles(
  connection: DatabaseConnection,
  runId: string
): Observable<FileSelect[]> {
  return defer(() => {
    return from(
      connection.db.select().from(files).where(eq(files.run_id, runId)).orderBy(files.path)
    ).pipe(
      catchError(error => {
        logger.error('Failed to get all files', error as Error, { runId });
        return of([]);
      })
    );
  });
}

/**
 * Get the most recent run_id from the database.
 * Uses descending lexicographic order which works because run_id format
 * is scan-{timestamp}-{random} and timestamps are monotonically increasing.
 */
export function getLatestRunId(connection: DatabaseConnection): Observable<string | null> {
  return defer(() => {
    return from(
      connection.db
        .selectDistinct({ run_id: files.run_id })
        .from(files)
        .orderBy(sql`${files.run_id} DESC`)
        .limit(1)
    ).pipe(
      map(results => (results.length > 0 ? results[0].run_id : null)),
      tap(runId => logger.debug('Retrieved latest run_id', { runId })),
      catchError(error => {
        logger.error('Failed to get latest run_id', error as Error);
        return of(null);
      })
    );
  });
}

/**
 * Get files in batches using keyset pagination.
 * Emits batches of files as an Observable stream for memory-efficient processing.
 *
 * @param connection Database connection
 * @param runId The scan run to retrieve files from
 * @param batchSize Number of files per batch (default: 5000)
 * @returns Observable that emits FileSelect[] batches
 */
export function getFilesBatched(
  connection: DatabaseConnection,
  runId: string,
  batchSize: number = 5000
): Observable<FileSelect[]> {
  return defer(() => {
    logger.debug('Starting batched file retrieval', { runId, batchSize });

    const fetchBatch = (lastPath: string | null): Observable<FileSelect[]> => {
      const query = lastPath
        ? connection.db
            .select()
            .from(files)
            .where(and(eq(files.run_id, runId), gt(files.path, lastPath)))
            .orderBy(files.path)
            .limit(batchSize)
        : connection.db
            .select()
            .from(files)
            .where(eq(files.run_id, runId))
            .orderBy(files.path)
            .limit(batchSize);

      return from(query).pipe(
        switchMap(batch => {
          if (batch.length === 0) {
            logger.debug('Batched retrieval complete', { runId });
            return EMPTY;
          }

          const newLastPath = batch[batch.length - 1].path;
          logger.debug('Retrieved batch', {
            runId,
            batchSize: batch.length,
            lastPath: newLastPath,
          });

          // Emit this batch, then recursively fetch next
          return concat(of(batch), fetchBatch(newLastPath));
        }),
        catchError(error => {
          logger.error('Failed to fetch file batch', error as Error, { runId, lastPath });
          return EMPTY;
        })
      );
    };

    return fetchBatch(null);
  });
}

/**
 * Database health check
 */
export function checkHealth(connection: DatabaseConnection): Observable<boolean> {
  return defer(() => {
    return from(connection.db.select({ count: count() }).from(files).limit(1)).pipe(
      map(() => true),
      catchError(() => of(false))
    );
  });
}

// === Scan Metadata Functions ===

/**
 * Insert scan metadata when a scan starts
 */
export function insertScanMetadata(
  connection: DatabaseConnection,
  runId: string,
  rootPath: string,
  startedAt: number
): Observable<void> {
  return defer(() => {
    return from(
      // onConflictDoNothing: on RESUME this row already exists (written at the original
      // scan start) — re-inserting must be a harmless no-op, not a PK violation.
      connection.db
        .insert(scanMetadata)
        .values({
          run_id: runId,
          root_path: rootPath,
          started_at: startedAt,
          status: 'running',
        })
        .onConflictDoNothing()
    ).pipe(
      map(() => void 0),
      tap(() => logger.debug('Scan metadata inserted', { runId, rootPath })),
      catchError(error => {
        logger.error('Failed to insert scan metadata', error as Error, { runId });
        return of(void 0);
      })
    );
  });
}

/**
 * Update scan metadata when a scan completes
 */
export function updateScanMetadata(
  connection: DatabaseConnection,
  runId: string,
  fileCount: number
): Observable<void> {
  return defer(() => {
    const completedAt = Math.floor(Date.now() / 1000);
    return from(
      connection.db
        .update(scanMetadata)
        .set({ completed_at: completedAt, file_count: fileCount, status: 'complete' })
        .where(eq(scanMetadata.run_id, runId))
    ).pipe(
      map(() => void 0),
      tap(() => logger.debug('Scan metadata updated', { runId, fileCount })),
      catchError(error => {
        logger.error('Failed to update scan metadata', error as Error, { runId });
        return of(void 0);
      })
    );
  });
}

/**
 * Set just the lifecycle status of a run (running | paused | complete | cancelled).
 * Used by the owner session's pause/resume/cancel control channel. Resume re-sets
 * 'running' (insertScanMetadata's onConflictDoNothing won't, since the row exists).
 */
export function setScanStatus(
  connection: DatabaseConnection,
  runId: string,
  status: 'running' | 'paused' | 'complete' | 'cancelled'
): Observable<void> {
  return defer(() =>
    from(
      connection.db.update(scanMetadata).set({ status }).where(eq(scanMetadata.run_id, runId))
    ).pipe(
      map(() => void 0),
      catchError(error => {
        logger.error('Failed to set scan status', error as Error, { runId, status });
        return of(void 0);
      })
    )
  );
}

/**
 * Get scan metadata for a run_id
 */
export function getScanMetadata(
  connection: DatabaseConnection,
  runId: string
): Observable<ScanMetadataSelect | null> {
  return defer(() => {
    return from(
      connection.db.select().from(scanMetadata).where(eq(scanMetadata.run_id, runId)).limit(1)
    ).pipe(
      map(results => (results.length > 0 ? results[0] : null)),
      tap(metadata => logger.debug('Retrieved scan metadata', { runId, hasMetadata: !!metadata })),
      catchError(error => {
        logger.error('Failed to get scan metadata', error as Error, { runId });
        return of(null);
      })
    );
  });
}

// === Factory Functions ===

export { createDatabase as createScanDatabase };
export { closeDatabase as closeScanDatabase };

/**
 * Simple database operations for CLI commands
 */
export const dbOperations = {
  create: createDatabase,
  close: closeDatabase,
  clean: cleanDatabase,
  insertBatch: insertFileBatch,
  updateHash: updateFileHash,
  findDuplicateSizes,
  getFilesNeedingHash,
  countRealDuplicates: countRealDuplicateGroups,
  getStats: getScanStats,
  getAllFiles,
  getLatestRunId,
  getFilesBatched,
  healthCheck: checkHealth,
  insertScanMetadata,
  updateScanMetadata,
  setScanStatus,
  getScanMetadata,
  markEnumerated,
  getFrontierDirs,
  getEnumeratedDirs,
  getFrontierArchives,
};
