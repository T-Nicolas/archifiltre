/**
 * Hash Calculator - Archive-Aware Streaming xxHash64 Implementation
 *
 * Calculates xxhash64 hashes for files with duplicate content_size using streaming processing.
 * Supports on-demand archive extraction for comprehensive duplicate detection across
 * regular files and archive contents. Uses bounded memory regardless of dataset size.
 *
 * Key design principles:
 * - Never load all file paths into memory
 * - Uses content_size for prefiltering (not physical_size)
 * - Archive-aware: extracts content on-demand from ZIP, TAR, 7Z, etc.
 * - Cross-platform path normalization for Windows/Unix compatibility
 * - Pure RxJS Observable pipeline for integration
 * - Batch processing with controlled concurrency
 * - Database as source of truth for all operations
 */

import * as os from 'node:os';
import type { Observable } from 'rxjs';
import { from, of, defer, range } from 'rxjs';
import {
  switchMap,
  mergeMap,
  concatMap,
  tap,
  map,
  filter,
  catchError,
  finalize,
  scan,
  last,
} from 'rxjs/operators';
import { logger } from '@lib/logging.ts';
import type { DatabaseConnection } from '@lib/database.ts';
import { duplicateSizesSubquery, hasDuplicateContentSize, files } from '@lib/database.ts';
import type { PipelineContext } from '@lib/pipeline-context.ts';
import { eq, and, count, gt, isNull, inArray, sql } from 'drizzle-orm';

import { createFileStream, type FileEntry } from '@lib/file-reader.ts';
import { pausable } from '@lib/pausable.ts';
import { createXXHash64 } from 'hash-wasm';
import type { Readable } from 'node:stream';

// Hash calculation configuration
export interface HashConfig {
  concurrency?: number;
  batchSize?: number;
  onProgress?: (processed: number, total: number, errors: number) => void;
  pauseSignal?: import('rxjs').Subject<void>;
  resumeSignal?: import('rxjs').Subject<void>;
  /** Read-priority hook called between hash batches (see PipelineContext). */
  betweenBatches?: () => Promise<void>;
}

// Hash calculation statistics
export interface HashStats {
  totalFiles: number;
  processed: number;
  errors: number;
  startTime: number;
  endTime?: number;
}

// Hash update item
export interface HashUpdate {
  path: string;
  hash: string;
}

// File entry for hashing with archive metadata
export interface FileHashEntry {
  path: string;
  archiveParentPath?: string;
  archiveFormat?: string;
}

/**
 * Default hashing configuration
 */
export const DEFAULT_HASH_CONFIG: Required<
  Omit<HashConfig, 'onProgress' | 'pauseSignal' | 'resumeSignal' | 'betweenBatches'>
> = {
  concurrency: Math.max(2, (os.cpus()?.length ?? 4) - 1),
  batchSize: 100, // Small batches to keep memory bounded
};

/**
 * Stream a file's bytes through incremental, standard xxHash64 (hash-wasm).
 *
 * Only the hasher's small internal state plus the current chunk are held in memory,
 * so peak RAM is O(chunk) regardless of file size. This is what prevents the whole-file
 * buffering (`fs.readFile` of every candidate) that used to OOM the duplicate-detection
 * phase on multi-GB files. hash-wasm inlines its WASM as base64, so it bundles cleanly
 * under `bun build --compile` on every target — no external .wasm to ship.
 *
 * Note: seed 0 yields the STANDARD XXH64 value (portable across tools/machines), unlike
 * the previous `Bun.hash(buffer, 'xxhash64')` which silently computed one-shot wyhash.
 */
async function hashStream(stream: Readable): Promise<string> {
  const hasher = await createXXHash64();
  hasher.init();
  try {
    for await (const chunk of stream) {
      hasher.update(chunk as Uint8Array);
    }
  } finally {
    // Release the fd promptly on the error path (for-await consumes it on success);
    // under concurrency a leaked descriptor per failed file adds up.
    const s = stream as Readable & { destroyed?: boolean };
    if (!s.destroyed) s.destroy();
  }
  return hasher.digest('hex'); // 16-char hex, matches the DB `hash` column
}

/**
 * Hash a single file with archive-aware processing and cross-platform path handling
 */
function hashSingleFile(rootPath: string, fileEntry: FileHashEntry): Observable<HashUpdate | null> {
  return defer(async () => {
    try {
      // Use shared file-reader abstraction for both regular files and archive entries
      const entry: FileEntry = {
        path: fileEntry.path,
        archiveParentPath: fileEntry.archiveParentPath,
        archiveFormat: fileEntry.archiveFormat,
      };

      if (fileEntry.archiveParentPath) {
        logger.debug('Hashing archive entry', {
          path: fileEntry.path,
          archive: fileEntry.archiveParentPath,
          format: fileEntry.archiveFormat,
        });
      }

      // Stream the bytes through the hasher — never materialise the whole file.
      // createFileStream gives a native fs stream for regular files (the OOM path)
      // and a bounded, already-extracted buffer stream for archive entries.
      const stream = await createFileStream(rootPath, entry);
      const hash = await hashStream(stream);

      return {
        path: fileEntry.path,
        hash,
      };
    } catch (error) {
      logger.warn('Hash calculation failed', {
        path: fileEntry.path,
        archiveParent: fileEntry.archiveParentPath,
        error: error instanceof Error ? error.message : String(error),
      });
      return null; // Signal failure but continue processing
    }
  }).pipe(
    catchError(error => {
      logger.warn('Hash calculation error', {
        path: fileEntry.path,
        archiveParent: fileEntry.archiveParentPath,
        error: error instanceof Error ? error.message : String(error),
      });
      return of(null);
    })
  );
}

/**
 * Get a batch of files that need hashing using streaming SQL
 * This avoids loading all file paths into memory
 * Now includes archive metadata for archive-aware processing
 *
 * Keyset-paginated on path (after `afterPath`), not OFFSET: hashed rows leave the
 * `hash IS NULL` set between batches, so an advancing offset would skip unhashed files.
 * Walks the primary key in path order with a per-row EXISTS, so each page costs O(page),
 * not a re-aggregation of the whole run.
 */
function getFilesBatch(
  connection: DatabaseConnection,
  runId: string,
  afterPath: string | null,
  batchSize: number
): Observable<FileHashEntry[]> {
  return defer(() => {
    return from(
      connection.db
        .select({
          path: files.path,
          archiveParentPath: files.archive_parent_path,
          archiveFormat: files.archive_format,
        })
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            isNull(files.hash), // Only files that don't have hash yet
            hasDuplicateContentSize(connection), // Only files with duplicate content_size
            afterPath === null ? undefined : gt(files.path, afterPath)
          )
        )
        .orderBy(files.path)
        .limit(batchSize)
    ).pipe(
      map(results =>
        results.map(r => ({
          path: r.path,
          archiveParentPath: r.archiveParentPath || undefined,
          archiveFormat: r.archiveFormat || undefined,
        }))
      ),
      catchError(error => {
        logger.error('Failed to get files batch', error as Error, {
          runId,
          afterPath,
          batchSize,
        });
        return of([]);
      })
    );
  });
}

/**
 * Get total count of files that need hashing
 */
function getTotalFilesNeedingHash(
  connection: DatabaseConnection,
  runId: string
): Observable<number> {
  return defer(() => {
    return from(
      connection.db
        .select({ total: count() })
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            isNull(files.hash),
            inArray(files.content_size, duplicateSizesSubquery(connection, runId))
          )
        )
    ).pipe(
      map(results => results[0]?.total || 0),
      catchError(error => {
        logger.error('Failed to get total files count', error as Error, {
          runId,
        });
        return of(0);
      })
    );
  });
}

/**
 * Refresh planner statistics for `files` before the hashing queries. PGlite runs Postgres
 * without autovacuum, so a freshly ingested run still looks empty to the planner, which then
 * nested-loops the duplicate-size subquery (minutes on ~100k rows, unbounded at millions).
 * Best-effort: a failure only costs speed.
 */
function analyzeFiles(connection: DatabaseConnection): Observable<void> {
  return defer(() => from(connection.db.execute(sql`ANALYZE files`))).pipe(
    map(() => void 0),
    catchError(error => {
      logger.warn('ANALYZE files failed, hashing queries may be slow', {
        error: error instanceof Error ? error.message : String(error),
      });
      return of(void 0);
    })
  );
}

/**
 * Update hash batch in database
 */
function updateHashBatch(
  connection: DatabaseConnection,
  runId: string,
  updates: HashUpdate[]
): Observable<number> {
  if (!updates.length) return of(0);

  return defer(() => {
    return from(
      connection.db.transaction(async tx => {
        let updated = 0;
        for (const update of updates) {
          try {
            await tx
              .update(files)
              .set({ hash: update.hash })
              .where(and(eq(files.run_id, runId), eq(files.path, update.path)));
            updated++;
          } catch (error) {
            logger.warn('Failed to update hash for file', {
              path: update.path,
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
        return updated;
      })
    ).pipe(
      catchError(error => {
        logger.error('Hash batch update failed', error as Error, {
          runId,
          batchSize: updates.length,
        });
        return of(0);
      })
    );
  });
}

/**
 * Process a single batch of files for hashing with archive-aware processing
 */
function processBatch(
  connection: DatabaseConnection,
  runId: string,
  rootPath: string,
  fileEntries: FileHashEntry[],
  concurrency: number
): Observable<number> {
  if (!fileEntries.length) return of(0);

  // Log batch composition for debugging
  const archiveEntries = fileEntries.filter(e => e.archiveParentPath).length;
  const regularEntries = fileEntries.length - archiveEntries;

  logger.debug('Processing hash batch', {
    runId,
    totalFiles: fileEntries.length,
    regularFiles: regularEntries,
    archiveEntries,
  });

  return from(fileEntries).pipe(
    mergeMap(fileEntry => hashSingleFile(rootPath, fileEntry), concurrency),
    filter((result): result is HashUpdate => result !== null),
    // Collect all hashes from this batch
    tap(update => {
      logger.debug('File hashed', {
        path: update.path,
        hash: update.hash,
      });
    }),
    // Convert to array for batch update
    map(update => [update]),
    // Reduce to single array
    scan((acc: HashUpdate[], updates: HashUpdate[]) => [...acc, ...updates], []),
    // Take the final accumulated result
    last(),
    // Update database with batch
    switchMap(updates =>
      updateHashBatch(connection, runId, updates).pipe(
        tap(updated => {
          logger.debug('Hash batch updated', {
            runId,
            updated,
            attempted: updates.length,
          });
        })
      )
    ),
    catchError(error => {
      logger.error('Batch processing failed', error as Error, {
        runId,
        batchSize: fileEntries.length,
      });
      return of(0);
    })
  );
}

/**
 * Streaming hash calculation pipeline
 * Processes files in batches to maintain bounded memory usage
 */
export function calculateHashes(
  connection: DatabaseConnection,
  runId: string,
  rootPath: string,
  config: HashConfig = {}
): Observable<void> {
  const finalConfig = { ...DEFAULT_HASH_CONFIG, ...config };

  const stats: HashStats = {
    totalFiles: 0,
    processed: 0,
    errors: 0,
    startTime: Date.now(),
  };

  logger.debug('Starting hash calculation', { runId, rootPath });

  // Get total count for progress reporting (0 also covers "no duplicate content sizes")
  return analyzeFiles(connection).pipe(
    switchMap(() => getTotalFilesNeedingHash(connection, runId)),
    tap(total => {
      stats.totalFiles = total;
      logger.debug('Total files needing hash calculation', {
        runId,
        totalFiles: total,
      });
    }),
    switchMap(totalFiles => {
      if (totalFiles === 0) {
        logger.debug('No files need hashing', { runId });
        return of(void 0);
      }

      // Process files in streaming batches. The candidate set is fixed for this run, so
      // ceil(total / batchSize) keyset pages cover it exactly, failed files included.
      let lastPath: string | null = null;

      return range(0, Math.ceil(totalFiles / finalConfig.batchSize)).pipe(
        pausable({ pauseSignal: finalConfig.pauseSignal, resumeSignal: finalConfig.resumeSignal }),
        concatMap(() => {
          const afterPath = lastPath;

          return getFilesBatch(connection, runId, afterPath, finalConfig.batchSize).pipe(
            switchMap(fileEntries => {
              if (!fileEntries.length) {
                return of(0); // No more files to process
              }
              // Advance past the whole page, hashed or not: a file that failed to hash stays
              // NULL and must not be fetched again.
              lastPath = fileEntries[fileEntries.length - 1].path;

              logger.debug('Processing hash batch', {
                runId,
                afterPath,
                batchSize: fileEntries.length,
              });

              return processBatch(
                connection,
                runId,
                rootPath,
                fileEntries,
                finalConfig.concurrency
              ).pipe(
                tap(processed => {
                  stats.processed += processed;

                  if (finalConfig.onProgress) {
                    finalConfig.onProgress(stats.processed, stats.totalFiles, stats.errors);
                  }
                })
              );
            }),
            // Read-priority: yield to any in-flight reads before the next hash
            // batch, so the UI stays responsive through the long hashing phase.
            finalConfig.betweenBatches
              ? switchMap((n: number) => from(finalConfig.betweenBatches!()).pipe(map(() => n)))
              : tap()
          );
        }),
        finalize(() => {
          stats.endTime = Date.now();
          const duration = (stats.endTime - stats.startTime) / 1000;

          logger.debug('Hash calculation completed', {
            runId,
            processed: stats.processed,
            total: stats.totalFiles,
            errors: stats.errors,
            duration: `${duration.toFixed(1)}s`,
            rate:
              stats.processed > 0
                ? `${(stats.processed / duration).toFixed(0)} files/second`
                : '0 files/second',
          });
        })
      );
    }),
    // Emit once when hashing is done, not once per batch: the scanner runs duplicate
    // detection and finalization on every emission.
    last(null, undefined),
    map(() => void 0),
    catchError(error => {
      logger.error('Hash calculation pipeline failed', error as Error, { runId });
      throw error;
    })
  );
}

/**
 * Convenience function for hash calculation with progress reporting
 */
export function performHashing(
  context: PipelineContext,
  progressCallback?: (processed: number, total: number, errors: number) => void
): Observable<void> {
  const { database: connection, runId, rootPath, onProgress, pauseSignal, resumeSignal, betweenBatches } = context;
  const combined =
    progressCallback || onProgress
      ? (processed: number, total: number, errors: number) => {
          progressCallback?.(processed, total, errors);
          onProgress?.('hashing', processed, total);
        }
      : undefined;
  return calculateHashes(connection, runId, rootPath, {
    onProgress: combined,
    pauseSignal,
    resumeSignal,
    betweenBatches,
  });
}

/**
 * Get hashing statistics and progress
 */
export function getHashingProgress(
  connection: DatabaseConnection,
  runId: string
): Observable<{ total: number; completed: number; remaining: number }> {
  return defer(() => {
    const duplicateSizes = duplicateSizesSubquery(connection, runId);

    // Get total files with duplicate content_size
    const totalQuery = connection.db
      .select({ count: count() })
      .from(files)
      .where(and(eq(files.run_id, runId), inArray(files.content_size, duplicateSizes)));

    // Get completed (already hashed) files
    const completedQuery = connection.db
      .select({ count: count() })
      .from(files)
      .where(
        and(
          eq(files.run_id, runId),
          inArray(files.content_size, duplicateSizes),
          sql`hash IS NOT NULL`
        )
      );

    return from(Promise.all([totalQuery, completedQuery])).pipe(
      map(([totalResult, completedResult]) => {
        const total = totalResult[0]?.count || 0;
        const completed = completedResult[0]?.count || 0;
        const remaining = total - completed;

        return { total, completed, remaining };
      })
    );
  }).pipe(
    catchError(error => {
      logger.error('Failed to get hashing progress', error as Error, { runId });
      return of({ total: 0, completed: 0, remaining: 0 });
    })
  );
}
