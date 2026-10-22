/**
 * Summary Extension
 *
 * Extension for displaying detailed scan results with file breakdowns
 */

import type { Command } from '@oclif/core';
import type { Observable} from 'rxjs';
import { from, defer, of } from 'rxjs';
import { map, catchError } from 'rxjs/operators';
import { getScanStats } from '@lib/database.ts';
import { eq, and, count, sum, gt, sql, isNotNull } from 'drizzle-orm';
import { logger } from '@lib/logging.ts';
import type { DatabaseConnection } from '@lib/database.ts';
import { files } from '@lib/database.ts';

/**
 * Get duplicate file statistics from database
 */
function getDuplicateStats(
  connection: DatabaseConnection,
  runId: string
): Observable<{ duplicateGroups: number; totalDuplicateFiles: number }> {
  return defer(() => {
    return from(
      connection.db
        .select({
          hash: files.hash,
          fileCount: count(),
        })
        .from(files)
        .where(
          and(
            eq(files.run_id, runId),
            eq(files.is_directory, false), // Only files, exclude directories
            isNotNull(files.hash) // Only files with hash (successfully processed)
          )
        )
        .groupBy(files.hash)
        .having(gt(count(), 1)) // Only hashes with more than 1 file (real duplicates)
    ).pipe(
      map(results => ({
        duplicateGroups: results.length,
        totalDuplicateFiles: results.reduce((sum, group) => sum + group.fileCount, 0),
      })),
      catchError(error => {
        logger.error('Failed to get duplicate statistics', error as Error, { runId });
        return of({ duplicateGroups: 0, totalDuplicateFiles: 0 });
      })
    );
  });
}

/**
 * Get empty files count from database
 */
function getEmptyFilesCount(connection: DatabaseConnection, runId: string): Observable<number> {
  return defer(() => {
    return from(
      connection.db
        .select({ count: count() })
        .from(files)
        .where(
          and(eq(files.run_id, runId), eq(files.content_size, 0), eq(files.is_directory, false))
        )
    ).pipe(
      map(results => results[0]?.count || 0),
      catchError(error => {
        logger.error('Failed to get empty files count', error as Error, { runId });
        return of(0);
      })
    );
  });
}

/**
 * Get folder statistics from database
 */
function getFolderStats(
  connection: DatabaseConnection,
  runId: string
): Observable<{ totalFolders: number; emptyFolders: number }> {
  return defer(() => {
    // Get total folders count
    const totalFoldersQuery = connection.db
      .select({ totalFolders: count() })
      .from(files)
      .where(and(eq(files.run_id, runId), eq(files.is_directory, true)));

    // Get empty folders: no row under 'folder/'. Expressed as the byte range
    // ['folder/', 'folder0') ('0' follows '/') with the text_pattern_ops operators, so each
    // folder is one probe of idx_files_path_pattern. A `LIKE folder || '/%'` pattern derived
    // from a column can't use an index: it scanned every row per folder (O(folders x files),
    // effectively unbounded on a multi-million-file scan), and read '_'/'%' in names as wildcards.
    const emptyFoldersQuery = connection.db
      .select({
        emptyFolders: sql<number>`COUNT(*)`,
      })
      .from(files)
      .where(
        and(
          eq(files.run_id, runId),
          eq(files.is_directory, true),
          sql`NOT EXISTS (
            SELECT 1 FROM files f2
            WHERE f2.run_id = ${runId}
              AND f2.path ~>=~ (${files.path} || '/')
              AND f2.path ~<~ (${files.path} || '0')
          )`
        )
      );

    return from(Promise.all([totalFoldersQuery, emptyFoldersQuery])).pipe(
      map(([totalResult, emptyResult]) => ({
        totalFolders: totalResult[0]?.totalFolders || 0,
        emptyFolders: emptyResult[0]?.emptyFolders || 0,
      })),
      catchError(error => {
        logger.error('Failed to get folder statistics', error as Error, { runId });
        return of({ totalFolders: 0, emptyFolders: 0 });
      })
    );
  });
}

/**
 * Get file statistics with filtering options
 */
function getFilteredStats(
  connection: DatabaseConnection,
  runId: string,
  options: {
    includeHidden?: boolean;
    includeSystem?: boolean;
    includeDirectories?: boolean;
  } = {}
): Observable<{ totalFiles: number; totalSize: number }> {
  const { includeHidden = true, includeSystem = true, includeDirectories = true } = options;

  return defer(() => {
    const conditions = [eq(files.run_id, runId)];

    if (!includeHidden) {
      conditions.push(eq(files.is_hidden, false));
    }

    if (!includeSystem) {
      conditions.push(eq(files.is_system, false));
    }

    if (!includeDirectories) {
      conditions.push(eq(files.is_directory, false));
    }

    return from(
      connection.db
        .select({
          totalFiles: count(),
          totalSize: sum(files.content_size),
        })
        .from(files)
        .where(and(...conditions))
    ).pipe(
      map(results => ({
        totalFiles: results[0]?.totalFiles || 0,
        totalSize: Number(results[0]?.totalSize) || 0,
      })),
      catchError(error => {
        logger.error('Failed to get filtered statistics', error as Error, { runId });
        return of({ totalFiles: 0, totalSize: 0 });
      })
    );
  });
}

/**
 * Display detailed scan results summary
 */
export async function summary(
  database: DatabaseConnection,
  runId: string,
  cli: Command
): Promise<void> {
  try {
    // Get all statistics from database
    const [totalStats, userStats, folderStats, duplicateStats, scanStats, emptyFilesCount] =
      await Promise.all([
        getFilteredStats(database, runId, {
          includeHidden: true,
          includeSystem: true,
          includeDirectories: false, // Only count files for total
        }).toPromise(),
        getFilteredStats(database, runId, {
          includeHidden: false,
          includeSystem: false,
          includeDirectories: false,
        }).toPromise(),
        getFolderStats(database, runId).toPromise(),
        getDuplicateStats(database, runId).toPromise(),
        getScanStats(database, runId).toPromise(),
        getEmptyFilesCount(database, runId).toPromise(),
      ]);

    // Provide defaults for potentially undefined values
    const safeStats = {
      total: totalStats ?? { totalFiles: 0, totalSize: 0 },
      user: userStats ?? { totalFiles: 0, totalSize: 0 },
      folders: folderStats ?? { totalFolders: 0, emptyFolders: 0 },
      duplicates: duplicateStats ?? { duplicateGroups: 0, totalDuplicateFiles: 0 },
      scan: scanStats ?? {
        totalFiles: 0,
        totalPhysicalSize: 0,
        totalContentSize: 0,
        duplicateGroups: 0,
        duplicateFiles: 0,
        totalArchives: 0,
        totalArchiveEntries: 0,
        archiveFormats: [],
      },
    };

    // Display summary in priority order
    cli.log('');
    cli.log('Scan completed.');
    cli.log(`  Files discovered: ${safeStats.total.totalFiles.toLocaleString()}`);

    if (safeStats.duplicates.duplicateGroups > 0) {
      cli.log(
        `  Duplicates: ${safeStats.duplicates.totalDuplicateFiles.toLocaleString()} files in ${safeStats.duplicates.duplicateGroups.toLocaleString()} groups`
      );
    }

    if ((emptyFilesCount || 0) > 0) {
      cli.log(`  Empty files: ${(emptyFilesCount || 0).toLocaleString()}`);
    }

    cli.log(`  Folders: ${safeStats.folders.totalFolders.toLocaleString()}`);
    cli.log(`  Empty folders: ${safeStats.folders.emptyFolders.toLocaleString()}`);

    // Show archive statistics if any archives were found
    if (safeStats.scan.totalArchives > 0) {
      cli.log(`  Archives: ${safeStats.scan.totalArchives.toLocaleString()}`);
      cli.log(`  Archive entries: ${safeStats.scan.totalArchiveEntries.toLocaleString()}`);

      if (safeStats.scan.archiveFormats.length > 0) {
        cli.log(`  Archive formats: ${safeStats.scan.archiveFormats.join(', ')}`);
      }
    }

    const hiddenFiles = safeStats.total.totalFiles - safeStats.user.totalFiles;
    if (hiddenFiles > 0) {
      cli.log(`  Hidden files: ${hiddenFiles.toLocaleString()}`);
    }
  } catch (error) {
    logger.error('Failed to generate summary', error as Error, { runId });
    cli.log('');
    cli.log('Scan completed.');
    cli.log('  Error generating detailed breakdown');
  }
}

export const MANIFEST = {
  id: 'summary',
  name: 'Summary',
  description: 'Displays scan statistics and duplicate detection results',
  version: '1.0.0',
};
