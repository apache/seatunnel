/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.connectors.seatunnel.file.hadoop.FileStatusListingSession;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

@Slf4j
final class FileDiscoveryScanner {
    private static final long PROGRESS_INTERVAL = 10_000L;

    private FileDiscoveryScanner() {}

    /** Traverses directories explicitly and applies the filter in the listing callback. */
    static ScanStats scan(
            Path root,
            boolean recursive,
            FileStatusListingSession session,
            FileFilter filter,
            FileStatusListingSession.FileStatusConsumer candidateConsumer)
            throws IOException {
        long started = System.nanoTime();
        ScanStats stats = new ScanStats();
        FileStatus rootStatus;
        try {
            rootStatus = session.getFileStatus(root);
        } catch (IOException e) {
            throw new IOException(
                    "Failed during source_listing root_stat for protocol="
                            + scheme(root)
                            + ", path="
                            + mask(root),
                    e);
        }
        if (rootStatus.isFile()) {
            process(rootStatus, filter, candidateConsumer, stats);
        } else {
            Deque<Path> directories = new ArrayDeque<>();
            directories.push(rootStatus.getPath());
            while (!directories.isEmpty()) {
                Path directory = directories.pop();
                try {
                    session.list(
                            directory,
                            status -> {
                                stats.sourceEntries++;
                                if (stats.sourceEntries % PROGRESS_INTERVAL == 0) {
                                    log.info(
                                            "File source listing progress: process=current_submission_process (CLI client or REST server/master), entries={}, candidates={}",
                                            stats.sourceEntries,
                                            stats.candidates);
                                }
                                if (status.isDirectory()) {
                                    if (recursive && !status.getPath().getName().startsWith(".")) {
                                        directories.addLast(status.getPath());
                                    }
                                    return;
                                }
                                process(status, filter, candidateConsumer, stats);
                            });
                    stats.listedDirectories++;
                } catch (IOException e) {
                    throw new IOException(
                            "Failed during source_listing for protocol="
                                    + scheme(directory)
                                    + ", path="
                                    + mask(directory),
                            e);
                }
            }
        }
        stats.listingNanos = System.nanoTime() - started - stats.filteringNanos;
        return stats;
    }

    private static void process(
            FileStatus status,
            FileFilter filter,
            FileStatusListingSession.FileStatusConsumer candidateConsumer,
            ScanStats stats)
            throws IOException {
        long filterStarted = System.nanoTime();
        boolean accepted = filter.test(status);
        stats.filteringNanos += System.nanoTime() - filterStarted;
        if (accepted) {
            candidateConsumer.accept(status);
            stats.candidates++;
        } else {
            stats.filtered++;
        }
    }

    private static String scheme(Path path) {
        return path.toUri().getScheme() == null ? "default" : path.toUri().getScheme();
    }

    private static String mask(Path path) {
        java.net.URI uri = path.toUri();
        if (uri.getUserInfo() == null || uri.getAuthority() == null) {
            return path.toString();
        }
        return path.toString().replace(uri.getUserInfo() + "@", "***@");
    }

    @FunctionalInterface
    interface FileFilter {
        boolean test(FileStatus status) throws IOException;
    }

    @Getter
    static final class ScanStats {
        private long sourceEntries;
        private long filtered;
        private long candidates;
        private long listedDirectories;
        private long listingNanos;
        private long filteringNanos;
    }
}
