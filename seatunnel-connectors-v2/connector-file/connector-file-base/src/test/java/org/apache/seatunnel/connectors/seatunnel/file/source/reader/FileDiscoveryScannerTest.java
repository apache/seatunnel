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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FileDiscoveryScannerTest {

    @Test
    void shouldFilterEntriesAsTheyAreListed() throws Exception {
        int total = 100_000;
        List<FileStatus> accepted = new ArrayList<>();
        GeneratingListingSession session = new GeneratingListingSession(total);

        FileDiscoveryScanner.ScanStats stats =
                FileDiscoveryScanner.scan(
                        new Path("file:///source"),
                        false,
                        session,
                        status -> status.getPath().getName().endsWith("000"),
                        accepted::add);

        Assertions.assertEquals(total, stats.getSourceEntries());
        Assertions.assertEquals(100, stats.getCandidates());
        Assertions.assertEquals(100, accepted.size());
        Assertions.assertTrue(session.callbackWasConsumedBeforeNextEntry);
    }

    @Test
    void shouldUseExplicitDirectoryTraversalAndListEachDirectoryOnce() throws Exception {
        Map<String, List<FileStatus>> entries = new HashMap<>();
        entries.put(
                "file:/source",
                Arrays.asList(directory("file:///source/day=01"), file("file:///source/root.bin")));
        entries.put(
                "file:/source/day=01",
                Collections.singletonList(file("file:///source/day=01/a.bin")));
        RecordingListingSession session = new RecordingListingSession(entries);
        List<FileStatus> accepted = new ArrayList<>();

        FileDiscoveryScanner.scan(
                new Path("file:///source"), true, session, status -> true, accepted::add);

        Assertions.assertEquals(2, accepted.size());
        Assertions.assertEquals(
                Arrays.asList("file:/source", "file:/source/day=01"), session.listed);
    }

    @Test
    void shouldCountRootFileAsSourceEntry() throws Exception {
        FileStatus rootFile = file("file:///source/file.bin");
        FileStatusListingSession session =
                new FileStatusListingSession() {
                    @Override
                    public FileStatus getFileStatus(Path path) {
                        return rootFile;
                    }

                    @Override
                    public void list(Path directory, FileStatusConsumer consumer) {
                        Assertions.fail("A root file must not be listed as a directory");
                    }

                    @Override
                    public void close() {}
                };

        FileDiscoveryScanner.ScanStats stats =
                FileDiscoveryScanner.scan(
                        rootFile.getPath(), true, session, status -> true, ignored -> {});

        Assertions.assertEquals(1, stats.getSourceEntries());
        Assertions.assertEquals(1, stats.getCandidates());
    }

    private static FileStatus file(String path) {
        return new FileStatus(1, false, 1, 1, 1, new Path(path));
    }

    private static FileStatus directory(String path) {
        return new FileStatus(0, true, 1, 1, 1, new Path(path));
    }

    private static final class GeneratingListingSession implements FileStatusListingSession {
        private final int total;
        private boolean callbackWasConsumedBeforeNextEntry = true;
        private int consumed;

        private GeneratingListingSession(int total) {
            this.total = total;
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            return directory(path.toString());
        }

        @Override
        public void list(Path directory, FileStatusConsumer consumer) throws IOException {
            for (int i = 0; i < total; i++) {
                int before = consumed;
                consumer.accept(file(directory + "/file-" + String.format("%06d", i)));
                consumed++;
                callbackWasConsumedBeforeNextEntry &= consumed == before + 1;
            }
        }

        @Override
        public void close() {}
    }

    private static final class RecordingListingSession implements FileStatusListingSession {
        private final Map<String, List<FileStatus>> entries;
        private final List<String> listed = new ArrayList<>();

        private RecordingListingSession(Map<String, List<FileStatus>> entries) {
            this.entries = entries;
        }

        @Override
        public FileStatus getFileStatus(Path path) {
            return directory(path.toString());
        }

        @Override
        public void list(Path directory, FileStatusConsumer consumer) throws IOException {
            listed.add(directory.toString());
            for (FileStatus status :
                    entries.getOrDefault(directory.toString(), Collections.emptyList())) {
                consumer.accept(status);
            }
        }

        @Override
        public void close() {}
    }
}
