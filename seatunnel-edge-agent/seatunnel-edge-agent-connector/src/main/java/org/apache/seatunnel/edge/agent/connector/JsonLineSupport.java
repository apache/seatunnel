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

package org.apache.seatunnel.edge.agent.connector;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Helpers for newline-delimited JSON (NDJSON) text lines. */
final class JsonLineSupport {

    private JsonLineSupport() {}

    /**
     * Returns a trimmed non-empty line as a JSON payload string, or {@code null} to skip blank
     * lines.
     */
    static String normalizeRecord(String line) {
        if (line == null) {
            return null;
        }
        String trimmed = line.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    static void appendRecordsFromPaths(List<Path> paths, Collection<String> sink)
            throws IOException {
        if (paths == null || paths.isEmpty()) {
            return;
        }
        for (Path path : paths) {
            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String record = normalizeRecord(line);
                    if (record != null) {
                        sink.add(record);
                    }
                }
            }
        }
    }

    static ArrayDeque<String> loadAllRecords(List<Path> paths) throws IOException {
        if (paths == null || paths.isEmpty()) {
            return new ArrayDeque<>();
        }
        ArrayDeque<String> deque = new ArrayDeque<>();
        appendRecordsFromPaths(paths, deque);
        return deque;
    }

    static List<Path> nonNullPaths(List<Path> paths) {
        return paths == null ? Collections.<Path>emptyList() : paths;
    }
}
