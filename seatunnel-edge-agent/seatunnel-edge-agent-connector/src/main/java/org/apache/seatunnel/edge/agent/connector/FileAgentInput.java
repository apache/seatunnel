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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** NDJSON file reader: reads newline-delimited JSON across one or more files in path order. */
public final class FileAgentInput implements AgentInput {

    private final List<Path> files;
    private final List<BufferedReader> readers = new ArrayList<>();

    private boolean opened;
    private boolean closed;

    public FileAgentInput(List<Path> files) {
        this.files = JsonLineSupport.nonNullPaths(files);
    }

    @Override
    public String id() {
        return AgentInputFactory.TYPE_FILE;
    }

    @Override
    public void open() throws IOException {
        ensureNotClosed();
        if (opened) {
            throw new IllegalStateException("already open");
        }
        IOException firstFailure = null;
        for (Path file : files) {
            try {
                readers.add(Files.newBufferedReader(file, StandardCharsets.UTF_8));
            } catch (IOException e) {
                firstFailure = e;
                break;
            }
        }
        if (firstFailure != null) {
            closeQuietly();
            throw firstFailure;
        }
        opened = true;
    }

    @Override
    public List<String> poll(int maxRecords) throws IOException {
        ensureOpen();
        if (maxRecords <= 0 || readers.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> records = new ArrayList<>(Math.min(maxRecords, 16));
        for (BufferedReader reader : readers) {
            while (records.size() < maxRecords) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String record = JsonLineSupport.normalizeRecord(line);
                if (record != null) {
                    records.add(record);
                }
            }
            if (records.size() >= maxRecords) {
                break;
            }
        }
        return records;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        opened = false;
        IOException firstException = null;
        for (BufferedReader reader : readers) {
            try {
                reader.close();
            } catch (IOException closeException) {
                if (firstException == null) {
                    firstException = closeException;
                }
            }
        }
        readers.clear();
        if (firstException != null) {
            throw firstException;
        }
    }

    private void closeQuietly() {
        for (BufferedReader reader : readers) {
            try {
                reader.close();
            } catch (IOException ignored) {
                // best-effort cleanup during failed open()
            }
        }
        readers.clear();
        opened = false;
    }

    private void ensureOpen() {
        ensureNotClosed();
        if (!opened) {
            throw new IllegalStateException("open() must be called before poll()");
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }
}
