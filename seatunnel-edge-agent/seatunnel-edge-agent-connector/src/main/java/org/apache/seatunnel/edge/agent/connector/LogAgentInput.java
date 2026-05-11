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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Single-file log reader using UTF-8. When {@code readFromBeginning} is {@code false}, {@link
 * #open()} seeks to the end of the file so {@link #poll(int)} returns newly appended NDJSON lines.
 */
public final class LogAgentInput implements AgentInput {

    private final Path logFile;
    private final boolean readFromBeginning;

    private RandomAccessFile reader;
    private boolean opened;
    private boolean closed;

    public LogAgentInput(Path logFile, boolean readFromBeginning) {
        if (logFile == null) {
            throw new IllegalArgumentException("logFile must not be null");
        }
        this.logFile = logFile;
        this.readFromBeginning = readFromBeginning;
    }

    @Override
    public String id() {
        return AgentInputFactory.TYPE_LOG;
    }

    @Override
    public void open() throws IOException {
        ensureNotClosed();
        if (opened) {
            throw new IllegalStateException("already open");
        }
        reader = new RandomAccessFile(logFile.toFile(), "r");
        if (!readFromBeginning) {
            reader.seek(reader.length());
        }
        opened = true;
    }

    @Override
    public List<String> poll(int maxRecords) throws IOException {
        ensureOpen();
        if (maxRecords <= 0) {
            return Collections.emptyList();
        }
        List<String> records = new ArrayList<>(Math.min(maxRecords, 16));
        while (records.size() < maxRecords) {
            String line = readUtf8Line();
            if (line == null) {
                break;
            }
            String record = JsonLineSupport.normalizeRecord(line);
            if (record != null) {
                records.add(record);
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
        if (reader != null) {
            try {
                reader.close();
            } finally {
                reader = null;
            }
        }
    }

    /**
     * Reads until LF or EOF using UTF-8 (avoids {@link RandomAccessFile#readLine()} legacy
     * charset).
     */
    private String readUtf8Line() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(128);
        while (true) {
            int b = reader.read();
            if (b == -1) {
                if (buffer.size() == 0) {
                    return null;
                }
                break;
            }
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                buffer.write(b);
            }
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private void ensureOpen() {
        ensureNotClosed();
        if (!opened || reader == null) {
            throw new IllegalStateException("open() must be called before poll()");
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }
}
