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

package org.apache.seatunnel.engine.server.event;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/** Writes OTLP JSON lines into per-job trace files and rotates them based on count or size. */
@Slf4j
public class TraceFileWriter implements Closeable {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH-mm-ss");
    private static final Pattern JOB_ID_PATTERN = Pattern.compile("[a-zA-Z0-9_-]+");

    private final String jobId;
    private final String date;
    private final Path filePath;
    private final BufferedWriter writer;
    private final AtomicLong eventCount;
    private final AtomicLong fileSize;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TraceFileWriter(String baseDir, String jobId, String date) throws IOException {
        if (jobId == null || !JOB_ID_PATTERN.matcher(jobId).matches()) {
            throw new IllegalArgumentException("Invalid jobId for trace file path: " + jobId);
        }
        this.jobId = jobId;
        this.date = date;
        this.eventCount = new AtomicLong(0);
        this.fileSize = new AtomicLong(0);

        // Create directory: {baseDir}/traces/{jobId}/{date}/
        Path basePath = Paths.get(baseDir).toAbsolutePath().normalize();
        Path traceDir = basePath.resolve(Paths.get("traces", jobId, date)).normalize();
        if (!traceDir.startsWith(basePath)) {
            throw new IllegalArgumentException("Resolved trace path escapes baseDir: " + traceDir);
        }
        Files.createDirectories(traceDir);

        // Generate file name: traces-{HH-mm-ss}-{uuid}.jsonl
        String timestamp = LocalDateTime.now().format(TIME_FORMATTER);
        String shortUuid = UUID.randomUUID().toString().substring(0, 8);
        String fileName = String.format("traces-%s-%s.jsonl", timestamp, shortUuid);

        this.filePath = traceDir.resolve(fileName);

        // Create file with BufferedWriter
        this.writer =
                Files.newBufferedWriter(
                        filePath,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE_NEW,
                        StandardOpenOption.WRITE);

        log.info("Created trace file: {}", filePath);
    }

    /** Appends one OTLP JSON object as a single line and updates local rotation counters. */
    public synchronized void writeJsonLine(String jsonLine) throws IOException {
        if (closed.get()) {
            return;
        }
        writer.write(jsonLine);
        writer.newLine();
        eventCount.incrementAndGet();
        fileSize.addAndGet(jsonLine.getBytes(StandardCharsets.UTF_8).length + 1); // +1 for newline
    }

    public synchronized void flush() throws IOException {
        if (closed.get()) {
            return;
        }
        writer.flush();
    }

    /** Indicates whether the current file reached either the event-count or file-size threshold. */
    public boolean needsRotation(int maxEvents, long maxSizeBytes) {
        return eventCount.get() >= maxEvents || fileSize.get() >= maxSizeBytes;
    }

    public long getEventCount() {
        return eventCount.get();
    }

    public long getFileSize() {
        return fileSize.get();
    }

    public Path getFilePath() {
        return filePath;
    }

    public String getJobId() {
        return jobId;
    }

    public String getDate() {
        return date;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            writer.close();
            log.info(
                    "Closed trace file: {} (events={}, size={} bytes)",
                    filePath,
                    eventCount.get(),
                    fileSize.get());
        } catch (IOException e) {
            if (e instanceof ClosedChannelException
                    || (e.getMessage() != null && e.getMessage().contains("Stream closed"))) {
                log.warn("Trace file already closed: {}", filePath);
                return;
            }
            log.error("Failed to close trace file: " + filePath, e);
            throw e;
        }
    }
}
