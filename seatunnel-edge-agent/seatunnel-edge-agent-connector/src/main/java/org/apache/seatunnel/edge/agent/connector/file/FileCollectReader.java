/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.connector.file;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeInputReader;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectConfig;
import org.apache.seatunnel.edge.agent.connector.file.cursor.FileTailCursor;
import org.apache.seatunnel.edge.agent.connector.file.glob.GlobPathResolver;
import org.apache.seatunnel.edge.agent.connector.file.multiline.MultilineAssembler;
import org.apache.seatunnel.edge.agent.connector.file.output.JsonOutputFormatter;
import org.apache.seatunnel.edge.agent.connector.file.output.LineOutputFormatter;
import org.apache.seatunnel.edge.agent.connector.file.output.OutputFormatter;
import org.apache.seatunnel.edge.agent.connector.record.CollectedRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FileCollectReader implements EdgeInputReader {

    private static final Logger LOG = LoggerFactory.getLogger(FileCollectReader.class);

    private final FileCollectConfig config;
    private final Charset charset;
    private final EdgeSourcePositionStore sourcePositionStore;

    private GlobPathResolver globResolver;
    private final Map<Path, MultilineAssembler> multilineAssemblers = new HashMap<>();
    private MultilineAssembler.MatchMode multilineMatchMode;
    private OutputFormatter outputFormatter;
    private final Map<Path, FileTailCursor> activeCursors = new LinkedHashMap<>();
    private long lastDiscoveryMs;

    private final Map<Path, Long> lineCounters = new HashMap<>();

    public FileCollectReader(
            FileCollectConfig config, EdgeSourcePositionStore sourcePositionStore) {
        this.config = config;
        this.charset = config.getCharset();
        this.sourcePositionStore = sourcePositionStore;
    }

    public String id() {
        return config.getId();
    }

    @Override
    public void open() throws Exception {
        Map<String, EdgeSourcePosition> initialPositions =
                sourcePositionStore.loadBySource(config.getId());

        this.globResolver = new GlobPathResolver(config.getPaths());

        if (config.isMultilineEnabled()) {
            this.multilineMatchMode =
                    "before".equalsIgnoreCase(config.getMultilineMatch())
                            ? MultilineAssembler.MatchMode.BEFORE
                            : MultilineAssembler.MatchMode.AFTER;
        }

        if (config.isJsonOutput()) {
            this.outputFormatter = new JsonOutputFormatter();
        } else {
            this.outputFormatter = new LineOutputFormatter();
        }

        List<Path> files = globResolver.resolveAll();
        for (Path file : files) {
            String pathStr = file.toAbsolutePath().toString();
            EdgeSourcePosition pos = initialPositions.get(pathStr);

            FileTailCursor cursor = openCursor(file, pos);
            activeCursors.put(file, cursor);
            lineCounters.put(file, restoredLineNumber(pos));
        }

        this.lastDiscoveryMs = System.currentTimeMillis();
    }

    @Override
    public List<EdgeEvent> poll(int maxRecords) throws Exception {
        List<EdgeEvent> records = new ArrayList<>();
        long now = System.currentTimeMillis();

        // Periodically scan glob patterns for newly appeared files
        discoverNewFiles(now);

        // Release file handles that have been idle too long
        closeInactiveCursors(now);

        // Read lines from each tracked file and assemble into events
        readFromActiveCursors(records, maxRecords);

        // Flush multiline buffers that have been waiting longer than the idle timeout
        flushIdleAssemblers(records, maxRecords, now);

        return records;
    }

    private void readFromActiveCursors(List<EdgeEvent> records, int maxRecords) throws Exception {
        Iterator<Map.Entry<Path, FileTailCursor>> it = activeCursors.entrySet().iterator();
        while (it.hasNext() && records.size() < maxRecords) {
            Map.Entry<Path, FileTailCursor> entry = it.next();
            Path filePath = entry.getKey();
            FileTailCursor cursor = entry.getValue();
            try {
                readLines(filePath, cursor, records, maxRecords);
            } catch (Exception e) {
                handleReadException(e, filePath, cursor, it);
            }
        }
    }

    private void flushIdleAssemblers(List<EdgeEvent> records, int maxRecords, long now) {
        if (!config.isMultilineEnabled() || config.getMultilineFlushIdleTimeoutMs() <= 0) {
            return;
        }
        for (MultilineAssembler assembler : multilineAssemblers.values()) {
            if (records.size() >= maxRecords) {
                break;
            }
            if (assembler.hasPending()) {
                long bufferAge = now - assembler.getBufferFirstTimestamp();
                if (bufferAge >= config.getMultilineFlushIdleTimeoutMs()) {
                    List<MultilineAssembler.LineElement> remaining = assembler.flush();
                    if (!remaining.isEmpty()) {
                        records.add(toEvent(outputFormatter.format(remaining, config.getId())));
                    }
                }
            }
        }
    }

    private void readLines(
            Path filePath, FileTailCursor cursor, List<EdgeEvent> records, int maxRecords)
            throws Exception {
        String filePathStr = filePath.toAbsolutePath().toString();
        while (records.size() < maxRecords) {
            String line = cursor.readLine();
            if (line == null) {
                if (cursor.hasRotated()) {
                    LOG.warn("File rotated: {}", filePath);
                    clearFileState(filePath, "file rotated");
                    cursor.reopen();
                    lineCounters.put(filePath, 0L);
                    continue;
                }
                break;
            }
            emitLine(filePath, filePathStr, cursor, line, records);
        }
    }

    private void emitLine(
            Path filePath,
            String filePathStr,
            FileTailCursor cursor,
            String line,
            List<EdgeEvent> records) {
        long lineNum = lineCounters.merge(filePath, 1L, Long::sum);
        long ts = System.currentTimeMillis();
        MultilineAssembler.LineElement element =
                new MultilineAssembler.LineElement(line, filePathStr, lineNum, cursor.offset(), ts);

        if (config.isMultilineEnabled()) {
            MultilineAssembler assembler = getOrCreateAssembler(filePath);
            List<MultilineAssembler.LineElement> event = assembler.addLine(element);
            if (event != null && !event.isEmpty()) {
                records.add(toEvent(outputFormatter.format(event, config.getId())));
            }
        } else {
            records.add(
                    toEvent(
                            outputFormatter.format(
                                    Collections.singletonList(element), config.getId())));
        }
    }

    private void handleReadException(
            Exception e, Path filePath, FileTailCursor cursor, Iterator<?> cursorIterator)
            throws Exception {
        if (config.isSkipOnError()) {
            LOG.error("IO error reading {}, skipping", filePath, e);
            clearFileState(filePath, "read error skipped");
            globResolver.forget(filePath);
            try {
                cursor.close();
            } catch (Exception ignored) {
            }
            cursorIterator.remove();
        } else {
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<Path, MultilineAssembler> entry : multilineAssemblers.entrySet()) {
            MultilineAssembler assembler = entry.getValue();
            if (assembler.hasPending()) {
                List<MultilineAssembler.LineElement> remaining = assembler.flush();
                LOG.warn(
                        "Multiline buffer for {} had {} pending line(s) at close; "
                                + "they will be re-read from saved offset on next restart",
                        entry.getKey(),
                        remaining.size());
            }
        }
        multilineAssemblers.clear();
        for (FileTailCursor cursor : activeCursors.values()) {
            try {
                cursor.close();
            } catch (Exception e) {
                LOG.warn("Error closing cursor", e);
            }
        }
        activeCursors.clear();
    }

    private void discoverNewFiles(long now) {
        if (now - lastDiscoveryMs < config.getGlobScanIntervalMs()) {
            return;
        }
        lastDiscoveryMs = now;
        try {
            List<Path> newFiles = globResolver.resolveNew();
            for (Path file : newFiles) {
                if (activeCursors.containsKey(file)) {
                    continue;
                }
                String pathStr = file.toAbsolutePath().toString();
                EdgeSourcePosition pos = resolvePosition(pathStr);

                FileTailCursor cursor = openCursor(file, pos);
                activeCursors.put(file, cursor);
                lineCounters.put(file, restoredLineNumber(pos));
                LOG.info("Discovered new file: {}", file);
            }
        } catch (Exception e) {
            if (config.isSkipOnError()) {
                LOG.warn("Error during file discovery", e);
            } else {
                throw new RuntimeException("File discovery failed", e);
            }
        }
    }

    private void closeInactiveCursors(long nowMs) {
        Iterator<Map.Entry<Path, FileTailCursor>> it = activeCursors.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Path, FileTailCursor> entry = it.next();
            Path filePath = entry.getKey();
            FileTailCursor cursor = entry.getValue();
            if (nowMs - cursor.lastActivityMs() > config.getCloseInactiveMs()) {
                LOG.debug("Closing inactive cursor: {}", filePath);
                clearFileState(filePath, "inactive timeout");
                try {
                    cursor.close();
                } catch (Exception ignored) {
                }
                globResolver.forget(filePath);
                it.remove();
            }
        }
    }

    private void clearFileState(Path filePath, String reason) {
        MultilineAssembler assembler = multilineAssemblers.remove(filePath);
        if (assembler != null && assembler.hasPending()) {
            List<MultilineAssembler.LineElement> discarded = assembler.flush();
            LOG.warn(
                    "Discarding {} pending multiline line(s) for {} ({})",
                    discarded.size(),
                    filePath,
                    reason);
        }
        lineCounters.remove(filePath);
    }

    private MultilineAssembler getOrCreateAssembler(Path filePath) {
        return multilineAssemblers.computeIfAbsent(
                filePath,
                k ->
                        new MultilineAssembler(
                                config.getMultilinePattern(),
                                multilineMatchMode,
                                config.isMultilineNegate(),
                                config.getMultilineMaxLines()));
    }

    private FileTailCursor openCursor(Path file, EdgeSourcePosition pos) throws IOException {
        FileTailCursor cursor = new FileTailCursor(file, charset);
        long seekOffset = 0;
        if (pos != null && pos.getOffset() > 0) {
            seekOffset = pos.getOffset();
        } else if (!config.isReadFromBeginning()) {
            seekOffset = Files.size(file);
        }
        cursor.open(seekOffset);

        if (pos != null && inode(pos) != 0 && cursor.inode() != 0 && inode(pos) != cursor.inode()) {
            cursor.close();
            cursor = new FileTailCursor(file, charset);
            cursor.open(0);
        }
        return cursor;
    }

    private EdgeSourcePosition resolvePosition(String pathStr) throws Exception {
        return sourcePositionStore.load(config.getId(), pathStr);
    }

    private static long restoredLineNumber(EdgeSourcePosition pos) {
        if (pos == null || pos.getMetadata() == null) {
            return 0L;
        }
        String line = pos.getMetadata().get("line");
        if (line == null || line.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(line);
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }

    private EdgeEvent toEvent(CollectedRecord record) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("filePath", record.getFilePath());
        metadata.put("line", Long.toString(record.getLine()));
        metadata.put("ts", Long.toString(record.getTs()));
        metadata.put("inode", Long.toString(inode(record.getFilePath())));
        EdgeSourcePosition position =
                EdgeSourcePosition.builder()
                        .sourceId(record.getSourceId())
                        .partition(record.getFilePath())
                        .offset(record.getOffset())
                        .updatedAt(System.currentTimeMillis())
                        .metadata(metadata)
                        .build();
        return EdgeEvent.builder()
                .sourceId(record.getSourceId())
                .payload(record.getPayload().getBytes(StandardCharsets.UTF_8))
                .eventTime(record.getTs())
                .sourcePosition(position)
                .metadata(metadata)
                .build();
    }

    private long inode(String filePath) {
        FileTailCursor cursor = activeCursors.get(Paths.get(filePath).normalize().toAbsolutePath());
        return cursor != null ? cursor.inode() : 0L;
    }

    private static long inode(EdgeSourcePosition position) {
        if (position == null
                || position.getMetadata() == null
                || position.getMetadata().get("inode") == null) {
            return 0L;
        }
        try {
            return Long.parseLong(position.getMetadata().get("inode"));
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }
}
