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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.LocalFileIdentity;
import org.apache.seatunnel.connectors.seatunnel.file.source.MarkdownKnowledgeSyncMetadata;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_STRATEGY_NOT_SUPPORT;

@Slf4j
public class MultipleTableFileSourceReader implements SourceReader<SeaTunnelRow, FileSourceSplit> {

    private static final long POLL_WAIT_MS = 1000L;

    private final Context context;
    private volatile boolean noMoreSplit;

    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();

    private final Map<String, ReadStrategy> readStrategyMap;
    private final Set<String> markdownKnowledgeSyncMetadataTableIds;

    public MultipleTableFileSourceReader(
            Context context, BaseMultipleTableFileSourceConfig multipleTableFileSourceConfig) {
        this.context = context;
        List<BaseFileSourceConfig> fileSourceConfigs =
                multipleTableFileSourceConfig.getFileSourceConfigs();
        this.readStrategyMap =
                fileSourceConfigs.stream()
                        .collect(
                                Collectors.toMap(
                                        MultipleTableFileSourceReader::tableId,
                                        BaseFileSourceConfig::getReadStrategy));
        this.markdownKnowledgeSyncMetadataTableIds =
                fileSourceConfigs.stream()
                        .filter(
                                MultipleTableFileSourceReader
                                        ::isMarkdownKnowledgeSyncMetadataEnabled)
                        .map(MultipleTableFileSourceReader::tableId)
                        .collect(Collectors.toSet());
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        FileSourceSplit split;
        long processedBytes = -1L;
        synchronized (output.getCheckpointLock()) {
            split = sourceSplits.poll();
            if (split != null) {
                ReadStrategy readStrategy = readStrategyMap.get(split.getTableId());
                boolean readStarted = false;
                if (readStrategy == null) {
                    throw new FileConnectorException(
                            FILE_READ_STRATEGY_NOT_SUPPORT,
                            "Cannot found the read strategy for this table: ["
                                    + split.getTableId()
                                    + "]");
                }
                try {
                    if (split.getFileIdentity() != null
                            && !split.getFileIdentity()
                                    .equals(LocalFileIdentity.read(split.getFilePath()))) {
                        log.warn(
                                "Skip stale local tail split because the file identity changed: {}",
                                split.getFilePath());
                        processedBytes = 0L;
                    } else {
                        readStarted = true;
                        readStrategy.read(split, output);
                        if (split.getFileIdentity() != null
                                && !split.getFileIdentity()
                                        .equals(LocalFileIdentity.read(split.getFilePath()))) {
                            throw new IOException(
                                    "Local file identity changed while reading the tail split");
                        }
                        processedBytes = readStrategy.getLastReadBytes();
                    }
                } catch (Exception e) {
                    if (!readStarted
                            && split.getFileIdentity() != null
                            && e instanceof java.nio.file.NoSuchFileException) {
                        log.warn(
                                "Skip local tail split because the file disappeared: {}",
                                split.getFilePath());
                        processedBytes = 0L;
                    } else {
                        boolean markdownKnowledgeSyncMetadataEnabled =
                                markdownKnowledgeSyncMetadataTableIds.contains(split.getTableId());
                        String sourceContext = split.splitId();
                        Throwable cause = e;
                        if (markdownKnowledgeSyncMetadataEnabled) {
                            sourceContext =
                                    MarkdownKnowledgeSyncMetadata.safeSourceContext(
                                            split.getFilePath());
                            cause = MarkdownKnowledgeSyncMetadata.copyStackTraceOnly(e);
                        }
                        String errorMsg =
                                String.format(
                                        "Read data from this file [%s] failed", sourceContext);
                        throw new FileConnectorException(FILE_READ_FAILED, errorMsg, cause);
                    }
                }
            }
        }

        if (split != null) {
            if (Boundedness.UNBOUNDED.equals(context.getBoundedness())) {
                ReadStrategy readStrategy = readStrategyMap.get(split.getTableId());
                SourceEvent event =
                        new FileSplitFinishedEvent(
                                split.splitId(),
                                readStrategy == null ? null : readStrategy.getLastReadFingerprint(),
                                processedBytes);
                context.sendSourceEventToEnumerator(event);
            }
            return;
        }

        if (noMoreSplit
                && sourceSplits.isEmpty()
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            log.info("There is no more element for bounded MultipleTableFileSourceReader");
            context.signalNoMoreElement();
            return;
        }

        context.sendSplitRequest();
        if (sourceSplits.isEmpty()) {
            try {
                Thread.sleep(POLL_WAIT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<FileSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public void open() throws Exception {
        // do nothing
        log.info("Opened the MultipleTableLocalFileSourceReader");
    }

    @Override
    public void close() throws IOException {
        // do nothing
        log.info("Closed the MultipleTableLocalFileSourceReader");
        for (ReadStrategy strategy : readStrategyMap.values()) {
            strategy.close();
        }
    }

    private static String tableId(BaseFileSourceConfig fileSourceConfig) {
        return fileSourceConfig.getCatalogTable().getTableId().toTablePath().toString();
    }

    private static boolean isMarkdownKnowledgeSyncMetadataEnabled(
            BaseFileSourceConfig fileSourceConfig) {
        return fileSourceConfig.getFileFormat() == FileFormat.MARKDOWN
                && fileSourceConfig
                        .getBaseFileSourceConfig()
                        .get(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED);
    }
}
