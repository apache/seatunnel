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

package org.apache.seatunnel.connectors.seatunnel.pulsar.source.reader;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarSemantics;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.PulsarConsumerMetadata;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PulsarSourceReader<T> implements SourceReader<T, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceReader.class);

    protected final SourceReader.Context context;
    protected final PulsarClientConfig clientConfig;
    protected final Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap;
    protected final TablePath defaultTablePath;
    protected final boolean injectTableIdForRouting;
    protected final Handover<RecordWithSplitId> handover;

    protected final Map<String, PulsarPartitionSplit> splitStates;
    protected final Map<String, PulsarSplitReaderThread> splitReaders;
    protected final SortedMap<Long, Map<String, MessageId>> pendingCursorsToCommit;
    protected final Map<String, MessageId> pendingCursorsToFinish;
    protected final Set<String> finishedSplits;
    protected final ConcurrentHashMap<String, TablePath> splitIdToTablePath;

    /** The maximum number of milliseconds to wait for a fetch batch. */
    protected final int pollTimeout;

    protected final long pollInterval;
    protected final int batchSize;

    protected PulsarClient pulsarClient;
    /** Indicating whether the SourceReader will be assigned more splits or not. */
    private boolean noMoreSplitsAssignment = false;

    private boolean noMoreElementSignaled = false;

    public PulsarSourceReader(
            SourceReader.Context context,
            PulsarClientConfig clientConfig,
            Map<TablePath, PulsarConsumerMetadata> consumerMetadataMap,
            boolean injectTableId,
            int pollTimeout,
            long pollInterval,
            int batchSize) {
        this.context = context;
        this.clientConfig = clientConfig;
        this.consumerMetadataMap = consumerMetadataMap;
        this.defaultTablePath =
                consumerMetadataMap.size() == 1
                        ? consumerMetadataMap.keySet().iterator().next()
                        : null;
        this.injectTableIdForRouting = injectTableId;
        this.pollTimeout = pollTimeout;
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
        this.splitStates = new HashMap<>();
        this.splitReaders = new HashMap<>();
        this.pendingCursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCursorsToFinish = Collections.synchronizedSortedMap(new TreeMap<>());
        this.finishedSplits = new TreeSet<>();
        this.splitIdToTablePath = new ConcurrentHashMap<>();
        this.handover = new Handover<>();
    }

    @Override
    public void open() {
        this.pulsarClient = PulsarConfigUtil.createClient(clientConfig, PulsarSemantics.NON);
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        for (PulsarSplitReaderThread pulsarSplitReaderThread : splitReaders.values()) {
            try {
                pulsarSplitReaderThread.close();
            } catch (IOException e) {
                throw new PulsarConnectorException(
                        CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                        "Failed to close the split reader thread.",
                        e);
            }
        }
    }

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        Map<TablePath, Collector<T>> collectorCache = new HashMap<>();
        for (int i = 0; i < batchSize; i++) {
            Optional<RecordWithSplitId> recordWithSplitId = handover.pollNext();
            if (recordWithSplitId.isPresent()) {
                String splitId = recordWithSplitId.get().getSplitId();
                Message<byte[]> message = recordWithSplitId.get().getMessage();
                synchronized (output.getCheckpointLock()) {
                    splitStates.get(splitId).setLatestConsumedId(message.getMessageId());
                    TablePath tablePath = resolveTablePath(splitId);
                    DeserializationSchema<T> deserializationSchema =
                            resolveDeserializationSchema(tablePath);
                    Collector<T> collector = resolveCollector(tablePath, output, collectorCache);
                    deserializationSchema.deserialize(message.getData(), collector);
                }
            }
        }
        signalNoMoreElementIfFinished();
    }

    @Override
    public List<PulsarPartitionSplit> snapshotState(long checkpointId) throws Exception {
        List<PulsarPartitionSplit> pendingSplit =
                splitStates.values().stream()
                        .map(PulsarPartitionSplit::copy)
                        .collect(Collectors.toList());
        // Perform a snapshot for these splits.
        int size = pendingSplit.size();
        Map<String, MessageId> cursors =
                pendingCursorsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>(size));
        // Put the cursors of the active splits.
        for (PulsarPartitionSplit split : pendingSplit) {
            MessageId latestConsumedId = split.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursors.put(split.splitId(), latestConsumedId);
            }
        }
        return pendingSplit;
    }

    @Override
    public void addSplits(List<PulsarPartitionSplit> splits) {
        for (PulsarPartitionSplit split : splits) {
            splitStates.put(split.splitId(), split);
            TablePath tablePath = resolveTablePath(split);
            if (tablePath != null) {
                splitIdToTablePath.put(split.splitId(), tablePath);
            }
            PulsarSplitReaderThread splitReaderThread = createPulsarSplitReaderThread(split);
            try {
                splitReaderThread.setName(
                        "Pulsar Source Data Consumer " + split.getPartition().getPartition());
                splitReaderThread.open();
                splitReaders.put(split.splitId(), splitReaderThread);
                splitReaderThread.start();
                LOG.info("PulsarSplitReaderThread = {} start", splitReaderThread.getName());
            } catch (PulsarClientException e) {
                throw new PulsarConnectorException(
                        CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                        "Failed to start the split reader thread.",
                        e);
            }
        }
    }

    protected PulsarSplitReaderThread createPulsarSplitReaderThread(PulsarPartitionSplit split) {
        PulsarConsumerMetadata metadata = resolveConsumerMetadata(resolveTablePath(split));
        return new PulsarSplitReaderThread(
                this,
                split,
                pulsarClient,
                metadata.getConsumerConfig(),
                pollTimeout,
                pollInterval,
                metadata.getStartCursor(),
                handover);
    }

    public void handleNoMoreElements(String splitId, MessageId messageId) {
        LOG.info("Reader received the split {} NoMoreElements event.", splitId);
        pendingCursorsToFinish.put(splitId, messageId);
        // BOUNDED not trigger snapshot and notifyCheckpointComplete
        if (context.getBoundedness() == Boundedness.BOUNDED) {
            finishedSplits.add(splitId);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        LOG.info("Reader received NoMoreSplits event.");
        this.noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing cursors for checkpoint {}", checkpointId);
        Map<String, MessageId> pendingCursors = pendingCursorsToCommit.remove(checkpointId);
        if (pendingCursors == null) {
            LOG.debug(
                    "Cursors for checkpoint {} either do not exist or have already been committed.",
                    checkpointId);
            return;
        }
        pendingCursors.forEach(this::committingCursor);
    }

    /** commit the cursor of consumer thread */
    private void committingCursor(String splitId, MessageId messageId) {
        if (finishedSplits.contains(splitId)) {
            return;
        }
        try {
            PulsarSplitReaderThread pulsarSplitReaderThread = splitReaders.get(splitId);
            pulsarSplitReaderThread.committingCursor(messageId);

            if (pendingCursorsToFinish.containsKey(splitId)
                    && pendingCursorsToFinish.get(splitId).compareTo(messageId) == 0) {
                finishedSplits.add(splitId);
                try {
                    pulsarSplitReaderThread.close();
                } catch (IOException e) {
                    throw new PulsarConnectorException(
                            CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                            "Failed to close the split reader thread.",
                            e);
                }
            }
        } catch (PulsarClientException e) {
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.ACK_CUMULATE_FAILED,
                    "pulsar consumer acknowledgeCumulative failed.",
                    e);
        }
    }

    private TablePath resolveTablePath(String splitId) {
        TablePath tablePath = splitIdToTablePath.get(splitId);
        return tablePath != null ? tablePath : defaultTablePath;
    }

    private TablePath resolveTablePath(PulsarPartitionSplit split) {
        return split.getTablePath() != null ? split.getTablePath() : defaultTablePath;
    }

    private PulsarConsumerMetadata resolveConsumerMetadata(TablePath tablePath) {
        PulsarConsumerMetadata metadata =
                tablePath == null ? null : consumerMetadataMap.get(tablePath);
        if (metadata == null && defaultTablePath != null) {
            metadata = consumerMetadataMap.get(defaultTablePath);
        }
        if (metadata == null) {
            String tablePathStr = tablePath != null ? tablePath.toString() : "null";
            String defaultTablePathStr =
                    defaultTablePath != null ? defaultTablePath.toString() : "null";
            String availableTables =
                    consumerMetadataMap.keySet().stream()
                            .map(TablePath::toString)
                            .collect(Collectors.joining(", "));
            throw new PulsarConnectorException(
                    PulsarConnectorErrorCode.DESERIALIZATION_SCHEMA_NOT_FOUND,
                    String.format(
                            "No consumer metadata found for table '%s'. "
                                    + "Default table path: '%s'. "
                                    + "Available tables: [%s]. "
                                    + "This is likely a bug in the multi-table routing logic.",
                            tablePathStr, defaultTablePathStr, availableTables));
        }
        return metadata;
    }

    @SuppressWarnings("unchecked")
    private DeserializationSchema<T> resolveDeserializationSchema(TablePath tablePath) {
        return (DeserializationSchema<T>)
                resolveConsumerMetadata(tablePath).getDeserializationSchema();
    }

    private Collector<T> resolveCollector(
            TablePath tablePath, Collector<T> output, Map<TablePath, Collector<T>> collectorCache) {
        if (!injectTableIdForRouting || tablePath == null) {
            return output;
        }
        // Reuse wrappers inside the current poll batch to avoid per-record allocations on hot path.
        return collectorCache.computeIfAbsent(
                tablePath, path -> new TableIdInjectingCollector<>(output, path));
    }

    private void signalNoMoreElementIfFinished() throws Exception {
        if (!noMoreElementSignaled
                && noMoreSplitsAssignment
                && finishedSplits.size() == splitStates.size()
                && handover.isEmpty()) {
            noMoreElementSignaled = true;
            context.signalNoMoreElement();
        }
    }
}
