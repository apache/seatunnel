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

package org.apache.seatunnel.connectors.seatunnel.kafka.source;

import org.apache.seatunnel.api.source.FactSourceGateCapability;
import org.apache.seatunnel.api.source.SourceGateCommand;
import org.apache.seatunnel.api.source.SourceGateState;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.kafka.source.fetch.KafkaSourceFetcherManager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaSourceReader
        extends SingleThreadMultiplexSourceReaderBase<
                ConsumerRecord<byte[], byte[]>,
                SeaTunnelRow,
                KafkaSourceSplit,
                KafkaSourceSplitState>
        implements FactSourceGateCapability {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceReader.class);
    private final SourceReader.Context context;

    private final KafkaSourceConfig kafkaSourceConfig;
    private final SortedMap<Long, Map<TopicPartition, OffsetAndMetadata>> checkpointOffsetMap;

    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> offsetsOfFinishedSplits;
    private final Object gateLock = new Object();
    private final Map<String, KafkaSourceSplit> stagedSplits = new LinkedHashMap<>();
    private volatile boolean gateOpen = true;
    private volatile boolean stagedNoMoreSplits;
    private volatile boolean readerOpened;
    private volatile boolean activateOnOpen;

    KafkaSourceReader(
            BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue,
            SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit>
                    splitFetcherManager,
            RecordEmitter<ConsumerRecord<byte[], byte[]>, SeaTunnelRow, KafkaSourceSplitState>
                    recordEmitter,
            SourceReaderOptions options,
            KafkaSourceConfig kafkaSourceConfig,
            Context context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, options, context);
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.context = context;
        this.checkpointOffsetMap = Collections.synchronizedSortedMap(new TreeMap<>());
        this.offsetsOfFinishedSplits = new ConcurrentHashMap<>();
    }

    @Override
    public void pollNext(org.apache.seatunnel.api.source.Collector<SeaTunnelRow> output)
            throws Exception {
        if (!gateOpen) {
            return;
        }
        super.pollNext(output);
    }

    @Override
    public void open() {
        super.open();
        readerOpened = true;
        if (activateOnOpen) {
            activateStagedSplits();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, KafkaSourceSplitState> finishedSplitIds) {
        finishedSplitIds.forEach(
                (ignored, splitState) -> {
                    if (splitState.getCurrentOffset() > 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getTopicPartition(),
                                new OffsetAndMetadata(splitState.getCurrentOffset()));
                    } else if (splitState.getEndOffset() > 0) {
                        offsetsOfFinishedSplits.put(
                                splitState.getTopicPartition(),
                                new OffsetAndMetadata(splitState.getEndOffset()));
                    }
                });
    }

    @Override
    protected KafkaSourceSplitState initializedState(KafkaSourceSplit split) {
        return new KafkaSourceSplitState(split);
    }

    @Override
    protected KafkaSourceSplit toSplitType(String splitId, KafkaSourceSplitState splitState) {
        return splitState.toKafkaSourceSplit();
    }

    @Override
    public List<KafkaSourceSplit> snapshotState(long checkpointId) {
        synchronized (gateLock) {
            return snapshotStateUnderGateLock(checkpointId).getSourceSplits();
        }
    }

    private GateSnapshot snapshotStateUnderGateLock(long checkpointId) {
        boolean snapshotGateOpen = gateOpen;
        boolean snapshotNoMoreSplits =
                snapshotGateOpen ? isNoMoreSplitsAssignment() : stagedNoMoreSplits;
        List<KafkaSourceSplit> sourceSplits =
                snapshotGateOpen
                        ? copySplits(super.snapshotState(checkpointId))
                        : copySplits(stagedSplits.values());
        if (snapshotGateOpen) {
            registerCheckpointOffsets(checkpointId, sourceSplits);
        }
        return new GateSnapshot(snapshotGateOpen, snapshotNoMoreSplits, sourceSplits);
    }

    private void registerCheckpointOffsets(long checkpointId, List<KafkaSourceSplit> sourceSplits) {
        if (!kafkaSourceConfig.isCommitOnCheckpoint()) {
            return;
        }
        if (sourceSplits.isEmpty() && offsetsOfFinishedSplits.isEmpty()) {
            logger.debug(
                    "checkpoint {} does not have an offset to submit for splits", checkpointId);
            checkpointOffsetMap.put(checkpointId, Collections.emptyMap());
        } else {
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
                    checkpointOffsetMap.computeIfAbsent(checkpointId, id -> new HashMap<>());
            for (KafkaSourceSplit kafkaSourceSplit : sourceSplits) {
                if (kafkaSourceSplit.getStartOffset() >= 0) {
                    offsetAndMetadataMap.put(
                            kafkaSourceSplit.getTopicPartition(),
                            new OffsetAndMetadata(kafkaSourceSplit.getStartOffset()));
                }
            }
            offsetAndMetadataMap.putAll(offsetsOfFinishedSplits);
        }
    }

    @Override
    public void addSplits(List<KafkaSourceSplit> splits) {
        synchronized (gateLock) {
            if (!gateOpen) {
                splits.stream()
                        .map(KafkaSourceSplit::copy)
                        .forEach(split -> stagedSplits.put(split.splitId(), split));
                return;
            }
        }
        super.addSplits(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        synchronized (gateLock) {
            if (!gateOpen) {
                stagedNoMoreSplits = true;
                return;
            }
        }
        super.handleNoMoreSplits();
    }

    @Override
    public void prepareClosedGate() {
        synchronized (gateLock) {
            gateOpen = false;
            stagedSplits.clear();
            stagedNoMoreSplits = false;
            activateOnOpen = false;
        }
    }

    @Override
    public SourceGateState snapshotGate(long checkpointId) throws Exception {
        GateSnapshot snapshot;
        synchronized (gateLock) {
            snapshot = snapshotStateUnderGateLock(checkpointId);
        }
        List<SourceGateState.PreparedSplit> preparedSplits =
                new ArrayList<>(snapshot.getSourceSplits().size());
        for (KafkaSourceSplit split : snapshot.getSourceSplits()) {
            byte[] serializedSplit = serializeSplit(split);
            preparedSplits.add(
                    new SourceGateState.PreparedSplit(
                            split.splitId(), serializedSplit, sha256(serializedSplit)));
        }
        return new SourceGateState(
                snapshot.isGateOpen(), snapshot.isNoMoreSplits(), preparedSplits);
    }

    @Override
    public void restoreGateState(SourceGateState gateState) throws Exception {
        List<KafkaSourceSplit> restoredSplits = new ArrayList<>();
        for (SourceGateState.PreparedSplit preparedSplit : gateState.getPreparedSplits()) {
            byte[] serializedSplit = preparedSplit.getSerializedSplit();
            if (!Arrays.equals(sha256(serializedSplit), preparedSplit.getSerializedSplitDigest())) {
                throw new IOException(
                        "Kafka source gate split digest mismatch: " + preparedSplit.getSplitId());
            }
            restoredSplits.add(deserializeSplit(serializedSplit));
        }
        synchronized (gateLock) {
            stagedSplits.clear();
            for (KafkaSourceSplit restoredSplit : restoredSplits) {
                stagedSplits.put(restoredSplit.splitId(), restoredSplit);
            }
            stagedNoMoreSplits = gateState.isNoMoreSplits();
            gateOpen = false;
            activateOnOpen = gateState.isGateOpen();
        }
        if (gateState.isGateOpen() && readerOpened) {
            activateStagedSplits();
        }
    }

    @Override
    public void applyGateCommand(SourceGateCommand command) {
        switch (command) {
            case OPEN:
                activateStagedSplits();
                return;
            case CLOSE:
                synchronized (gateLock) {
                    if (gateOpen) {
                        throw new IllegalStateException(
                                "Kafka source gate cannot be closed after activation");
                    }
                }
                return;
            case ABORT:
                synchronized (gateLock) {
                    stagedSplits.clear();
                    stagedNoMoreSplits = false;
                    gateOpen = false;
                    activateOnOpen = false;
                }
                return;
            default:
                throw new IllegalArgumentException(
                        "Unsupported Kafka source gate command: " + command);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        logger.debug("Committing offsets for checkpoint {}", checkpointId);
        if (!kafkaSourceConfig.isCommitOnCheckpoint()) {
            logger.debug("Submitting offsets after snapshot completion is prohibited");
            return;
        }
        Map<TopicPartition, OffsetAndMetadata> committedPartitions =
                checkpointOffsetMap.get(checkpointId);

        if (committedPartitions == null) {
            logger.debug("Offsets for checkpoint {} have already been committed.", checkpointId);
            return;
        }

        if (committedPartitions.isEmpty()) {
            logger.debug("There are no offsets to commit for checkpoint {}.", checkpointId);
            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
            return;
        }

        ((KafkaSourceFetcherManager) splitFetcherManager)
                .commitOffsets(
                        committedPartitions,
                        (ignored, e) -> {
                            if (e != null) {
                                logger.warn(
                                        "Failed to commit consumer offsets for checkpoint {}",
                                        checkpointId,
                                        e);
                                return;
                            }
                            offsetsOfFinishedSplits
                                    .keySet()
                                    .removeIf(committedPartitions::containsKey);
                            removeAllOffsetsToCommitUpToCheckpoint(checkpointId);
                        });
    }

    private void removeAllOffsetsToCommitUpToCheckpoint(long checkpointId) {
        while (!checkpointOffsetMap.isEmpty() && checkpointOffsetMap.firstKey() <= checkpointId) {
            checkpointOffsetMap.remove(checkpointOffsetMap.firstKey());
        }
    }

    private void activateStagedSplits() {
        synchronized (gateLock) {
            if (gateOpen) {
                return;
            }
            List<KafkaSourceSplit> splitsToActivate = copySplits(stagedSplits.values());
            boolean noMoreSplits = stagedNoMoreSplits;
            if (!splitsToActivate.isEmpty()) {
                super.addSplits(splitsToActivate);
            }
            if (noMoreSplits) {
                super.handleNoMoreSplits();
            }
            stagedSplits.clear();
            stagedNoMoreSplits = false;
            activateOnOpen = false;
            gateOpen = true;
        }
    }

    private static List<KafkaSourceSplit> copySplits(Iterable<KafkaSourceSplit> splits) {
        List<KafkaSourceSplit> copies = new ArrayList<>();
        for (KafkaSourceSplit split : splits) {
            copies.add(split.copy());
        }
        return copies;
    }

    /** Immutable reader and gate snapshot captured under one gate lock acquisition. */
    private static final class GateSnapshot {
        private final boolean gateOpen;
        private final boolean noMoreSplits;
        private final List<KafkaSourceSplit> sourceSplits;

        private GateSnapshot(
                boolean gateOpen, boolean noMoreSplits, List<KafkaSourceSplit> sourceSplits) {
            this.gateOpen = gateOpen;
            this.noMoreSplits = noMoreSplits;
            this.sourceSplits = Collections.unmodifiableList(copySplits(sourceSplits));
        }

        private boolean isGateOpen() {
            return gateOpen;
        }

        private boolean isNoMoreSplits() {
            return noMoreSplits;
        }

        private List<KafkaSourceSplit> getSourceSplits() {
            return copySplits(sourceSplits);
        }
    }

    private static byte[] serializeSplit(KafkaSourceSplit split) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(output)) {
            objectOutputStream.writeObject(split);
        }
        return output.toByteArray();
    }

    private static KafkaSourceSplit deserializeSplit(byte[] serializedSplit)
            throws IOException, ClassNotFoundException {
        try (ObjectInputStream objectInputStream =
                new KafkaGateObjectInputStream(new ByteArrayInputStream(serializedSplit))) {
            Object split = objectInputStream.readObject();
            if (!(split instanceof KafkaSourceSplit)) {
                throw new IOException("Unexpected Kafka source gate split type: " + split);
            }
            return ((KafkaSourceSplit) split).copy();
        }
    }

    private static byte[] sha256(byte[] payload) throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 is required by the Java runtime", e);
        }
    }

    /** Restricts restored gate split payloads to the split classes owned by this reader. */
    private static final class KafkaGateObjectInputStream extends ObjectInputStream {

        private KafkaGateObjectInputStream(ByteArrayInputStream input) throws IOException {
            super(input);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass descriptor)
                throws IOException, ClassNotFoundException {
            String className = descriptor.getName();
            if (isAllowedClass(className)) {
                return super.resolveClass(descriptor);
            }
            throw new IOException("Rejected Kafka source gate split class: " + className);
        }

        private static boolean isAllowedClass(String className) {
            return className.equals("[B")
                    || className.equals("java.lang.String")
                    || className.equals("java.lang.Long")
                    || className.equals("java.lang.Integer")
                    || className.equals(KafkaSourceSplit.class.getName())
                    || className.equals(TopicPartition.class.getName())
                    || className.equals(
                            org.apache.seatunnel.api.table.catalog.TablePath.class.getName());
        }
    }
}
