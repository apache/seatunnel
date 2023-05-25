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
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
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
import java.util.stream.Collectors;

public class PulsarSourceReader<T> implements SourceReader<T, PulsarPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceReader.class);
    protected final SourceReader.Context context;
    protected final PulsarClientConfig clientConfig;
    protected final PulsarConsumerConfig consumerConfig;
    protected final StartCursor startCursor;
    protected final Handover<RecordWithSplitId> handover;

    protected final Map<String, PulsarPartitionSplit> splitStates;
    protected final Map<String, PulsarSplitReaderThread> splitReaders;
    protected final SortedMap<Long, Map<String, MessageId>> pendingCursorsToCommit;
    protected final Map<String, MessageId> pendingCursorsToFinish;
    protected final Set<String> finishedSplits;

    protected final DeserializationSchema<T> deserialization;

    /** The maximum number of milliseconds to wait for a fetch batch. */
    protected final int pollTimeout;

    protected final long pollInterval;
    protected final int batchSize;

    protected PulsarClient pulsarClient;
    /** Indicating whether the SourceReader will be assigned more splits or not. */
    private boolean noMoreSplitsAssignment = false;

    public PulsarSourceReader(
            SourceReader.Context context,
            PulsarClientConfig clientConfig,
            PulsarConsumerConfig consumerConfig,
            StartCursor startCursor,
            DeserializationSchema<T> deserialization,
            int pollTimeout,
            long pollInterval,
            int batchSize) {
        this.context = context;
        this.clientConfig = clientConfig;
        this.consumerConfig = consumerConfig;
        this.startCursor = startCursor;
        this.deserialization = deserialization;
        this.pollTimeout = pollTimeout;
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
        this.splitStates = new HashMap<>();
        this.splitReaders = new HashMap<>();
        this.pendingCursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.pendingCursorsToFinish = Collections.synchronizedSortedMap(new TreeMap<>());
        this.finishedSplits = new TreeSet<>();
        this.handover = new Handover<>();
    }

    @Override
    public void open() {
        this.pulsarClient = PulsarConfigUtil.createClient(clientConfig);
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
                        CommonErrorCode.READER_OPERATION_FAILED,
                        "Failed to close the split reader thread.",
                        e);
            }
        }
    }

    @Override
    public void pollNext(Collector<T> output) throws Exception {
        for (int i = 0; i < batchSize; i++) {
            Optional<RecordWithSplitId> recordWithSplitId = handover.pollNext();
            if (recordWithSplitId.isPresent()) {
                final String splitId = recordWithSplitId.get().getSplitId();
                final Message<byte[]> message = recordWithSplitId.get().getMessage();
                synchronized (output.getCheckpointLock()) {
                    splitStates.get(splitId).setLatestConsumedId(message.getMessageId());
                    deserialization.deserialize(message.getData(), output);
                }
            }
            if (noMoreSplitsAssignment && finishedSplits.size() == splitStates.size()) {
                context.signalNoMoreElement();
                break;
            }
        }
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
                        CommonErrorCode.READER_OPERATION_FAILED,
                        "Failed to start the split reader thread.",
                        e);
            }
        }
    }

    protected PulsarSplitReaderThread createPulsarSplitReaderThread(PulsarPartitionSplit split) {
        return new PulsarSplitReaderThread(
                this,
                split,
                pulsarClient,
                consumerConfig,
                pollTimeout,
                pollInterval,
                startCursor,
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
                            CommonErrorCode.READER_OPERATION_FAILED,
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
}
