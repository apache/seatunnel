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
package org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.source;

import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsBySplits;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsChange;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polls one assigned partition per fetch in round-robin order. The fetch position advances when an
 * event enters the bounded common-reader queue, while checkpoint state advances only when the event
 * is emitted. This lets queued but un-emitted events replay after recovery without duplicate
 * polling during normal execution.
 */
@Slf4j
public class AzureEventHubsSourceSplitReader
        implements SplitReader<EventHubsRecord, AzureEventHubsSourceSplit> {

    private final AzureEventHubsSourceConfig config;
    private final EventHubsConsumerFactory consumerFactory;
    private final Duration pollTimeout;
    private final Map<String, PartitionReadState> assignedSplits = new LinkedHashMap<>();
    private final AtomicBoolean wakeUp = new AtomicBoolean();

    private EventHubsConsumer consumer;
    private int nextSplitIndex;

    public AzureEventHubsSourceSplitReader(
            AzureEventHubsSourceConfig config, EventHubsConsumerFactory consumerFactory) {
        this.config = config;
        this.consumerFactory = consumerFactory;
        this.pollTimeout = Duration.ofMillis(config.getPollTimeoutMs());
    }

    @Override
    public RecordsWithSplitIds<EventHubsRecord> fetch() throws IOException {
        if (assignedSplits.isEmpty() || wakeUp.getAndSet(false)) {
            return emptyRecords();
        }

        List<PartitionReadState> states = new ArrayList<>(assignedSplits.values());
        if (nextSplitIndex >= states.size()) {
            nextSplitIndex = 0;
        }
        PartitionReadState state = states.get(nextSplitIndex);
        nextSplitIndex = (nextSplitIndex + 1) % states.size();

        List<EventHubsRecord> records =
                consumer()
                        .receive(
                                state.split.getPartitionId(),
                                state.nextSequenceNumber,
                                config.getMaxBatchSize(),
                                pollTimeout);
        if (!records.isEmpty()) {
            state.nextSequenceNumber =
                    validateAndGetNextSequenceNumber(
                            records, state.nextSequenceNumber, state.split.getPartitionId());
        }

        Map<String, Collection<EventHubsRecord>> bySplit = new LinkedHashMap<>();
        if (!records.isEmpty()) {
            bySplit.put(state.split.splitId(), records);
        }
        return new RecordsBySplits<>(bySplit, Collections.emptySet());
    }

    private long validateAndGetNextSequenceNumber(
            List<EventHubsRecord> records, long expectedSequenceNumber, String partitionId) {
        long nextSequenceNumber = expectedSequenceNumber;
        try {
            for (EventHubsRecord record : records) {
                if (record.getSequenceNumber() != nextSequenceNumber) {
                    throw new AzureEventHubsConnectorException(
                            AzureEventHubsConnectorErrorCode.READ_FAILED,
                            "Expected Event Hubs partition '"
                                    + partitionId
                                    + "' sequence number "
                                    + nextSequenceNumber
                                    + " but received "
                                    + record.getSequenceNumber()
                                    + ". The checkpoint position may no longer be retained");
                }
                nextSequenceNumber = Math.addExact(nextSequenceNumber, 1L);
            }
            return nextSequenceNumber;
        } catch (ArithmeticException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.READ_FAILED,
                    "Event Hubs sequence number overflow in partition '" + partitionId + "'",
                    e);
        }
    }

    private RecordsWithSplitIds<EventHubsRecord> emptyRecords() {
        return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
    }

    private EventHubsConsumer consumer() {
        if (consumer == null) {
            consumer = consumerFactory.create(config);
        }
        return consumer;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<AzureEventHubsSourceSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    "Unsupported split change: " + splitsChanges.getClass().getName());
        }
        for (AzureEventHubsSourceSplit split : splitsChanges.splits()) {
            if (assignedSplits.containsKey(split.splitId())) {
                throw new IllegalStateException("Split already assigned: " + split.splitId());
            }
            assignedSplits.put(
                    split.splitId(), new PartitionReadState(split, split.getNextSequenceNumber()));
            log.info(
                    "Assigned Event Hubs partition {} at sequence number {}",
                    split.getPartitionId(),
                    split.getNextSequenceNumber());
        }
    }

    @Override
    public void wakeUp() {
        wakeUp.set(true);
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
    }

    private static class PartitionReadState {
        private final AzureEventHubsSourceSplit split;
        private long nextSequenceNumber;

        private PartitionReadState(AzureEventHubsSourceSplit split, long nextSequenceNumber) {
            this.split = split;
            this.nextSequenceNumber = nextSequenceNumber;
        }
    }
}
