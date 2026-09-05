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
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsStartMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;

import org.reactivestreams.Subscription;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.PartitionProperties;
import com.azure.messaging.eventhubs.models.EventPosition;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Synchronous Azure SDK adapter. Each instance is confined to one SeaTunnel reader thread. */
public class AzureEventHubsConsumer implements EventHubsConsumer {

    private final EventHubConsumerAsyncClient client;
    private final int prefetchCount;
    private final Map<String, PartitionReceiver> partitionReceivers = new HashMap<>();

    public AzureEventHubsConsumer(AzureEventHubsSourceConfig config) {
        try {
            this.prefetchCount = config.getPrefetchCount();
            this.client =
                    new EventHubClientBuilder()
                            .connectionString(
                                    config.getConnectionString(), config.getEventHubName())
                            .consumerGroup(config.getConsumerGroup())
                            .prefetchCount(config.getPrefetchCount())
                            .buildAsyncConsumerClient();
        } catch (RuntimeException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.CONNECTION_FAILED,
                    "Could not create Event Hubs consumer for hub '"
                            + config.getEventHubName()
                            + "'",
                    e);
        }
    }

    @Override
    public List<String> partitionIds() {
        try {
            List<String> partitionIds = client.getPartitionIds().collectList().block();
            if (partitionIds == null) {
                throw new IllegalStateException(
                        "Event Hubs partition discovery returned no result");
            }
            return partitionIds;
        } catch (RuntimeException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.PARTITION_DISCOVERY_FAILED,
                    "Could not list Event Hubs partitions",
                    e);
        }
    }

    @Override
    public long initialSequenceNumber(String partitionId, AzureEventHubsStartMode startMode) {
        try {
            PartitionProperties properties = client.getPartitionProperties(partitionId).block();
            if (properties == null) {
                throw new IllegalStateException(
                        "Event Hubs partition properties returned no result for " + partitionId);
            }
            return resolveInitialSequenceNumber(
                    properties.getBeginningSequenceNumber(),
                    properties.getLastEnqueuedSequenceNumber(),
                    startMode);
        } catch (RuntimeException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.PARTITION_DISCOVERY_FAILED,
                    "Could not resolve initial position for Event Hubs partition '"
                            + partitionId
                            + "'",
                    e);
        }
    }

    static long resolveInitialSequenceNumber(
            long beginningSequenceNumber,
            long lastEnqueuedSequenceNumber,
            AzureEventHubsStartMode startMode) {
        if (startMode == AzureEventHubsStartMode.EARLIEST) {
            return beginningSequenceNumber;
        }
        return Math.addExact(lastEnqueuedSequenceNumber, 1L);
    }

    @Override
    public List<EventHubsRecord> receive(
            String partitionId, long nextSequenceNumber, int maxEvents, Duration maximumWaitTime) {
        try {
            PartitionReceiver receiver = partitionReceivers.get(partitionId);
            if (receiver == null) {
                receiver = new PartitionReceiver(partitionId, prefetchCount);
                Flux<EventHubsRecord> events =
                        client.receiveFromPartition(
                                        partitionId,
                                        EventPosition.fromSequenceNumber(nextSequenceNumber, true))
                                .map(
                                        event ->
                                                new EventHubsRecord(
                                                        event.getData().getBody(),
                                                        event.getData().getSequenceNumber()));
                receiver.subscribe(events);
                partitionReceivers.put(partitionId, receiver);
            }
            return receiver.poll(maxEvents, maximumWaitTime);
        } catch (RuntimeException e) {
            if (e instanceof AzureEventHubsConnectorException) {
                throw e;
            }
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.READ_FAILED,
                    "Could not read Event Hubs partition '"
                            + partitionId
                            + "' from sequence number "
                            + nextSequenceNumber,
                    e);
        }
    }

    @Override
    public void close() {
        try {
            partitionReceivers.values().forEach(PartitionReceiver::close);
            partitionReceivers.clear();
            client.close();
        } catch (RuntimeException e) {
            throw new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.CLOSE_FAILED,
                    "Could not close Event Hubs consumer",
                    e);
        }
    }

    /**
     * Owns one long-lived Azure receiver link and applies bounded demand. Terminal signals are
     * retained until all events queued ahead of them have been returned to the source reader.
     */
    static class PartitionReceiver extends BaseSubscriber<EventHubsRecord> {

        private final String partitionId;
        private final int prefetchCount;
        private final LinkedBlockingQueue<ReceiverSignal> signals;

        private final AtomicReference<ReceiverSignal> pendingTerminalSignal =
                new AtomicReference<>();

        PartitionReceiver(String partitionId, int prefetchCount) {
            this.partitionId = partitionId;
            this.prefetchCount = prefetchCount;
            this.signals = new LinkedBlockingQueue<>(prefetchCount + 1);
        }

        void subscribe(Flux<EventHubsRecord> events) {
            events.subscribe(this);
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(prefetchCount);
        }

        @Override
        protected void hookOnNext(EventHubsRecord record) {
            if (!signals.offer(ReceiverSignal.record(record))) {
                cancel();
                hookOnError(
                        new IllegalStateException(
                                "Event Hubs receiver buffer is full for partition " + partitionId));
            }
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            offerTerminalSignal(ReceiverSignal.error(throwable));
        }

        @Override
        protected void hookOnComplete() {
            offerTerminalSignal(ReceiverSignal.complete());
        }

        List<EventHubsRecord> poll(int maxEvents, Duration maximumWaitTime) {
            ReceiverSignal terminalSignal = pendingTerminalSignal.get();
            if (terminalSignal != null && signals.isEmpty()) {
                throw terminalException(terminalSignal);
            }

            ReceiverSignal signal;
            try {
                signal = signals.poll(maximumWaitTime.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AzureEventHubsConnectorException(
                        AzureEventHubsConnectorErrorCode.READ_FAILED,
                        "Interrupted while reading Event Hubs partition '" + partitionId + "'",
                        e);
            }
            if (signal == null) {
                return new ArrayList<>();
            }

            List<EventHubsRecord> records = new ArrayList<>(Math.min(maxEvents, prefetchCount));
            collectSignal(signal, records);
            while (records.size() < maxEvents) {
                ReceiverSignal next = signals.poll();
                if (next == null) {
                    break;
                }
                collectSignal(next, records);
                if (next.record == null) {
                    break;
                }
            }

            terminalSignal = pendingTerminalSignal.get();
            if (records.isEmpty() && terminalSignal != null) {
                throw terminalException(terminalSignal);
            }
            return records;
        }

        private void offerTerminalSignal(ReceiverSignal terminalSignal) {
            if (!signals.offer(terminalSignal)) {
                pendingTerminalSignal.compareAndSet(null, terminalSignal);
            }
        }

        private void collectSignal(ReceiverSignal signal, List<EventHubsRecord> records) {
            if (signal.record != null) {
                records.add(signal.record);
                request(1);
            } else {
                pendingTerminalSignal.compareAndSet(null, signal);
            }
        }

        private AzureEventHubsConnectorException terminalException(ReceiverSignal signal) {
            if (signal.error != null) {
                return new AzureEventHubsConnectorException(
                        AzureEventHubsConnectorErrorCode.READ_FAILED,
                        "Event Hubs receiver failed for partition '" + partitionId + "'",
                        signal.error);
            }
            return new AzureEventHubsConnectorException(
                    AzureEventHubsConnectorErrorCode.READ_FAILED,
                    "Event Hubs receiver completed unexpectedly for partition '"
                            + partitionId
                            + "'");
        }

        void close() {
            cancel();
            signals.clear();
            pendingTerminalSignal.set(null);
        }
    }

    private static class ReceiverSignal {
        private final EventHubsRecord record;
        private final Throwable error;

        private ReceiverSignal(EventHubsRecord record, Throwable error) {
            this.record = record;
            this.error = error;
        }

        private static ReceiverSignal record(EventHubsRecord record) {
            return new ReceiverSignal(record, null);
        }

        private static ReceiverSignal error(Throwable error) {
            return new ReceiverSignal(null, error);
        }

        private static ReceiverSignal complete() {
            return new ReceiverSignal(null, null);
        }
    }
}
