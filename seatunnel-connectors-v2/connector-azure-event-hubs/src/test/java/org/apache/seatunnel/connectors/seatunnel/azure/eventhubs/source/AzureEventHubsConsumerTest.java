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

import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.config.AzureEventHubsStartMode;
import org.apache.seatunnel.connectors.seatunnel.azure.eventhubs.exception.AzureEventHubsConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class AzureEventHubsConsumerTest {

    @Test
    void earliestUsesTheCurrentPartitionBeginning() {
        Assertions.assertEquals(
                41L,
                AzureEventHubsConsumer.resolveInitialSequenceNumber(
                        41L, 99L, AzureEventHubsStartMode.EARLIEST));
    }

    @Test
    void latestStartsAfterTheLastEnqueuedEvent() {
        Assertions.assertEquals(
                100L,
                AzureEventHubsConsumer.resolveInitialSequenceNumber(
                        41L, 99L, AzureEventHubsStartMode.LATEST));
    }

    @Test
    void partitionReceiverKeepsOneSubscriptionAcrossPolls() {
        AtomicInteger subscriptions = new AtomicInteger();
        AzureEventHubsConsumer.PartitionReceiver receiver =
                new AzureEventHubsConsumer.PartitionReceiver("0", 2);
        receiver.subscribe(
                Flux.defer(
                        () -> {
                            subscriptions.incrementAndGet();
                            return Flux.just(event(10L), event(11L)).concatWith(Flux.never());
                        }));

        List<EventHubsRecord> first = receiver.poll(1, Duration.ofSeconds(1));
        List<EventHubsRecord> second = receiver.poll(1, Duration.ofSeconds(1));
        receiver.close();

        Assertions.assertEquals(1, subscriptions.get());
        Assertions.assertEquals(10L, first.get(0).getSequenceNumber());
        Assertions.assertEquals(11L, second.get(0).getSequenceNumber());
    }

    @Test
    void partitionReceiverDeliversBufferedRecordsBeforeTerminalError() {
        AzureEventHubsConsumer.PartitionReceiver receiver =
                new AzureEventHubsConsumer.PartitionReceiver("0", 2);
        IllegalStateException failure = new IllegalStateException("receiver failed");
        receiver.subscribe(Flux.concat(Flux.just(event(10L)), Flux.error(failure)));

        List<EventHubsRecord> records = receiver.poll(2, Duration.ofSeconds(1));
        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () -> receiver.poll(2, Duration.ofSeconds(1)));
        receiver.close();

        Assertions.assertEquals(10L, records.get(0).getSequenceNumber());
        Assertions.assertSame(failure, exception.getCause());
    }

    @Test
    void partitionReceiverRetainsBufferOverflowFailureWhenSignalQueueIsFull() {
        AzureEventHubsConsumer.PartitionReceiver receiver =
                new AzureEventHubsConsumer.PartitionReceiver("0", 1);
        receiver.hookOnNext(event(10L));
        receiver.hookOnNext(event(11L));
        receiver.hookOnNext(event(12L));

        List<EventHubsRecord> records = receiver.poll(2, Duration.ZERO);
        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () -> receiver.poll(1, Duration.ZERO));
        receiver.close();

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(10L, records.get(0).getSequenceNumber());
        Assertions.assertEquals(11L, records.get(1).getSequenceNumber());
        Assertions.assertInstanceOf(IllegalStateException.class, exception.getCause());
        Assertions.assertTrue(exception.getCause().getMessage().contains("buffer is full"));
    }

    @Test
    void partitionReceiverRetainsCompletionWhenSignalQueueIsFull() {
        AzureEventHubsConsumer.PartitionReceiver receiver =
                new AzureEventHubsConsumer.PartitionReceiver("0", 1);
        receiver.hookOnNext(event(10L));
        receiver.hookOnNext(event(11L));
        receiver.hookOnComplete();

        List<EventHubsRecord> records = receiver.poll(2, Duration.ZERO);
        AzureEventHubsConnectorException exception =
                Assertions.assertThrows(
                        AzureEventHubsConnectorException.class,
                        () -> receiver.poll(1, Duration.ZERO));
        receiver.close();

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(10L, records.get(0).getSequenceNumber());
        Assertions.assertEquals(11L, records.get(1).getSequenceNumber());
        Assertions.assertNull(exception.getCause());
        Assertions.assertTrue(exception.getMessage().contains("completed unexpectedly"));
    }

    private EventHubsRecord event(long sequenceNumber) {
        return new EventHubsRecord(new byte[] {1}, sequenceNumber);
    }
}
