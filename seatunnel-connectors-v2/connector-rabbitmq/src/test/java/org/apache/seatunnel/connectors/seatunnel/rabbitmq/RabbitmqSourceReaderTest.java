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

package org.apache.seatunnel.connectors.seatunnel.rabbitmq;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.client.RabbitmqClient;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.config.RabbitmqConfig;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.exception.RabbitmqConnectorException;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.DeliveryMessage;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.source.RabbitmqSourceReader;
import org.apache.seatunnel.connectors.seatunnel.rabbitmq.split.RabbitmqSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

/**
 * Unit tests for {@link RabbitmqSourceReader}. This test class specifically validates the
 * robustness of the reader, focusing on edge-cases such as resource management, thread safety
 * during state snapshots, and fail-fast mechanisms.
 */
public class RabbitmqSourceReaderTest {

    /**
     * Verifies that duplicate splits assigned to the reader do not create duplicate consumers,
     * preventing memory and connection leaks. Also ensures that all active consumers are gracefully
     * canceled when the reader is closed.
     */
    @Test
    public void testConsumerResourceLeakAndDuplicates() throws Exception {
        // Intercept the creation of RabbitmqClient to prevent actual network connections
        try (MockedConstruction<RabbitmqClient> mocked =
                Mockito.mockConstruction(
                        RabbitmqClient.class,
                        (mock, context) -> {
                            Channel mockChannel = Mockito.mock(Channel.class);
                            Mockito.when(mockChannel.isOpen()).thenReturn(true);
                            Mockito.when(mock.getChannel()).thenReturn(mockChannel);

                            DefaultConsumer mockConsumer = Mockito.mock(DefaultConsumer.class);
                            Mockito.when(mockConsumer.getConsumerTag()).thenReturn("mock-tag");
                            Mockito.when(
                                            mock.getQueueingConsumer(
                                                    Mockito.any(), Mockito.anyString()))
                                    .thenReturn(mockConsumer);
                        })) {

            SourceReader.Context mockContext = Mockito.mock(SourceReader.Context.class);
            RabbitmqConfig mockConfig = Mockito.mock(RabbitmqConfig.class);
            RabbitmqSourceReader reader =
                    new RabbitmqSourceReader(new HashMap<>(), mockContext, mockConfig);

            RabbitmqClient mockClient = mocked.constructed().get(0);
            Channel mockChannel = mockClient.getChannel();

            RabbitmqSplit split = new RabbitmqSplit("test_queue");

            // First assignment of the split -> should initiate basicConsume
            reader.addSplits(Collections.singletonList(split));
            Mockito.verify(mockChannel, Mockito.times(1))
                    .basicConsume(
                            Mockito.eq("test_queue"),
                            Mockito.anyBoolean(),
                            Mockito.any(DefaultConsumer.class));

            // Assigning the SAME split again -> should be ignored (prevents duplicate leak)
            reader.addSplits(Collections.singletonList(split));
            Mockito.verify(mockChannel, Mockito.times(1)) // Verification count remains 1!
                    .basicConsume(
                            Mockito.eq("test_queue"),
                            Mockito.anyBoolean(),
                            Mockito.any(DefaultConsumer.class));

            // Close the reader -> should trigger basicCancel for the active consumer
            reader.close();
            Mockito.verify(mockChannel, Mockito.times(1)).basicCancel("mock-tag");
        }
    }

    /**
     * Verifies the thread safety of the internal split tracking collection. Ensures that adding
     * splits (done by the Enumerator thread) and snapshotting state (done by the Checkpoint thread)
     * can occur safely without throwing ConcurrentModificationException.
     */
    @Test
    public void testSourceSplitsThreadSafety() throws Exception {
        try (MockedConstruction<RabbitmqClient> mocked =
                Mockito.mockConstruction(
                        RabbitmqClient.class,
                        (mock, context) -> {
                            Channel mockChannel = Mockito.mock(Channel.class);
                            Mockito.when(mock.getChannel()).thenReturn(mockChannel);
                            DefaultConsumer mockConsumer = Mockito.mock(DefaultConsumer.class);
                            Mockito.when(
                                            mock.getQueueingConsumer(
                                                    Mockito.any(), Mockito.anyString()))
                                    .thenReturn(mockConsumer);
                        })) {

            SourceReader.Context mockContext = Mockito.mock(SourceReader.Context.class);
            Mockito.when(mockContext.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
            RabbitmqConfig mockConfig = Mockito.mock(RabbitmqConfig.class);

            RabbitmqSourceReader reader =
                    new RabbitmqSourceReader(new HashMap<>(), mockContext, mockConfig);

            // reader.open() MUST be called to initialize internal collections used by snapshotState
            reader.open();

            // Simulate adding a split while the system is running
            RabbitmqSplit split = new RabbitmqSplit("queue_ts");
            reader.addSplits(Collections.singletonList(split));

            // Verify that the snapshot correctly captures the split without throwing exceptions
            Assertions.assertEquals(1, reader.snapshotState(1L).size());
            Assertions.assertEquals("queue_ts", reader.snapshotState(1L).get(0).splitId());
        }
    }

    /**
     * Verifies the fail-fast behavior for unconfigured queues. If a message arrives from a queue
     * that does not have a mapped schema, the reader must immediately throw an exception rather
     * than silently dropping the data.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testSilentMessageDroppingThrowsException() throws Exception {
        try (MockedConstruction<RabbitmqClient> mocked =
                Mockito.mockConstruction(
                        RabbitmqClient.class,
                        (mock, context) -> {
                            Channel mockChannel = Mockito.mock(Channel.class);
                            Mockito.when(mock.getChannel()).thenReturn(mockChannel);
                        })) {

            SourceReader.Context mockContext = Mockito.mock(SourceReader.Context.class);
            Mockito.when(mockContext.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
            RabbitmqConfig mockConfig = Mockito.mock(RabbitmqConfig.class);

            // Initialize the reader with an EMPTY schema map
            RabbitmqSourceReader reader =
                    new RabbitmqSourceReader(new HashMap<>(), mockContext, mockConfig);
            reader.open();

            // Use Java Reflection to bypass normal consumption and inject a fake message
            // directly into the reader's internal BlockingQueue.
            Field queueField = RabbitmqSourceReader.class.getDeclaredField("queue");
            queueField.setAccessible(true);
            BlockingQueue<DeliveryMessage> internalQueue =
                    (BlockingQueue<DeliveryMessage>) queueField.get(reader);

            // Construct a fake RabbitMQ Delivery
            Envelope mockEnvelope = Mockito.mock(Envelope.class);
            Mockito.when(mockEnvelope.getDeliveryTag()).thenReturn(1L);

            Delivery mockDelivery = Mockito.mock(Delivery.class);
            Mockito.when(mockDelivery.getEnvelope()).thenReturn(mockEnvelope);
            Mockito.when(mockDelivery.getBody()).thenReturn(new byte[0]);
            Mockito.when(mockDelivery.getProperties())
                    .thenReturn(Mockito.mock(AMQP.BasicProperties.class));

            // Assign the message to an unknown/unconfigured split (queue)
            DeliveryMessage fakeMessage = Mockito.mock(DeliveryMessage.class);
            Mockito.when(fakeMessage.getSplitId()).thenReturn("unknown_ghost_queue");
            Mockito.when(fakeMessage.getDelivery()).thenReturn(mockDelivery);

            internalQueue.put(fakeMessage);

            // Mock the collector. Returning a new Object() for getCheckpointLock() prevents
            // a NullPointerException during the synchronized block inside pollNext().
            Collector<SeaTunnelRow> mockCollector = Mockito.mock(Collector.class);
            Mockito.when(mockCollector.getCheckpointLock()).thenReturn(new Object());

            // Executing pollNext() should pull the fake message, fail to find its schema, and
            // throw.
            RabbitmqConnectorException exception =
                    Assertions.assertThrows(
                            RabbitmqConnectorException.class, () -> reader.pollNext(mockCollector));

            Assertions.assertTrue(
                    exception.getMessage().contains("Cannot find schema or tableId for queue"),
                    "Should throw fail-fast exception instead of silently dropping the data.");
        }
    }
}
