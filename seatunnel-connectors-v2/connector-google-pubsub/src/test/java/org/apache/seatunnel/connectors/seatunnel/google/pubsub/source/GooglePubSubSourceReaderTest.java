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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.AckReplyConsumerWithResponse;
import com.google.cloud.pubsub.v1.AckResponse;
import com.google.cloud.pubsub.v1.MessageReceiverWithAckResponse;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class GooglePubSubSourceReaderTest {

    @Test
    void shouldAcknowledgeOnlyAfterCheckpointCompletes() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);
        TestAcknowledgement acknowledgement = new TestAcknowledgement();

        assignSplit(reader);
        subscriberFactory.emit("first", acknowledgement);
        reader.pollNext(new TestCollector());

        Assertions.assertEquals(0, acknowledgement.ackCount.get());
        reader.snapshotState(1L);
        Assertions.assertEquals(0, acknowledgement.ackCount.get());

        reader.notifyCheckpointComplete(1L);
        Assertions.assertEquals(1, acknowledgement.ackCount.get());
    }

    // An aborted checkpoint must not release messages. A later successful checkpoint owns them.
    @Test
    void shouldKeepAcknowledgementAfterCheckpointIsAborted() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);
        TestAcknowledgement acknowledgement = new TestAcknowledgement();

        assignSplit(reader);
        subscriberFactory.emit("first", acknowledgement);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);
        reader.notifyCheckpointAborted(1L);

        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);

        Assertions.assertEquals(1, acknowledgement.ackCount.get());
    }

    // Completing a newer overlapping checkpoint acknowledges every message contained in it once.
    @Test
    void shouldAcknowledgeMessagesFromOverlappingCheckpointsOnce() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);
        TestAcknowledgement first = new TestAcknowledgement();
        TestAcknowledgement second = new TestAcknowledgement();

        assignSplit(reader);
        subscriberFactory.emit("first", first);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);

        subscriberFactory.emit("second", second);
        reader.pollNext(new TestCollector());
        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);
        reader.notifyCheckpointComplete(1L);

        Assertions.assertEquals(1, first.ackCount.get());
        Assertions.assertEquals(1, second.ackCount.get());
    }

    @Test
    void shouldFailCheckpointWhenPubSubRejectsAcknowledgement() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);
        TestAcknowledgement acknowledgement = new TestAcknowledgement(AckResponse.INVALID);

        assignSplit(reader);
        subscriberFactory.emit("first", acknowledgement);
        reader.pollNext(new TestCollector());
        reader.snapshotState(1L);

        GooglePubSubConnectorException exception =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class,
                        () -> reader.notifyCheckpointComplete(1L));
        Assertions.assertTrue(exception.getMessage().contains("INVALID"));
    }

    @Test
    void shouldNackMessageWhenDeserializationFails() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader =
                new GooglePubSubSourceReader(new FailingDeserializationSchema(), subscriberFactory);
        TestAcknowledgement acknowledgement = new TestAcknowledgement();

        assignSplit(reader);
        subscriberFactory.emit("invalid", acknowledgement);

        Assertions.assertThrows(
                GooglePubSubConnectorException.class, () -> reader.pollNext(new TestCollector()));
        Assertions.assertEquals(1, acknowledgement.nackCount.get());
    }

    @Test
    void shouldPropagateSubscriberFailure() {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);

        assignSplit(reader);
        subscriberFactory.fail(new IOException("stream stopped"));

        GooglePubSubConnectorException exception =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class,
                        () -> reader.pollNext(new TestCollector()));
        Assertions.assertTrue(exception.getMessage().contains("stopped unexpectedly"));
    }

    @Test
    void shouldDeserializeMessagePayload() throws Exception {
        TestSubscriberFactory subscriberFactory = new TestSubscriberFactory();
        GooglePubSubSourceReader reader = createReader(subscriberFactory);
        TestCollector collector = new TestCollector();

        assignSplit(reader);
        subscriberFactory.emit("hello", new TestAcknowledgement());
        reader.pollNext(collector);

        Assertions.assertEquals("hello", collector.value);
    }

    private GooglePubSubSourceReader createReader(TestSubscriberFactory subscriberFactory) {
        return new GooglePubSubSourceReader(new TestDeserializationSchema(), subscriberFactory);
    }

    private void assignSplit(GooglePubSubSourceReader reader) {
        reader.addSplits(Collections.singletonList(new SingleSplit(null)));
    }

    private static final class TestSubscriberFactory
            implements GooglePubSubSourceReader.SubscriberFactory {
        private MessageReceiverWithAckResponse receiver;
        private Consumer<Throwable> failureHandler;

        @Override
        public PubSubSubscriber create(
                MessageReceiverWithAckResponse receiver, Consumer<Throwable> failureHandler) {
            this.receiver = receiver;
            this.failureHandler = failureHandler;
            return new PubSubSubscriber() {
                @Override
                public void start() {}

                @Override
                public void close() {}
            };
        }

        private void emit(String value, AckReplyConsumerWithResponse acknowledgement) {
            receiver.receiveMessage(
                    PubsubMessage.newBuilder()
                            .setMessageId(value)
                            .setData(ByteString.copyFromUtf8(value))
                            .build(),
                    acknowledgement);
        }

        private void fail(Throwable failure) {
            failureHandler.accept(failure);
        }
    }

    private static final class TestAcknowledgement implements AckReplyConsumerWithResponse {
        private final AtomicInteger ackCount = new AtomicInteger();
        private final AtomicInteger nackCount = new AtomicInteger();
        private final AckResponse response;

        private TestAcknowledgement() {
            this(AckResponse.SUCCESSFUL);
        }

        private TestAcknowledgement(AckResponse response) {
            this.response = response;
        }

        @Override
        public ApiFuture<AckResponse> ack() {
            ackCount.incrementAndGet();
            return ApiFutures.immediateFuture(response);
        }

        @Override
        public ApiFuture<AckResponse> nack() {
            nackCount.incrementAndGet();
            return ApiFutures.immediateFuture(AckResponse.SUCCESSFUL);
        }
    }

    private static class TestDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
        private static final SeaTunnelRowType ROW_TYPE =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            return new SeaTunnelRow(new Object[] {new String(message, StandardCharsets.UTF_8)});
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return ROW_TYPE;
        }
    }

    private static final class FailingDeserializationSchema extends TestDeserializationSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            throw new IOException("invalid payload");
        }
    }

    private static final class TestCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private String value;

        @Override
        public void collect(SeaTunnelRow record) {
            value = record.getField(0).toString();
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
