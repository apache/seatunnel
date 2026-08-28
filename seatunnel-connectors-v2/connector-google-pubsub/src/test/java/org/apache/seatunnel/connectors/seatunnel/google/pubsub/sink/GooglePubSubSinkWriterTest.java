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

package org.apache.seatunnel.connectors.seatunnel.google.pubsub.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.GooglePubSubSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.google.pubsub.exception.GooglePubSubConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class GooglePubSubSinkWriterTest {

    private SeaTunnelRowType rowType;
    private TestPubSubPublisher publisher;

    @BeforeEach
    void setUp() {
        rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        publisher = new TestPubSubPublisher();
    }

    @Test
    void shouldSerializeRowAsJson() throws IOException {
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");

        writer.write(row("alice", 30));

        Assertions.assertEquals(
                "{\"name\":\"alice\",\"age\":30}",
                publisher.messages.get(0).getData().toStringUtf8());
    }

    @Test
    void shouldUseConfiguredTextDelimiter() throws IOException {
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.TEXT, "|");

        writer.write(row("alice", 30));

        Assertions.assertEquals("alice|30", publisher.messages.get(0).getData().toStringUtf8());
    }

    @Test
    void shouldFlushOutstandingMessagesBeforeCommit() throws IOException {
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");
        writer.write(row("alice", 30));

        writer.prepareCommit();

        Assertions.assertEquals(1, publisher.flushCount);
    }

    @Test
    void shouldSurfaceAsynchronousPublishFailure() throws IOException {
        SettableApiFuture<String> future = SettableApiFuture.create();
        publisher.nextFuture = future;
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");
        writer.write(row("alice", 30));
        future.setException(new IOException("publish failed"));

        GooglePubSubConnectorException exception =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class, writer::prepareCommit);
        Assertions.assertTrue(exception.getMessage().contains("Failed to publish message"));
    }

    @Test
    void shouldSurfaceSynchronousPublishFailure() {
        publisher.publishFailure = new IllegalStateException("publish failed");
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");

        GooglePubSubConnectorException exception =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class, () -> writer.write(row("alice", 30)));
        Assertions.assertTrue(exception.getMessage().contains("Failed to publish message"));
    }

    @Test
    void shouldSurfaceFlushFailure() throws IOException {
        publisher.flushFailure = new IllegalStateException("flush failed");
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");
        writer.write(row("alice", 30));

        GooglePubSubConnectorException exception =
                Assertions.assertThrows(
                        GooglePubSubConnectorException.class, writer::prepareCommit);
        Assertions.assertTrue(exception.getMessage().contains("Failed to publish message"));
    }

    @Test
    void shouldClosePublisherAfterPublishFailure() throws IOException {
        SettableApiFuture<String> future = SettableApiFuture.create();
        publisher.nextFuture = future;
        GooglePubSubSinkWriter writer = createWriter(MessageFormat.JSON, ",");
        writer.write(row("alice", 30));
        future.setException(new IOException("publish failed"));

        Assertions.assertThrows(GooglePubSubConnectorException.class, writer::close);
        Assertions.assertTrue(publisher.closed);
    }

    private GooglePubSubSinkWriter createWriter(MessageFormat format, String delimiter) {
        GooglePubSubSinkConfig config =
                GooglePubSubSinkConfig.builder()
                        .projectId("test-project")
                        .topic("test-topic")
                        .format(format)
                        .fieldDelimiter(delimiter)
                        .build();
        return new GooglePubSubSinkWriter(rowType, config, publisher);
    }

    private SeaTunnelRow row(String name, int age) {
        return new SeaTunnelRow(new Object[] {name, age});
    }

    private static class TestPubSubPublisher implements PubSubPublisher {

        private final List<PubsubMessage> messages = new ArrayList<>();
        private ApiFuture<String> nextFuture = ApiFutures.immediateFuture("message-id");
        private RuntimeException publishFailure;
        private RuntimeException flushFailure;
        private int flushCount;
        private boolean closed;

        @Override
        public ApiFuture<String> publish(PubsubMessage message) {
            if (publishFailure != null) {
                throw publishFailure;
            }
            messages.add(message);
            return nextFuture;
        }

        @Override
        public void publishAllOutstanding() {
            flushCount++;
            if (flushFailure != null) {
                throw flushFailure;
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
