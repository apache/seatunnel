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

package org.apache.seatunnel.connectors.seatunnel.azure.queue.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AuthenticationType;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.AzureQueueSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageEncoding;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.config.MessageFormat;
import org.apache.seatunnel.connectors.seatunnel.azure.queue.exception.AzureQueueConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class AzureQueueStorageSinkWriterTest {

    private final List<ExecutorService> executors = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (ExecutorService executor : executors) {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldSerializeRowAsJson() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        AzureQueueStorageSinkWriter writer = writer(twoFieldRowType(), config(), sender);

        writer.write(new SeaTunnelRow(new Object[] {"alice", 30}));

        Assertions.assertEquals("{\"name\":\"alice\",\"age\":30}", sender.messages.get(0));
    }

    @Test
    void shouldUseConfiguredTextDelimiter() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        AzureQueueSinkConfig config =
                config().toBuilder().format(MessageFormat.TEXT).fieldDelimiter("|").build();
        AzureQueueStorageSinkWriter writer = writer(twoFieldRowType(), config, sender);

        writer.write(new SeaTunnelRow(new Object[] {"alice", 30}));

        Assertions.assertEquals("alice|30", sender.messages.get(0));
    }

    @Test
    void shouldWaitForOutstandingSendsBeforeCommit() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        sender.nextFuture = pending;
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config(), sender);
        writer.write(row("message"));

        ExecutorService executor = executor();
        CompletableFuture<Void> commit =
                CompletableFuture.runAsync(writer::prepareCommit, executor);
        Assertions.assertFalse(commit.isDone());

        pending.complete(null);
        commit.join();
    }

    @Test
    void shouldBoundOutstandingSends() throws Exception {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> firstSend = new CompletableFuture<>();
        sender.nextFuture = firstSend;
        AzureQueueSinkConfig config = config().toBuilder().maxInFlight(1).build();
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config, sender);
        writer.write(row("first"));

        sender.nextFuture = CompletableFuture.completedFuture(null);
        ExecutorService executor = executor();
        CompletableFuture<Void> secondWrite =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                writer.write(row("second"));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        executor);
        Assertions.assertThrows(
                TimeoutException.class, () -> secondWrite.get(100, TimeUnit.MILLISECONDS));

        firstSend.complete(null);
        secondWrite.get(2, TimeUnit.SECONDS);
        Assertions.assertEquals(2, sender.messages.size());
    }

    @Test
    void shouldSurfaceAsynchronousSendFailure() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        sender.nextFuture = pending;
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config(), sender);
        writer.write(row("message"));
        pending.completeExceptionally(new IOException("send failed"));

        AzureQueueConnectorException exception =
                Assertions.assertThrows(AzureQueueConnectorException.class, writer::prepareCommit);

        Assertions.assertTrue(exception.getMessage().contains("Failed to send message"));
    }

    @Test
    void shouldWaitForOtherOutstandingSendsAfterFailure() throws Exception {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> failed = new CompletableFuture<>();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        sender.futures.add(failed);
        sender.futures.add(pending);
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config(), sender);
        writer.write(row("first"));
        writer.write(row("second"));
        failed.completeExceptionally(new IOException("send failed"));

        ExecutorService executor = executor();
        CompletableFuture<Void> commit =
                CompletableFuture.runAsync(writer::prepareCommit, executor);
        Assertions.assertThrows(
                TimeoutException.class, () -> commit.get(100, TimeUnit.MILLISECONDS));

        pending.complete(null);
        Assertions.assertThrows(java.util.concurrent.CompletionException.class, commit::join);
    }

    @Test
    void shouldReleaseSendCapacityAfterFailure() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> failed = new CompletableFuture<>();
        sender.nextFuture = failed;
        AzureQueueSinkConfig config = config().toBuilder().maxInFlight(1).build();
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config, sender);
        writer.write(row("message"));
        failed.completeExceptionally(new IOException("send failed"));

        Assertions.assertThrows(AzureQueueConnectorException.class, writer::prepareCommit);
        Assertions.assertThrows(
                AzureQueueConnectorException.class, () -> writer.write(row("next")));
    }

    @Test
    void shouldAllowMessageAtNoneEncodingLimit() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        AzureQueueSinkConfig config =
                config().toBuilder()
                        .format(MessageFormat.TEXT)
                        .messageEncoding(MessageEncoding.NONE)
                        .build();
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config, sender);

        writer.write(row(repeat('a', 65_536)));

        Assertions.assertEquals(65_536, sender.messages.get(0).length());
    }

    @Test
    void shouldRejectMessageAboveNoneEncodingLimit() {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        AzureQueueSinkConfig config =
                config().toBuilder()
                        .format(MessageFormat.TEXT)
                        .messageEncoding(MessageEncoding.NONE)
                        .build();
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config, sender);

        AzureQueueConnectorException exception =
                Assertions.assertThrows(
                        AzureQueueConnectorException.class,
                        () -> writer.write(row(repeat('a', 65_537))));

        Assertions.assertTrue(exception.getMessage().contains("65537 bytes"));
        Assertions.assertTrue(sender.messages.isEmpty());
    }

    @Test
    void shouldApplyBase64ExpansionToMessageLimit() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        AzureQueueSinkConfig config =
                config().toBuilder()
                        .format(MessageFormat.TEXT)
                        .messageEncoding(MessageEncoding.BASE64)
                        .build();
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config, sender);

        writer.write(row(repeat('a', 49_152)));
        Assertions.assertThrows(
                AzureQueueConnectorException.class, () -> writer.write(row(repeat('a', 49_153))));

        Assertions.assertEquals(1, sender.messages.size());
    }

    @Test
    void shouldCloseSenderAfterSendFailure() throws IOException {
        TestAzureQueueSender sender = new TestAzureQueueSender();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        sender.nextFuture = pending;
        AzureQueueStorageSinkWriter writer = writer(oneFieldRowType(), config(), sender);
        writer.write(row("message"));
        pending.completeExceptionally(new IOException("send failed"));

        Assertions.assertThrows(AzureQueueConnectorException.class, writer::close);
        Assertions.assertTrue(sender.closed);
    }

    private AzureQueueStorageSinkWriter writer(
            SeaTunnelRowType rowType, AzureQueueSinkConfig config, AzureQueueSender sender) {
        return new AzureQueueStorageSinkWriter(rowType, config, sender);
    }

    private AzureQueueSinkConfig config() {
        return AzureQueueSinkConfig.builder()
                .queueName("events")
                .authenticationType(AuthenticationType.CONNECTION_STRING)
                .connectionString("UseDevelopmentStorage=true")
                .format(MessageFormat.JSON)
                .fieldDelimiter(",")
                .messageEncoding(MessageEncoding.NONE)
                .maxInFlight(100)
                .operationTimeoutMillis(2_000L)
                .build();
    }

    private SeaTunnelRowType oneFieldRowType() {
        return new SeaTunnelRowType(
                new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    private SeaTunnelRowType twoFieldRowType() {
        return new SeaTunnelRowType(
                new String[] {"name", "age"},
                new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private SeaTunnelRow row(String value) {
        return new SeaTunnelRow(new Object[] {value});
    }

    private String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int index = 0; index < count; index++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private ExecutorService executor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executors.add(executor);
        return executor;
    }

    private static class TestAzureQueueSender implements AzureQueueSender {

        private final List<String> messages = new ArrayList<>();
        private final List<CompletableFuture<Void>> futures = new ArrayList<>();
        private CompletableFuture<Void> nextFuture = CompletableFuture.completedFuture(null);
        private RuntimeException sendFailure;
        private boolean closed;

        @Override
        public CompletableFuture<Void> send(String message) {
            if (sendFailure != null) {
                throw sendFailure;
            }
            messages.add(message);
            return futures.isEmpty() ? nextFuture : futures.remove(0);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
