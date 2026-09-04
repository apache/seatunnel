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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.amazonsqs.exception.AmazonSqsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class AmazonSqsSourceReaderTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

    @Test
    void shouldNotDeleteMessageWhenDeserializationThrows() {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("invalid", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, true, new FailingDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        AmazonSqsConnectorException exception =
                Assertions.assertThrows(
                        AmazonSqsConnectorException.class, () -> reader.pollNext(collector));

        Assertions.assertTrue(
                exception.getMessage().contains("Failed to deserialize Amazon SQS message"));
        Assertions.assertInstanceOf(IOException.class, exception.getCause());
        Assertions.assertEquals("invalid payload", exception.getCause().getMessage());
        Assertions.assertTrue(collector.records.isEmpty());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    @Test
    void shouldNotDeleteMessageWhenDeserializerReturnsNull() {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("invalid", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, true, new NullDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        AmazonSqsConnectorException exception =
                Assertions.assertThrows(
                        AmazonSqsConnectorException.class, () -> reader.pollNext(collector));

        Assertions.assertTrue(
                exception.getMessage().contains("Failed to deserialize Amazon SQS message"));
        Assertions.assertTrue(collector.records.isEmpty());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    @Test
    void shouldDeleteMessageAfterSuccessfulCollectionWhenEnabled() throws Exception {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("order-1", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, true, new TestDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        reader.pollNext(collector);

        Assertions.assertEquals(Collections.singletonList("order-1"), collector.values());
        Assertions.assertEquals(
                Collections.singletonList("receipt-1"), sqsClient.deletedReceiptHandles());
        Assertions.assertEquals(1, context.noMoreElementSignals);
    }

    @Test
    void shouldKeepMessageAfterSuccessfulCollectionWhenDeletionDisabled() throws Exception {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("order-1", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, false, new TestDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        reader.pollNext(collector);

        Assertions.assertEquals(Collections.singletonList("order-1"), collector.values());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(1, context.noMoreElementSignals);
    }

    @Test
    void shouldNotDeleteMessageWhenCollectionFails() {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("order-1", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, true, new TestDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector("collector rejected row");

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class, () -> reader.pollNext(collector));

        Assertions.assertTrue(exception.getMessage().contains("collector rejected row"));
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    @Test
    void shouldStopBatchAtFailedMessageWithoutDeletingItOrLaterMessages() {
        RecordingSqsClient sqsClient =
                new RecordingSqsClient(
                        message("order-1", "receipt-1"),
                        message("invalid", "receipt-2"),
                        message("order-3", "receipt-3"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, true, new SelectiveFailingDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        AmazonSqsConnectorException exception =
                Assertions.assertThrows(
                        AmazonSqsConnectorException.class, () -> reader.pollNext(collector));

        Assertions.assertTrue(
                exception.getMessage().contains("Failed to deserialize Amazon SQS message"));
        Assertions.assertEquals(Collections.singletonList("order-1"), collector.values());
        Assertions.assertEquals(
                Collections.singletonList("receipt-1"), sqsClient.deletedReceiptHandles());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    @Test
    void shouldSkipFailedMessageAndContinueWhenParseErrorsIgnored() throws Exception {
        RecordingSqsClient sqsClient =
                new RecordingSqsClient(
                        message("order-1", "receipt-1"),
                        message("invalid", "receipt-2"),
                        message("order-3", "receipt-3"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(
                        context,
                        true,
                        true,
                        new SelectiveFailingDeserializationSchema(),
                        sqsClient);
        RecordingCollector collector = new RecordingCollector();

        reader.pollNext(collector);

        Assertions.assertEquals(Arrays.asList("order-1", "order-3"), collector.values());
        Assertions.assertEquals(
                Arrays.asList("receipt-1", "receipt-2", "receipt-3"),
                sqsClient.deletedReceiptHandles());
        Assertions.assertEquals(1, context.noMoreElementSignals);
    }

    @Test
    void shouldKeepIgnoredNullMessageWhenDeletionDisabled() throws Exception {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("invalid", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(context, false, true, new NullDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        reader.pollNext(collector);

        Assertions.assertTrue(collector.records.isEmpty());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(1, context.noMoreElementSignals);
    }

    @Test
    void shouldApplyIgnoreParseErrorsToJsonMessagesCreatedByFactory() throws Exception {
        RecordingSqsClient sqsClient =
                new RecordingSqsClient(
                        message("invalid", "receipt-1"),
                        message("{\"value\":\"order-2\"}", "receipt-2"));
        RecordingReaderContext context = new RecordingReaderContext();

        Map<String, Object> fields = new HashMap<>();
        fields.put("value", "string");
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);
        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://sqs.us-east-1.amazonaws.com/123456789012/orders");
        options.put("region", "us-east-1");
        options.put("schema", schema);
        options.put("delete_message", true);
        options.put("ignore_parse_errors", true);

        TableSource<?, ?, ?> tableSource =
                new AmazonSqsSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(options),
                                        Thread.currentThread().getContextClassLoader()));
        AmazonSqsSource source = (AmazonSqsSource) tableSource.createSource();
        AmazonSqsSourceReader reader =
                (AmazonSqsSourceReader) source.createReader(new SingleSplitReaderContext(context));
        reader.sqsClient = sqsClient.client;
        RecordingCollector collector = new RecordingCollector();

        reader.pollNext(collector);

        Assertions.assertEquals(Collections.singletonList("order-2"), collector.values());
        Assertions.assertEquals(
                Arrays.asList("receipt-1", "receipt-2"), sqsClient.deletedReceiptHandles());
        Assertions.assertEquals(1, context.noMoreElementSignals);
    }

    @Test
    void shouldWrapJsonParseErrorCreatedByFactory() throws Exception {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("invalid", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();

        Map<String, Object> fields = new HashMap<>();
        fields.put("value", "string");
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", fields);
        Map<String, Object> options = new HashMap<>();
        options.put("url", "https://sqs.us-east-1.amazonaws.com/123456789012/orders");
        options.put("region", "us-east-1");
        options.put("schema", schema);
        options.put("delete_message", true);

        TableSource<?, ?, ?> tableSource =
                new AmazonSqsSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(options),
                                        Thread.currentThread().getContextClassLoader()));
        AmazonSqsSource source = (AmazonSqsSource) tableSource.createSource();
        AmazonSqsSourceReader reader =
                (AmazonSqsSourceReader) source.createReader(new SingleSplitReaderContext(context));
        reader.sqsClient = sqsClient.client;
        RecordingCollector collector = new RecordingCollector();

        AmazonSqsConnectorException exception =
                Assertions.assertThrows(
                        AmazonSqsConnectorException.class, () -> reader.pollNext(collector));

        Assertions.assertEquals(
                AmazonSqsConnectorErrorCode.DESERIALIZE_FAILED, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception.getMessage().contains("Failed to deserialize Amazon SQS message"));
        SeaTunnelRuntimeException cause =
                Assertions.assertInstanceOf(SeaTunnelRuntimeException.class, exception.getCause());
        Assertions.assertEquals(
                CommonErrorCode.JSON_OPERATION_FAILED, cause.getSeaTunnelErrorCode());
        Assertions.assertNotNull(cause.getCause());
        Assertions.assertTrue(collector.records.isEmpty());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    @Test
    void shouldPropagateNonJsonRuntimeExceptionWhenParseErrorsIgnored() {
        RecordingSqsClient sqsClient = new RecordingSqsClient(message("invalid", "receipt-1"));
        RecordingReaderContext context = new RecordingReaderContext();
        AmazonSqsSourceReader reader =
                createReader(
                        context, true, true, new UnsupportedDeserializationSchema(), sqsClient);
        RecordingCollector collector = new RecordingCollector();

        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> reader.pollNext(collector));

        Assertions.assertFalse(exception instanceof AmazonSqsConnectorException);
        Assertions.assertEquals(
                CommonErrorCode.OPERATION_NOT_SUPPORTED, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(collector.records.isEmpty());
        Assertions.assertTrue(sqsClient.deletedRequests.isEmpty());
        Assertions.assertEquals(0, context.noMoreElementSignals);
    }

    private static AmazonSqsSourceReader createReader(
            RecordingReaderContext context,
            boolean deleteMessage,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            RecordingSqsClient sqsClient) {
        return createReader(context, deleteMessage, false, deserializationSchema, sqsClient);
    }

    private static AmazonSqsSourceReader createReader(
            RecordingReaderContext context,
            boolean deleteMessage,
            boolean ignoreParseErrors,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            RecordingSqsClient sqsClient) {
        AmazonSqsSourceConfig config =
                new AmazonSqsSourceConfig(
                        "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
                        "us-east-1",
                        null,
                        null,
                        null,
                        deleteMessage,
                        ignoreParseErrors,
                        null);
        AmazonSqsSourceReader reader =
                new AmazonSqsSourceReader(
                        new SingleSplitReaderContext(context),
                        config,
                        deserializationSchema,
                        ROW_TYPE);
        reader.sqsClient = sqsClient.client;
        return reader;
    }

    private static Message message(String body, String receiptHandle) {
        return Message.builder().body(body).receiptHandle(receiptHandle).build();
    }

    private static class TestDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

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

    private static final class NullDeserializationSchema extends TestDeserializationSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            return null;
        }
    }

    private static final class SelectiveFailingDeserializationSchema
            extends TestDeserializationSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            String value = new String(message, StandardCharsets.UTF_8);
            if (value.equals("invalid")) {
                throw new IOException("invalid payload");
            }
            return super.deserialize(message);
        }
    }

    private static final class UnsupportedDeserializationSchema extends TestDeserializationSchema {
        @Override
        public SeaTunnelRow deserialize(byte[] message) throws IOException {
            throw CommonError.unsupportedOperation("AmazonSqs", "deserialize");
        }
    }

    private static final class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> records = new ArrayList<>();
        private final String failureMessage;

        private RecordingCollector() {
            this(null);
        }

        private RecordingCollector(String failureMessage) {
            this.failureMessage = failureMessage;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            if (failureMessage != null) {
                throw new IllegalStateException(failureMessage);
            }
            records.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }

        private List<String> values() {
            List<String> values = new ArrayList<>();
            for (SeaTunnelRow record : records) {
                values.add(record.getField(0).toString());
            }
            return values;
        }
    }

    private static final class RecordingReaderContext implements SourceReader.Context {
        private int noMoreElementSignals;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            noMoreElementSignals++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    private static final class RecordingSqsClient implements InvocationHandler {
        private final SqsClient client;
        private final List<Message> messages;
        private final List<DeleteMessageRequest> deletedRequests = new ArrayList<>();

        private RecordingSqsClient(Message... messages) {
            this.messages = Collections.unmodifiableList(Arrays.asList(messages));
            this.client =
                    (SqsClient)
                            Proxy.newProxyInstance(
                                    SqsClient.class.getClassLoader(),
                                    new Class<?>[] {SqsClient.class},
                                    this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            if (method.getDeclaringClass() == Object.class) {
                return invokeObjectMethod(proxy, method, args);
            }
            if (method.getName().equals("receiveMessage")) {
                return ReceiveMessageResponse.builder().messages(messages).build();
            }
            if (method.getName().equals("deleteMessage")) {
                deletedRequests.add((DeleteMessageRequest) args[0]);
                return DeleteMessageResponse.builder().build();
            }
            if (method.getName().equals("close")) {
                return null;
            }
            throw new UnsupportedOperationException(method.getName());
        }

        private Object invokeObjectMethod(Object proxy, Method method, Object[] args) {
            if (method.getName().equals("toString")) {
                return "RecordingSqsClient";
            }
            if (method.getName().equals("hashCode")) {
                return System.identityHashCode(proxy);
            }
            if (method.getName().equals("equals")) {
                return proxy == args[0];
            }
            throw new UnsupportedOperationException(method.getName());
        }

        private List<String> deletedReceiptHandles() {
            List<String> receiptHandles = new ArrayList<>();
            for (DeleteMessageRequest request : deletedRequests) {
                receiptHandles.add(request.receiptHandle());
            }
            return receiptHandles;
        }
    }
}
