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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarClientConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.config.PulsarConsumerConfig;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.pulsar.exception.PulsarConnectorException;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.PulsarConsumerMetadata;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.start.StartCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.cursor.stop.StopCursor;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.discoverer.TopicListDiscoverer;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.seatunnel.connectors.seatunnel.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class PulsarSourceReaderRestoreTest {

    @Test
    void shouldFallbackToSingleTableMetadataForLegacySplitWithoutOverridingRowTableId()
            throws Exception {
        TablePath tablePath = TablePath.of("db.orders");
        TestingPulsarSourceReader reader =
                new TestingPulsarSourceReader(
                        new TestingReaderContext(),
                        Collections.singletonMap(
                                tablePath,
                                createMetadata(
                                        tablePath, new TableIdAwareDeserializationSchema())));

        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/orders", 0),
                        StopCursor.never(),
                        null,
                        null);
        reader.addSplits(Collections.singletonList(split));

        reader.handover.produce(
                new RecordWithSplitId(
                        testingMessage(
                                "value".getBytes(StandardCharsets.UTF_8), MessageId.earliest),
                        split.splitId()));

        TestingCollector collector = new TestingCollector();
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.records.size());
        Assertions.assertEquals("deserializer.table", collector.records.get(0).getTableId());
    }

    @Test
    void shouldRejectLegacySplitRestoreForMultiTableReader() {
        TablePath ordersPath = TablePath.of("db.orders");
        TablePath usersPath = TablePath.of("db.users");
        Map<TablePath, PulsarConsumerMetadata> metadataMap = new LinkedHashMap<>();
        metadataMap.put(ordersPath, createMetadata(ordersPath));
        metadataMap.put(usersPath, createMetadata(usersPath));

        TestingPulsarSourceReader reader =
                new TestingPulsarSourceReader(new TestingReaderContext(), metadataMap);
        PulsarPartitionSplit legacySplit =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/orders", 0),
                        StopCursor.never(),
                        null,
                        null);

        Assertions.assertThrows(
                PulsarConnectorException.class,
                () -> reader.addSplits(Collections.singletonList(legacySplit)));
    }

    @Test
    void shouldInjectTableIdForMultiTableRouting() throws Exception {
        TablePath ordersPath = TablePath.of("db.orders");
        TablePath usersPath = TablePath.of("db.users");
        Map<TablePath, PulsarConsumerMetadata> metadataMap = new LinkedHashMap<>();
        metadataMap.put(ordersPath, createMetadata(ordersPath));
        metadataMap.put(usersPath, createMetadata(usersPath));

        TestingPulsarSourceReader reader =
                new TestingPulsarSourceReader(new TestingReaderContext(), metadataMap);
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/orders", 0),
                        StopCursor.never(),
                        null,
                        ordersPath);
        reader.addSplits(Collections.singletonList(split));

        reader.handover.produce(
                new RecordWithSplitId(
                        testingMessage(
                                "value".getBytes(StandardCharsets.UTF_8), MessageId.earliest),
                        split.splitId()));

        TestingCollector collector = new TestingCollector();
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.records.size());
        Assertions.assertEquals(ordersPath.toString(), collector.records.get(0).getTableId());
    }

    @Test
    void shouldInjectConfiguredTableIdForSingleTableTablesConfigs() throws Exception {
        TablePath tablePath = TablePath.of("db.orders");
        TestingPulsarSourceReader reader =
                new TestingPulsarSourceReader(
                        new TestingReaderContext(),
                        Collections.singletonMap(
                                tablePath,
                                createMetadata(tablePath, new TableIdAwareDeserializationSchema())),
                        true);
        PulsarPartitionSplit split =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/orders", 0),
                        StopCursor.never(),
                        null,
                        tablePath);
        reader.addSplits(Collections.singletonList(split));

        reader.handover.produce(
                new RecordWithSplitId(
                        testingMessage(
                                "value".getBytes(StandardCharsets.UTF_8), MessageId.earliest),
                        split.splitId()));

        TestingCollector collector = new TestingCollector();
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.records.size());
        Assertions.assertEquals(tablePath.toString(), collector.records.get(0).getTableId());
    }

    @Test
    void shouldDrainBufferedRecordsBeforeSignalingNoMoreElements() throws Exception {
        TablePath ordersPath = TablePath.of("db.orders");
        TablePath usersPath = TablePath.of("db.users");
        Map<TablePath, PulsarConsumerMetadata> metadataMap = new LinkedHashMap<>();
        metadataMap.put(ordersPath, createMetadata(ordersPath));
        metadataMap.put(usersPath, createMetadata(usersPath));

        TestingReaderContext context = new TestingReaderContext();
        TestingPulsarSourceReader reader = new TestingPulsarSourceReader(context, metadataMap);
        PulsarPartitionSplit ordersSplit =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/orders", 0),
                        StopCursor.never(),
                        null,
                        ordersPath);
        PulsarPartitionSplit usersSplit =
                new PulsarPartitionSplit(
                        new TopicPartition("persistent://public/default/users", 0),
                        StopCursor.never(),
                        null,
                        usersPath);
        reader.addSplits(Arrays.asList(ordersSplit, usersSplit));
        reader.handleNoMoreSplits();
        reader.handover.produce(
                new RecordWithSplitId(
                        testingMessage(
                                "order".getBytes(StandardCharsets.UTF_8), MessageId.earliest),
                        ordersSplit.splitId()));
        reader.handleNoMoreElements(ordersSplit.splitId(), MessageId.earliest);
        reader.handover.produce(
                new RecordWithSplitId(
                        testingMessage("user".getBytes(StandardCharsets.UTF_8), MessageId.earliest),
                        usersSplit.splitId()));
        reader.handleNoMoreElements(usersSplit.splitId(), MessageId.earliest);

        TestingCollector collector = new TestingCollector();
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.records.size());
        Assertions.assertEquals(0, context.noMoreElementSignals);

        reader.pollNext(collector);

        Assertions.assertEquals(2, collector.records.size());
        Assertions.assertEquals(1, context.noMoreElementSignals);
        Assertions.assertEquals(
                Arrays.asList(ordersPath.toString(), usersPath.toString()),
                Arrays.asList(
                        collector.records.get(0).getTableId(),
                        collector.records.get(1).getTableId()));
    }

    private static PulsarConsumerMetadata createMetadata(TablePath tablePath) {
        return createMetadata(tablePath, new TestingDeserializationSchema());
    }

    private static PulsarConsumerMetadata createMetadata(
            TablePath tablePath, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        return new PulsarConsumerMetadata(
                tablePath,
                CatalogTableUtil.buildSimpleTextTable(),
                deserializationSchema,
                new TopicListDiscoverer(Collections.singletonList(tablePath.toString())),
                StartCursor.earliest(),
                StopCursor.never(),
                PulsarConsumerConfig.builder().subscriptionName("seatunnel-sub").build());
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> testingMessage(byte[] value, MessageId messageId) {
        return (Message<byte[]>)
                Proxy.newProxyInstance(
                        Message.class.getClassLoader(),
                        new Class<?>[] {Message.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "getData":
                                case "getValue":
                                    return value;
                                case "getMessageId":
                                    return messageId;
                                case "size":
                                    return value.length;
                                case "hasKey":
                                case "hasOrderingKey":
                                case "hasBase64EncodedKey":
                                case "isReplicated":
                                case "hasBrokerPublishTime":
                                case "hasIndex":
                                    return false;
                                case "getProperties":
                                    return Collections.emptyMap();
                                case "getProperty":
                                case "getTopicName":
                                case "getProducerName":
                                case "getReplicatedFrom":
                                case "getKey":
                                case "getSchemaInternal":
                                case "getEncryptionCtx":
                                    return null;
                                case "getPublishTime":
                                case "getEventTime":
                                case "getSequenceId":
                                    return 0L;
                                case "getRedeliveryCount":
                                    return 0;
                                case "getSchemaVersion":
                                case "getKeyBytes":
                                case "getOrderingKey":
                                    return new byte[0];
                                case "getReaderSchema":
                                case "getBrokerPublishTime":
                                case "getIndex":
                                    return Optional.empty();
                                case "equals":
                                    return proxy == args[0];
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "toString":
                                    return "TestingMessage";
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unsupported method: " + method.getName());
                            }
                        });
    }

    private static final class TestingPulsarSourceReader extends PulsarSourceReader<SeaTunnelRow> {

        private TestingPulsarSourceReader(
                SourceReader.Context context, Map<TablePath, PulsarConsumerMetadata> metadataMap) {
            this(context, metadataMap, metadataMap.size() > 1);
        }

        private TestingPulsarSourceReader(
                SourceReader.Context context,
                Map<TablePath, PulsarConsumerMetadata> metadataMap,
                boolean injectTableId) {
            super(
                    context,
                    PulsarClientConfig.builder().serviceUrl("pulsar://localhost:6650").build(),
                    metadataMap,
                    injectTableId,
                    100,
                    50L,
                    1);
        }

        @Override
        protected PulsarSplitReaderThread createPulsarSplitReaderThread(
                PulsarPartitionSplit split) {
            TablePath tablePath =
                    split.getTablePath() != null ? split.getTablePath() : defaultTablePath;
            PulsarConsumerMetadata metadata =
                    tablePath == null ? null : consumerMetadataMap.get(tablePath);
            if (metadata == null && defaultTablePath != null) {
                metadata = consumerMetadataMap.get(defaultTablePath);
            }
            if (metadata == null) {
                throw new PulsarConnectorException(
                        PulsarConnectorErrorCode.DESERIALIZATION_SCHEMA_NOT_FOUND,
                        String.format("No consumer metadata found for table '%s'", tablePath));
            }
            return new NoopSplitReaderThread(this, split, handover);
        }
    }

    private static final class NoopSplitReaderThread extends PulsarSplitReaderThread {

        private NoopSplitReaderThread(
                PulsarSourceReader sourceReader,
                PulsarPartitionSplit split,
                org.apache.seatunnel.common.Handover<RecordWithSplitId> handover) {
            super(
                    sourceReader,
                    split,
                    null,
                    PulsarConsumerConfig.builder().subscriptionName("seatunnel-sub").build(),
                    100,
                    50L,
                    StartCursor.earliest(),
                    handover);
        }

        @Override
        public void open() {}

        @Override
        public synchronized void start() {}

        @Override
        public void close() throws IOException {}
    }

    private static final class TestingReaderContext implements SourceReader.Context {

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

    private static final class TestingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> records = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            records.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    private static class TestingDeserializationSchema
            implements DeserializationSchema<SeaTunnelRow> {

        private static final SeaTunnelRowType ROW_TYPE =
                new SeaTunnelRowType(
                        new String[] {"content"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        @Override
        public SeaTunnelRow deserialize(byte[] message) {
            return new SeaTunnelRow(new Object[] {new String(message, StandardCharsets.UTF_8)});
        }

        @Override
        public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
            return ROW_TYPE;
        }
    }

    private static final class TableIdAwareDeserializationSchema
            extends TestingDeserializationSchema {

        @Override
        public SeaTunnelRow deserialize(byte[] message) {
            SeaTunnelRow row = super.deserialize(message);
            row.setTableId("deserializer.table");
            return row;
        }
    }
}
