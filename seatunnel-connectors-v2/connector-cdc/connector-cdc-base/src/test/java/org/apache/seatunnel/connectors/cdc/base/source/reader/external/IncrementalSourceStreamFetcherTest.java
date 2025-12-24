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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.debezium.config.CommonConnectorConfig.TRANSACTION_TOPIC;
import static io.debezium.connector.AbstractSourceInfo.DEBEZIUM_CONNECTOR_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class IncrementalSourceStreamFetcherTest {
    private static final Configuration dezConf =
            JdbcConfiguration.create()
                    .with(Heartbeat.HEARTBEAT_INTERVAL, 1)
                    .with(TRANSACTION_TOPIC, "test")
                    .build();
    private static final String UNKNOWN_SCHEMA_KEY = "UNKNOWN";

    @Test
    public void testSplitSchemaChangeStream() throws Exception {
        IncrementalSourceStreamFetcher fetcher = createFetcher();

        List<DataChangeEvent> inputEvents = new ArrayList<>();
        List<SourceRecords> records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        Iterator<SourceRecords> outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals(2, records.get(0).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(1)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(1, records.get(0).getSourceRecordList().size());
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(0)));
        Assertions.assertEquals(3, records.get(1).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(2)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeUnknownEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(2, records.size());
        Assertions.assertEquals(3, records.get(0).getSourceRecordList().size());
        Assertions.assertEquals(3, records.get(1).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(2)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(2)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeUnknownEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals(1, records.get(0).getSourceRecordList().size());
        Assertions.assertEquals(3, records.get(1).getSourceRecordList().size());
        Assertions.assertEquals(2, records.get(2).getSourceRecordList().size());
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(2)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(2).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(2).getSourceRecordList().get(1)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals(2, records.get(0).getSourceRecordList().size());
        Assertions.assertEquals(3, records.get(1).getSourceRecordList().size());
        Assertions.assertEquals(1, records.get(2).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(2)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(2).getSourceRecordList().get(0)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(4, records.size());
        Assertions.assertEquals(2, records.get(0).getSourceRecordList().size());
        Assertions.assertEquals(2, records.get(1).getSourceRecordList().size());
        Assertions.assertEquals(2, records.get(2).getSourceRecordList().size());
        Assertions.assertEquals(2, records.get(3).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(2).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(2).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(3).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(3).getSourceRecordList().get(1)));

        inputEvents = new ArrayList<>();
        records = new ArrayList<>();
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createSchemaChangeEvent()));
        inputEvents.add(new DataChangeEvent(createDataEvent()));
        inputEvents.add(new DataChangeEvent(createHeartbeatEvent()));
        outputEvents = fetcher.splitSchemaChangeStream(inputEvents);
        outputEvents.forEachRemaining(records::add);

        Assertions.assertEquals(11, records.size());
        Assertions.assertEquals(3, records.get(0).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(0).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(0).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(0).getSourceRecordList().get(2)));
        Assertions.assertEquals(2, records.get(1).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(1).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(1).getSourceRecordList().get(1)));
        Assertions.assertEquals(2, records.get(2).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(2).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(2).getSourceRecordList().get(1)));
        Assertions.assertEquals(2, records.get(3).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(3).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(3).getSourceRecordList().get(1)));
        Assertions.assertEquals(3, records.get(4).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(4).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(4).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(4).getSourceRecordList().get(2)));
        Assertions.assertEquals(2, records.get(5).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(5).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(5).getSourceRecordList().get(1)));
        Assertions.assertEquals(4, records.get(6).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(6).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(6).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(6).getSourceRecordList().get(2)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(6).getSourceRecordList().get(3)));
        Assertions.assertEquals(3, records.get(7).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(7).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(7).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(7).getSourceRecordList().get(2)));
        Assertions.assertEquals(3, records.get(8).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(8).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(8).getSourceRecordList().get(1)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeBeforeWatermarkEvent(
                        records.get(8).getSourceRecordList().get(2)));
        Assertions.assertEquals(2, records.get(9).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isSchemaChangeEvent(records.get(9).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                WatermarkEvent.isSchemaChangeAfterWatermarkEvent(
                        records.get(9).getSourceRecordList().get(1)));
        Assertions.assertEquals(2, records.get(10).getSourceRecordList().size());
        Assertions.assertTrue(
                SourceRecordUtils.isDataChangeRecord(records.get(10).getSourceRecordList().get(0)));
        Assertions.assertTrue(
                SourceRecordUtils.isHeartbeatRecord(records.get(10).getSourceRecordList().get(1)));
    }

    static SourceRecord createSchemaChangeEvent() {
        return createSchemaChangeEvent("SCHEMA_CHANGE_TOPIC");
    }

    static SourceRecord createSchemaChangeUnknownEvent() {
        return createSchemaChangeEvent(UNKNOWN_SCHEMA_KEY);
    }

    static SourceRecord createSchemaChangeEvent(String topic) {
        Schema keySchema =
                SchemaBuilder.struct().name("io.debezium.connector.mysql.SchemaChangeKey").build();
        Schema valueKeySchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.Source")
                        .field(DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                        .build();
        Struct valueValues = new Struct(valueKeySchema);
        valueValues.put(DEBEZIUM_CONNECTOR_KEY, "mysql");

        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, valueKeySchema)
                        .name("")
                        .build();
        Struct value = new Struct(valueSchema);
        value.put(valueSchema.field(Envelope.FieldName.SOURCE), valueValues);
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        topic,
                        keySchema,
                        null,
                        valueSchema,
                        value);
        Assertions.assertTrue(SourceRecordUtils.isSchemaChangeEvent(record));
        return record;
    }

    static SourceRecord createDataEvent() {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema);
        value.put(valueSchema.field(Envelope.FieldName.OPERATION), "c");
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        null,
                        null,
                        null,
                        valueSchema,
                        value);
        Assertions.assertTrue(SourceRecordUtils.isDataChangeRecord(record));
        return record;
    }

    static SourceRecord createHeartbeatEvent() throws InterruptedException {
        TestConnectorConfig testConnectorConfig = new TestConnectorConfig(dezConf, "test", 1000);
        HeartbeatFactory<TableId> heartbeatFactory =
                new HeartbeatFactory<>(
                        testConnectorConfig,
                        TopicSelector.defaultSelector(
                                testConnectorConfig, (id, prefix, delimiter) -> "test"),
                        SchemaNameAdjuster.create());
        Heartbeat heartbeat = heartbeatFactory.createHeartbeat();
        AtomicReference<SourceRecord> eventRef = new AtomicReference<>();
        heartbeat.forcedBeat(
                Collections.singletonMap("heartbeat", "heartbeat"),
                Collections.singletonMap("heartbeat", "heartbeat"),
                sourceRecord -> eventRef.set(sourceRecord));
        return eventRef.get();
    }

    static IncrementalSourceStreamFetcher createFetcher() {
        SchemaChangeResolver schemaChangeResolver = mock(SchemaChangeResolver.class);
        when(schemaChangeResolver.support(any()))
                .thenAnswer(
                        (Answer<Boolean>)
                                invocationOnMock -> {
                                    SourceRecord record = invocationOnMock.getArgument(0);
                                    return record.topic() == null
                                            || !record.topic().equalsIgnoreCase(UNKNOWN_SCHEMA_KEY);
                                });
        IncrementalSourceStreamFetcher fetcher =
                new IncrementalSourceStreamFetcher(null, 0, schemaChangeResolver);
        IncrementalSourceStreamFetcher spy = spy(fetcher);
        doReturn(true).when(spy).shouldEmit(any());
        return spy;
    }

    public static class TestConnectorConfig extends CommonConnectorConfig {

        protected TestConnectorConfig(
                Configuration config, String logicalName, int defaultSnapshotFetchSize) {
            super(config, logicalName, defaultSnapshotFetchSize);
        }

        @Override
        public String getContextName() {
            return null;
        }

        @Override
        public String getConnectorName() {
            return null;
        }

        @Override
        protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
            return null;
        }
    }
}
