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
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.CompletedSnapshotSplitInfo;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
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
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
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

    static SourceRecord createDataEventWithSource() {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.postgresql.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("schema", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .field(DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                        .build();
        Struct sourceStruct = new Struct(sourceSchema);
        sourceStruct.put("db", "testdb");
        sourceStruct.put("schema", "public");
        sourceStruct.put("table", "test_table");
        sourceStruct.put(DEBEZIUM_CONNECTOR_KEY, "postgresql");

        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema);
        value.put(Envelope.FieldName.SOURCE, sourceStruct);
        value.put(Envelope.FieldName.OPERATION, "c");
        SourceRecord record =
                new SourceRecord(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "testdb.public.test_table",
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

    @Test
    public void testPollSplitRecordsDrainsQueueBeforeBoundedCompletion() throws Exception {
        IncrementalSourceStreamFetcher fetcher = createFetcher();

        // A bounded incremental split: the stop offset is set and is not the
        // never-stop sentinel, so the split can finish.
        Offset stopOffset = mock(Offset.class);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-0",
                        Collections.emptyList(),
                        null,
                        stopOffset,
                        Collections.emptyList());

        // The fetch task has already finished (isRunning() == false), simulating the
        // bounded reader reaching the stop offset and stopping the binlog context.
        FetchTask<SourceSplitBase> fetchTask = mock(FetchTask.class);
        when(fetchTask.isRunning()).thenReturn(false);
        when(fetchTask.getSplit()).thenReturn(incrementalSplit);

        // The queue still holds one last batch that must be drained before completion.
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        when(queue.poll())
                .thenReturn(
                        Arrays.asList(
                                new DataChangeEvent(createDataEvent()),
                                new DataChangeEvent(createDataEvent())),
                        Collections.emptyList());

        setField(fetcher, "queue", queue);
        setField(fetcher, "streamFetchTask", fetchTask);
        setField(fetcher, "currentIncrementalSplit", incrementalSplit);
        setField(fetcher, "taskStarted", true);
        setField(fetcher, "executing", false);

        // First poll: the last queued batch must be returned, not null.
        Iterator<SourceRecords> first = fetcher.pollSplitRecords();
        Assertions.assertNotNull(first);
        Assertions.assertTrue(first.hasNext());
        SourceRecords records = first.next();
        Assertions.assertEquals(2, records.getSourceRecordList().size());

        // Second poll: the queue is drained, so the bounded split completion is
        // signaled by returning null.
        Assertions.assertNull(fetcher.pollSplitRecords());
    }

    @Test
    public void testNeverStopSentinelIsNotSignaledAsBoundedCompletion() throws Exception {
        IncrementalSourceStreamFetcher fetcher = createFetcher();

        // A real never-stop sentinel offset (e.g. LsnOffset.NO_STOPPING_OFFSET or
        // RedoLogOffset.NO_STOPPING_OFFSET) reports isNeverStop() == true. The fetch task
        // has finished without an error, but the split must NOT be marked completed: an
        // unbounded connector must never be turned into a bounded one by the shared reader.
        Offset stopOffset = mock(Offset.class);
        when(stopOffset.isNeverStop()).thenReturn(true);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-0",
                        Collections.emptyList(),
                        null,
                        stopOffset,
                        Collections.emptyList());

        FetchTask<SourceSplitBase> fetchTask = mock(FetchTask.class);
        when(fetchTask.isRunning()).thenReturn(false);
        when(fetchTask.getSplit()).thenReturn(incrementalSplit);

        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        when(queue.poll()).thenReturn(Collections.emptyList());

        setField(fetcher, "queue", queue);
        setField(fetcher, "streamFetchTask", fetchTask);
        setField(fetcher, "currentIncrementalSplit", incrementalSplit);
        setField(fetcher, "taskStarted", true);
        setField(fetcher, "executing", false);

        // Even though the task is finished, an unbounded sentinel must not cause a
        // bounded-completion signal (null); an empty iterator is returned instead.
        Iterator<SourceRecords> result = fetcher.pollSplitRecords();
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.hasNext());
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = IncrementalSourceStreamFetcher.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
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

    static IncrementalSourceStreamFetcher createPlainFetcher() {
        return new IncrementalSourceStreamFetcher(null, 0, null);
    }

    @Test
    public void testShouldEmitAtOrAfterWatermark() throws Exception {
        IncrementalSourceStreamFetcher fetcher = createPlainFetcher();

        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        when(taskContext.isDataChangeRecord(any())).thenReturn(true);
        when(taskContext.isExactlyOnce()).thenReturn(false);

        Offset splitStartWatermark = mock(Offset.class);
        Offset position = mock(Offset.class);
        when(position.isAtOrAfter(splitStartWatermark)).thenReturn(true);
        when(taskContext.getStreamOffset(any())).thenReturn(position);

        setField(fetcher, "taskContext", taskContext);
        setField(fetcher, "splitStartWatermark", splitStartWatermark);

        SourceRecord record = createDataEventWithSource();
        Assertions.assertTrue(fetcher.shouldEmit(record));
    }

    @Test
    public void testShouldEmitBeforeWatermarkFiltered() throws Exception {
        IncrementalSourceStreamFetcher fetcher = createPlainFetcher();

        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        when(taskContext.isDataChangeRecord(any())).thenReturn(true);
        when(taskContext.isExactlyOnce()).thenReturn(false);

        Offset splitStartWatermark = mock(Offset.class);
        Offset position = mock(Offset.class);
        when(position.isAtOrAfter(splitStartWatermark)).thenReturn(false);
        when(taskContext.getStreamOffset(any())).thenReturn(position);

        setField(fetcher, "taskContext", taskContext);
        setField(fetcher, "splitStartWatermark", splitStartWatermark);

        SourceRecord record = createDataEventWithSource();
        Assertions.assertFalse(fetcher.shouldEmit(record));
    }

    @Test
    public void testShouldEmitRestoredTableChangesWhileAddedTableIsSnapshotting() {
        // Restored after a savepoint that captured old_table, with new_table added to the
        // configuration: the re-created split only carries snapshot info for new_table.
        TableId oldTable = new TableId("testdb", "public", "old_table");
        TableId newTable = new TableId("testdb", "public", "new_table");
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(oldTable, newTable),
                        new TestOffset(100),
                        null,
                        Collections.singletonList(completedSnapshotSplitInfo(newTable, 140, 150)));

        IncrementalSourceStreamFetcher fetcher = submitExactlyOnceTask(split);
        try {
            Assertions.assertTrue(
                    fetcher.shouldEmit(createDataEventWithSource(oldTable, 120)),
                    "change of the restored table between its checkpoint offset and the added table's high watermark was dropped");
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(oldTable, 160)));
            Assertions.assertFalse(fetcher.shouldEmit(createDataEventWithSource(newTable, 120)));
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(newTable, 160)));
        } finally {
            fetcher.close();
        }
    }

    @Test
    public void testShouldEmitRegroupedRestoredTablesFromTheirOwnOffsets() {
        // With incremental.parallelism > 1, table1 and table2 were read by different splits and
        // checkpointed at 100 and 200. After the restore they share one split that starts at 100.
        TableId table1 = new TableId("testdb", "public", "table1");
        TableId table2 = new TableId("testdb", "public", "table2");
        TableId newTable = new TableId("testdb", "public", "new_table");
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Arrays.asList(table1, table2, newTable),
                        new TestOffset(100),
                        null,
                        Collections.singletonList(completedSnapshotSplitInfo(newTable, 140, 150)),
                        null,
                        Collections.emptyMap(),
                        Collections.singletonMap(table2, new TestOffset(200)));

        IncrementalSourceStreamFetcher fetcher = submitTask(exactlyOnceTaskContext(), split);
        try {
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(table1, 120)));
            Assertions.assertFalse(
                    fetcher.shouldEmit(createDataEventWithSource(table2, 150)),
                    "change of table2 already emitted before the checkpoint was emitted again");
            Assertions.assertFalse(fetcher.shouldEmit(createDataEventWithSource(table2, 199)));
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(table2, 200)));
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(table2, 250)));
            Assertions.assertFalse(fetcher.shouldEmit(createDataEventWithSource(newTable, 120)));
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(newTable, 160)));
        } finally {
            fetcher.close();
        }
    }

    @Test
    public void testShouldNotEmitSnapshotRangeChangesBeforeRestoredTableOffset() {
        // The table was checkpointed at 200 before its split entered the pure incremental phase,
        // and is regrouped into a split that starts earlier.
        TableId table = new TableId("testdb", "public", "table1");
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(table),
                        new TestOffset(100),
                        null,
                        Collections.singletonList(completedSnapshotSplitInfo(table, 140, 150)),
                        null,
                        Collections.emptyMap(),
                        Collections.singletonMap(table, new TestOffset(200)));
        FetchTask.Context taskContext = exactlyOnceTaskContext();
        when(taskContext.isRecordBetween(any(), any(), any())).thenReturn(true);

        IncrementalSourceStreamFetcher fetcher = submitTask(taskContext, split);
        try {
            Assertions.assertFalse(fetcher.shouldEmit(createDataEventWithSource(table, 170)));
            Assertions.assertTrue(fetcher.shouldEmit(createDataEventWithSource(table, 200)));
        } finally {
            fetcher.close();
        }
    }

    private static IncrementalSourceStreamFetcher submitExactlyOnceTask(IncrementalSplit split) {
        return submitTask(exactlyOnceTaskContext(), split);
    }

    private static FetchTask.Context exactlyOnceTaskContext() {
        FetchTask.Context taskContext = mock(FetchTask.Context.class);
        when(taskContext.isExactlyOnce()).thenReturn(true);
        when(taskContext.isDataChangeRecord(any())).thenReturn(true);
        when(taskContext.getStreamOffset(any()))
                .thenAnswer(
                        invocation -> {
                            SourceRecord record = invocation.getArgument(0);
                            return new TestOffset(
                                    Long.parseLong((String) record.sourceOffset().get("pos")));
                        });
        return taskContext;
    }

    private static IncrementalSourceStreamFetcher submitTask(
            FetchTask.Context taskContext, IncrementalSplit split) {
        FetchTask<SourceSplitBase> fetchTask = mock(FetchTask.class);
        when(fetchTask.getSplit()).thenReturn(split);

        IncrementalSourceStreamFetcher fetcher =
                new IncrementalSourceStreamFetcher(taskContext, 0, null);
        fetcher.submitTask(fetchTask);
        return fetcher;
    }

    private static CompletedSnapshotSplitInfo completedSnapshotSplitInfo(
            TableId tableId, long lowWatermark, long highWatermark) {
        String splitId = tableId + ":0";
        return new CompletedSnapshotSplitInfo(
                splitId,
                tableId,
                null,
                null,
                null,
                new SnapshotSplitWatermark(
                        splitId, new TestOffset(lowWatermark), new TestOffset(highWatermark)));
    }

    static SourceRecord createDataEventWithSource(TableId tableId, long position) {
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.postgresql.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("schema", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .field(DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                        .build();
        Struct sourceStruct = new Struct(sourceSchema);
        sourceStruct.put("db", tableId.catalog());
        sourceStruct.put("schema", tableId.schema());
        sourceStruct.put("table", tableId.table());
        sourceStruct.put(DEBEZIUM_CONNECTOR_KEY, "postgresql");

        Schema valueSchema =
                SchemaBuilder.struct()
                        .field(Envelope.FieldName.SOURCE, sourceSchema)
                        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                        .build();
        Struct value = new Struct(valueSchema);
        value.put(Envelope.FieldName.SOURCE, sourceStruct);
        value.put(Envelope.FieldName.OPERATION, "u");
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.singletonMap("pos", String.valueOf(position)),
                tableId.toString(),
                null,
                null,
                valueSchema,
                value);
    }

    static class TestOffset extends Offset {
        private static final long serialVersionUID = 1L;

        private final long position;

        TestOffset(long position) {
            this.position = position;
            this.offset = Collections.singletonMap("pos", String.valueOf(position));
        }

        @Override
        public int compareTo(Offset o) {
            return Long.compare(position, ((TestOffset) o).position);
        }
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
