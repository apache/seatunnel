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

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Supplies in-memory binlog events to the package-private stream filter and record emitter. */
public final class IncrementalSourceRecordEmitterFixture implements AutoCloseable {

    public enum Operation {
        INSERT,
        UPDATE
    }

    public static final int BATCH_EVENTS = 256;

    private static final String DATABASE = "inventory";
    private static final String TABLE = "customers";
    private static final String TABLE_ID = DATABASE + "." + TABLE;
    private static final String BINLOG_FILE = "mysql-bin.000001";
    private static final Map<String, String> SOURCE_PARTITION = Map.of("server", "mysql");
    private static final long FIRST_POSITION = 4096L;
    private static final long FETCH_DELAY_MS = 20L;
    private static final long TIMESTAMP_REFRESH_MS = 500L;

    private final int columns;
    private final Operation operation;
    private final boolean exactlyOnce;
    private final int rowsPerEvent;
    private final List<SourceRecord> records = new ArrayList<>(BATCH_EVENTS);
    private final RowCollector output;

    private IncrementalSourceStreamFetcher fetcher;
    private IncrementalSourceRecordEmitter<SeaTunnelRow> emitter;
    private IncrementalSplitState splitState;
    private Offset expectedLastOffset;
    private long refreshTimestampsAt;

    public IncrementalSourceRecordEmitterFixture(
            int columns, Operation operation, boolean exactlyOnce) {
        if (columns < 2) {
            throw new IllegalArgumentException("Expected an ID and at least one data column");
        }
        this.columns = columns;
        this.operation = Objects.requireNonNull(operation, "operation");
        this.exactlyOnce = exactlyOnce;
        this.rowsPerEvent = operation == Operation.UPDATE ? 2 : 1;
        this.output = new RowCollector(BATCH_EVENTS * rowsPerEvent);
    }

    public void setUp() throws Exception {
        TableSchema.Builder tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, false, null, ""));
        SchemaBuilder rowSchema =
                SchemaBuilder.struct()
                        .name(TABLE_ID + ".Value")
                        .optional()
                        .field("id", Schema.INT64_SCHEMA);
        for (int column = 1; column < columns; column++) {
            String name = "field_" + column;
            boolean numeric = column % 2 == 0;
            tableSchema.column(
                    PhysicalColumn.of(
                            name,
                            numeric ? BasicType.LONG_TYPE : BasicType.STRING_TYPE,
                            numeric ? 20L : 64L,
                            true,
                            null,
                            ""));
            rowSchema.field(
                    name, numeric ? Schema.OPTIONAL_INT64_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
        }
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("mysql", DATABASE, TABLE),
                        tableSchema.build(),
                        Map.of(),
                        List.of(),
                        "");
        List<CatalogTable> tables = List.of(table);

        SourceContext sourceContext = new SourceContext();
        emitter =
                new IncrementalSourceRecordEmitter<>(
                        SeaTunnelRowDebeziumDeserializeSchema.builder()
                                .setTables(tables)
                                .setServerTimeZone(ZoneOffset.UTC)
                                .build(),
                        new InMemoryOffsetFactory(),
                        sourceContext);

        TableId tableId = new TableId(DATABASE, null, TABLE);
        SourceRecord precedingRecord = prepareRecords(rowSchema.build());
        IncrementalSplit split =
                new IncrementalSplit(
                        "benchmark-binlog",
                        List.of(tableId),
                        MySqlUtils.getBinlogPosition(records.get(0)),
                        BinlogOffset.NO_STOPPING_OFFSET,
                        List.of());
        splitState = new IncrementalSplitState(split);
        expectedLastOffset = MySqlUtils.getBinlogPosition(records.get(BATCH_EVENTS - 1));
        fetcher =
                new IncrementalSourceStreamFetcher(new InMemoryFetchContext(exactlyOnce), 0, null);
        try {
            fetcher.submitTask(new PreparedFetchTask(split));
            if (fetcher.shouldEmit(precedingRecord) || !fetcher.shouldEmit(records.get(0))) {
                throw new IllegalStateException(
                        "Unexpected filtering at the starting binlog offset");
            }
            filterAndEmit();
            verifyBatch();
            if (sourceContext.getMetricsContext().counter("CDCRecordFetchDelay").getCount()
                    != FETCH_DELAY_MS) {
                throw new IllegalStateException("Unexpected CDC fetch delay");
            }
        } catch (Exception | Error failure) {
            close();
            throw failure;
        }
    }

    private SourceRecord prepareRecords(Schema rowSchema) {
        Schema keySchema =
                SchemaBuilder.struct()
                        .name(TABLE_ID + ".Key")
                        .field("id", Schema.INT64_SCHEMA)
                        .build();
        Schema sourceSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.Source")
                        .field("db", Schema.STRING_SCHEMA)
                        .field("table", Schema.STRING_SCHEMA)
                        .field("ts_ms", Schema.INT64_SCHEMA)
                        .field("file", Schema.STRING_SCHEMA)
                        .field("pos", Schema.INT64_SCHEMA)
                        .field("row", Schema.INT32_SCHEMA)
                        .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        Envelope envelope =
                Envelope.defineSchema()
                        .withName(TABLE_ID + ".Envelope")
                        .withRecord(rowSchema)
                        .withSource(sourceSchema)
                        .build();
        Instant now = Instant.now();
        SourceRecord precedingRecord =
                createRecord(envelope, rowSchema, keySchema, sourceSchema, now, -1);
        for (int index = 0; index < BATCH_EVENTS; index++) {
            records.add(createRecord(envelope, rowSchema, keySchema, sourceSchema, now, index));
        }
        return precedingRecord;
    }

    private SourceRecord createRecord(
            Envelope envelope,
            Schema rowSchema,
            Schema keySchema,
            Schema sourceSchema,
            Instant now,
            long index) {
        long position = FIRST_POSITION + index;
        Instant sourceTime = now.minusMillis(FETCH_DELAY_MS);
        Struct source =
                new Struct(sourceSchema)
                        .put("db", DATABASE)
                        .put("table", TABLE)
                        .put("ts_ms", sourceTime.toEpochMilli())
                        .put("file", BINLOG_FILE)
                        .put("pos", position)
                        .put("row", 0);
        Map<String, Object> offset = new HashMap<>();
        offset.put(BinlogOffset.SERVER_ID_KEY, 1L);
        offset.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, BINLOG_FILE);
        offset.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, position);
        offset.put(BinlogOffset.TIMESTAMP_KEY, sourceTime.getEpochSecond());
        Struct after = createRow(rowSchema, index, "after");
        Struct value =
                operation == Operation.UPDATE
                        ? envelope.update(createRow(rowSchema, index, "before"), after, source, now)
                        : envelope.create(after, source, now);
        return new SourceRecord(
                SOURCE_PARTITION,
                offset,
                "mysql." + TABLE_ID,
                null,
                keySchema,
                new Struct(keySchema).put("id", index),
                envelope.schema(),
                value);
    }

    private Struct createRow(Schema rowSchema, long id, String value) {
        Struct row = new Struct(rowSchema).put("id", id);
        for (int column = 1; column < columns; column++) {
            row.put(rowSchema.fields().get(column), column % 2 == 0 ? id + column : value);
        }
        return row;
    }

    /** Keeps lag metrics active while the prepared records are replayed across iterations. */
    private void refreshTimestampsIfNeeded() {
        long now = System.currentTimeMillis();
        if (now < refreshTimestampsAt) {
            return;
        }
        Long fetchedAt = now;
        Long createdAt = now - FETCH_DELAY_MS;
        for (SourceRecord record : records) {
            Struct value = (Struct) record.value();
            value.put(Envelope.FieldName.TIMESTAMP, fetchedAt);
            value.getStruct(Envelope.FieldName.SOURCE).put(Envelope.FieldName.TIMESTAMP, createdAt);
        }
        refreshTimestampsAt = now + TIMESTAMP_REFRESH_MS;
    }

    public int filterAndEmit() throws Exception {
        refreshTimestampsIfNeeded();
        List<SourceRecord> accepted = new ArrayList<>(BATCH_EVENTS);
        for (SourceRecord record : records) {
            if (fetcher.shouldEmit(record)) {
                accepted.add(record);
            }
        }
        output.size = 0;
        emitter.emitRecord(new SourceRecords(accepted), output, splitState);
        return output.size;
    }

    public void verifyBatch() {
        if (output.size != BATCH_EVENTS * rowsPerEvent) {
            throw new IllegalStateException("Unexpected number of CDC rows: " + output.size);
        }
        for (int index = 0; index < BATCH_EVENTS; index++) {
            int firstRow = index * rowsPerEvent;
            if (operation == Operation.UPDATE) {
                verifyRow(index, output.rows[firstRow], RowKind.UPDATE_BEFORE, "before");
                verifyRow(index, output.rows[firstRow + 1], RowKind.UPDATE_AFTER, "after");
            } else {
                verifyRow(index, output.rows[firstRow], RowKind.INSERT, "after");
            }
        }
        Offset checkpointOffset = splitState.toSourceSplit().getStartupOffset();
        if (!expectedLastOffset.equals(checkpointOffset)) {
            throw new IllegalStateException("Unexpected checkpoint offset: " + checkpointOffset);
        }
    }

    private void verifyRow(int index, SeaTunnelRow row, RowKind kind, String value) {
        int lastColumn = columns - 1;
        Object expectedLastField = lastColumn % 2 == 0 ? (long) index + lastColumn : value;
        if (row.getArity() != columns
                || row.getRowKind() != kind
                || !TABLE_ID.equals(row.getTableId())
                || (Long) row.getField(0) != index
                || !value.equals(row.getField(1))
                || !expectedLastField.equals(row.getField(lastColumn))
                || (Long) row.getOptions().get(CommonOptions.BINLOG_POS.getName())
                        != FIRST_POSITION + index) {
            throw new IllegalStateException("Unexpected CDC row for event " + index);
        }
    }

    @Override
    public void close() {
        if (fetcher != null) {
            fetcher.close();
            fetcher = null;
        }
    }

    /** Supplies the production filter with MySQL offsets and an in-memory split. */
    private static final class InMemoryFetchContext implements FetchTask.Context {

        private final boolean exactlyOnce;
        private final ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .maxQueueSize(BATCH_EVENTS)
                        .maxBatchSize(BATCH_EVENTS)
                        .pollInterval(Duration.ofMillis(1))
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("mysql", "benchmark", "filter"))
                        .build();

        private InMemoryFetchContext(boolean exactlyOnce) {
            this.exactlyOnce = exactlyOnce;
        }

        @Override
        public void configure(SourceSplitBase split) {}

        @Override
        public ChangeEventQueue<DataChangeEvent> getQueue() {
            return queue;
        }

        @Override
        public Offset getStreamOffset(SourceRecord record) {
            return MySqlUtils.getBinlogPosition(record);
        }

        @Override
        public boolean isExactlyOnce() {
            return exactlyOnce;
        }

        @Override
        public boolean isDataChangeRecord(SourceRecord record) {
            return SourceRecordUtils.isDataChangeRecord(record);
        }

        @Override
        public TableId getTableId(SourceRecord record) {
            return SourceRecordUtils.getTableId(record);
        }

        @Override
        public Tables.TableFilter getTableFilter() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isRecordBetween(SourceRecord record, Object[] start, Object[] end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void rewriteOutputBuffer(Map<Struct, SourceRecord> buffer, SourceRecord record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> records) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }

    /** Provides the same binlog offset representation used by the MySQL emitter. */
    private static final class InMemoryOffsetFactory extends OffsetFactory {

        @Override
        public Offset specific(Map<String, String> offset) {
            return new BinlogOffset(offset);
        }

        @Override
        public Offset specific(String filename, Long position) {
            return new BinlogOffset(filename, position);
        }

        @Override
        public Offset neverStop() {
            return BinlogOffset.NO_STOPPING_OFFSET;
        }

        @Override
        public Offset timestamp(long timestamp) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset earliest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Offset latest() {
            throw new UnsupportedOperationException();
        }
    }

    /** Lets the fetcher initialize its filter through the regular split submission lifecycle. */
    private static final class PreparedFetchTask implements FetchTask<SourceSplitBase> {

        private final IncrementalSplit split;

        private PreparedFetchTask(IncrementalSplit split) {
            this.split = split;
        }

        @Override
        public void execute(Context context) {}

        @Override
        public boolean isRunning() {
            return false;
        }

        @Override
        public void shutdown() {}

        @Override
        public SourceSplitBase getSplit() {
            return split;
        }
    }

    private static final class RowCollector implements Collector<SeaTunnelRow> {

        private final SeaTunnelRow[] rows;
        private int size;

        private RowCollector(int rowCount) {
            this.rows = new SeaTunnelRow[rowCount];
        }

        @Override
        public void collect(SeaTunnelRow row) {
            rows[size++] = row;
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }

    private static final class SourceContext implements SourceReader.Context {

        private final MetricsContext metrics = new AbstractMetricsContext() {};

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.UNBOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {
            throw new IllegalStateException("Expected steady incremental phase");
        }

        @Override
        public MetricsContext getMetricsContext() {
            return metrics;
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }
}
