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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The context for fetch task that fetching data of snapshot split from JDBC data source. */
public abstract class JdbcSourceFetchTaskContext implements FetchTask.Context {

    protected final JdbcSourceConfig sourceConfig;
    protected final JdbcDataSourceDialect dataSourceDialect;
    protected final CommonConnectorConfig dbzConnectorConfig;
    protected final SchemaNameAdjuster schemaNameAdjuster;
    protected final ConnectTableChangeSerializer tableChangeSerializer =
            new ConnectTableChangeSerializer();
    protected final JsonConverter jsonConverter;

    public JdbcSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
        this.sourceConfig = sourceConfig;
        this.dataSourceDialect = dataSourceDialect;
        this.dbzConnectorConfig = sourceConfig.getDbzConnectorConfig();
        this.schemaNameAdjuster = SchemaNameAdjuster.create();
        this.jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap("schemas.enable", true), false);
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return SourceRecordUtils.getTableId(record);
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return SourceRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        SeaTunnelRowType splitKeyType =
                getSplitType(getDatabaseSchema().tableFor(getTableId(record)));
        Object[] key = SourceRecordUtils.getSplitKey(splitKeyType, record, getSchemaNameAdjuster());
        return SourceRecordUtils.splitKeyRangeContains(key, splitStart, splitEnd);
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {
        Struct key = (Struct) changeRecord.key();
        Struct value = (Struct) changeRecord.value();
        if (value != null) {
            Envelope.Operation operation =
                    Envelope.Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
            switch (operation) {
                case CREATE:
                case UPDATE:
                    Envelope envelope = Envelope.fromSchema(changeRecord.valueSchema());
                    Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                    Struct after = value.getStruct(Envelope.FieldName.AFTER);
                    Instant fetchTs =
                            Instant.ofEpochMilli((Long) source.get(Envelope.FieldName.TIMESTAMP));
                    SourceRecord record =
                            new SourceRecord(
                                    changeRecord.sourcePartition(),
                                    changeRecord.sourceOffset(),
                                    changeRecord.topic(),
                                    changeRecord.kafkaPartition(),
                                    changeRecord.keySchema(),
                                    changeRecord.key(),
                                    changeRecord.valueSchema(),
                                    envelope.read(after, source, fetchTs));
                    outputBuffer.put(key, record);
                    break;
                case DELETE:
                    outputBuffer.remove(key);
                    break;
                case READ:
                    throw new IllegalStateException(
                            String.format(
                                    "Data change record shouldn't use READ operation, the the record is %s.",
                                    changeRecord));
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .map(
                        record -> {
                            Envelope envelope = Envelope.fromSchema(record.valueSchema());
                            Struct value = (Struct) record.value();
                            Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                            source.put(Envelope.FieldName.TIMESTAMP, 0L);
                            // extend the fetch timestamp(ts_ms)
                            Instant fetchTs =
                                    Instant.ofEpochMilli(
                                            value.getInt64(Envelope.FieldName.TIMESTAMP));
                            SourceRecord sourceRecord =
                                    new SourceRecord(
                                            record.sourcePartition(),
                                            record.sourceOffset(),
                                            record.topic(),
                                            record.kafkaPartition(),
                                            record.keySchema(),
                                            record.key(),
                                            record.valueSchema(),
                                            envelope.read(updateAfter, source, fetchTs));
                            return sourceRecord;
                        })
                .collect(Collectors.toList());
    }

    protected void registerDatabaseHistory(
            SourceSplitBase sourceSplitBase, JdbcConnection connection) {
        List<TableChanges.TableChange> engineHistory = new ArrayList<>();
        // TODO: support save table schema
        if (sourceSplitBase instanceof SnapshotSplit) {
            SnapshotSplit snapshotSplit = (SnapshotSplit) sourceSplitBase;
            engineHistory.add(
                    dataSourceDialect.queryTableSchema(connection, snapshotSplit.getTableId()));
        } else {
            IncrementalSplit incrementalSplit = (IncrementalSplit) sourceSplitBase;
            Map<TableId, byte[]> historyTableChanges = incrementalSplit.getHistoryTableChanges();
            for (TableId tableId : incrementalSplit.getTableIds()) {
                if (historyTableChanges != null && historyTableChanges.containsKey(tableId)) {
                    SchemaAndValue schemaAndValue =
                            jsonConverter.toConnectData("topic", historyTableChanges.get(tableId));
                    Struct deserializedStruct = (Struct) schemaAndValue.value();

                    TableChanges tableChanges =
                            tableChangeSerializer.deserialize(
                                    Collections.singletonList(deserializedStruct), false);

                    Iterator<TableChanges.TableChange> iterator = tableChanges.iterator();
                    TableChanges.TableChange tableChange = null;
                    while (iterator.hasNext()) {
                        if (tableChange != null) {
                            throw new IllegalStateException(
                                    "The table changes should only have one element");
                        }
                        tableChange = iterator.next();
                    }
                    engineHistory.add(tableChange);
                    continue;
                }
                engineHistory.add(dataSourceDialect.queryTableSchema(connection, tableId));
            }
        }

        EmbeddedDatabaseHistory.registerHistory(
                sourceConfig
                        .getDbzConfiguration()
                        .getString(EmbeddedDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME),
                engineHistory);
    }

    public SourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Override
    public boolean isExactlyOnce() {
        return sourceConfig.isExactlyOnce();
    }

    public JdbcDataSourceDialect getDataSourceDialect() {
        return dataSourceDialect;
    }

    public CommonConnectorConfig getDbzConnectorConfig() {
        return dbzConnectorConfig;
    }

    public SchemaNameAdjuster getSchemaNameAdjuster() {
        return schemaNameAdjuster;
    }

    public abstract RelationalDatabaseSchema getDatabaseSchema();

    public abstract SeaTunnelRowType getSplitType(Table table);

    public abstract ErrorHandler getErrorHandler();

    public abstract JdbcSourceEventDispatcher getDispatcher();

    public abstract OffsetContext getOffsetContext();

    public abstract Partition getPartition();
}
