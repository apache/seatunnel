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

package org.apache.seatunnel.connectors.cdc.debezium.row;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.MultipleRowType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.MetadataConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeAfterWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.isSchemaChangeBeforeWatermarkEvent;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** Deserialization schema from Debezium object to {@link SeaTunnelRow}. */
@Slf4j
public final class SeaTunnelRowDebeziumDeserializeSchema
        implements DebeziumDeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_TABLE_NAME_KEY = null;

    private final MetadataConverter[] metadataConverters;
    private final ZoneId serverTimeZone;
    private final DebeziumDeserializationConverterFactory userDefinedConverterFactory;
    private final SchemaChangeResolver schemaChangeResolver;
    private final DataTypeChangeEventHandler dataTypeChangeEventHandler;
    private SeaTunnelDataType<SeaTunnelRow> resultTypeInfo;
    private Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters;

    SeaTunnelRowDebeziumDeserializeSchema(
            SeaTunnelDataType<SeaTunnelRow> physicalDataType,
            MetadataConverter[] metadataConverters,
            SeaTunnelDataType<SeaTunnelRow> resultType,
            ZoneId serverTimeZone,
            DebeziumDeserializationConverterFactory userDefinedConverterFactory,
            SchemaChangeResolver schemaChangeResolver) {
        this.metadataConverters = metadataConverters;
        this.serverTimeZone = serverTimeZone;
        this.userDefinedConverterFactory = userDefinedConverterFactory;
        this.resultTypeInfo = checkNotNull(resultType);
        this.schemaChangeResolver = schemaChangeResolver;
        this.dataTypeChangeEventHandler = new DataTypeChangeEventDispatcher();
        this.tableRowConverters =
                createTableRowConverters(
                        resultType,
                        metadataConverters,
                        serverTimeZone,
                        userDefinedConverterFactory);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<SeaTunnelRow> collector)
            throws Exception {
        if (isSchemaChangeBeforeWatermarkEvent(record)) {
            collector.markSchemaChangeBeforeCheckpoint();
            return;
        }
        if (isSchemaChangeAfterWatermarkEvent(record)) {
            collector.markSchemaChangeAfterCheckpoint();
            return;
        }
        if (isSchemaChangeEvent(record)) {
            deserializeSchemaChangeRecord(record, collector);
            return;
        }

        if (isDataChangeRecord(record)) {
            deserializeDataChangeRecord(record, collector);
            return;
        }

        log.debug("Unsupported record {}, just skip.", record);
    }

    private void deserializeSchemaChangeRecord(
            SourceRecord record, Collector<SeaTunnelRow> collector) {
        SchemaChangeEvent schemaChangeEvent = schemaChangeResolver.resolve(record, resultTypeInfo);
        if (schemaChangeEvent == null) {
            log.warn("Unsupported resolve schemaChangeEvent {}, just skip.", record);
            return;
        }
        if (resultTypeInfo instanceof MultipleRowType) {
            Map<String, SeaTunnelRowType> newRowTypeMap = new HashMap<>();
            for (Map.Entry<String, SeaTunnelRowType> entry : (MultipleRowType) resultTypeInfo) {
                if (!entry.getKey().equals(schemaChangeEvent.tablePath().toString())) {
                    newRowTypeMap.put(entry.getKey(), entry.getValue());
                    continue;
                }

                log.debug("Table[{}] datatype change before: {}", entry.getKey(), entry.getValue());
                SeaTunnelRowType newRowType =
                        dataTypeChangeEventHandler.reset(entry.getValue()).apply(schemaChangeEvent);
                newRowTypeMap.put(entry.getKey(), newRowType);
                log.debug("Table[{}] datatype change after: {}", entry.getKey(), newRowType);
            }
            resultTypeInfo = new MultipleRowType(newRowTypeMap);
        } else {
            log.debug("Table datatype change before: {}", resultTypeInfo);
            resultTypeInfo =
                    dataTypeChangeEventHandler
                            .reset((SeaTunnelRowType) resultTypeInfo)
                            .apply(schemaChangeEvent);
            log.debug("table datatype change after: {}", resultTypeInfo);
        }

        tableRowConverters =
                createTableRowConverters(
                        resultTypeInfo,
                        metadataConverters,
                        serverTimeZone,
                        userDefinedConverterFactory);

        collector.collect(schemaChangeEvent);
    }

    private void deserializeDataChangeRecord(SourceRecord record, Collector<SeaTunnelRow> collector)
            throws Exception {
        Envelope.Operation operation = Envelope.operationFor(record);
        Struct messageStruct = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        String tableId = tablePath.toString();
        SeaTunnelRowDebeziumDeserializationConverters converters;
        if (resultTypeInfo instanceof MultipleRowType) {
            converters = tableRowConverters.get(tableId);
            if (converters == null) {
                log.debug("Ignore newly added table {}", tableId);
                return;
            }
        } else {
            converters = tableRowConverters.get(DEFAULT_TABLE_NAME_KEY);
        }
        Long fetchTimestamp = SourceRecordUtils.getFetchTimestamp(record);
        Long messageTimestamp = SourceRecordUtils.getMessageTimestamp(record);
        long delay = -1L;
        if (fetchTimestamp != null && messageTimestamp != null) {
            delay = fetchTimestamp - messageTimestamp;
        }
        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            SeaTunnelRow insert = extractAfterRow(converters, record, messageStruct, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            insert.setTableId(tableId);
            MetadataUtil.setDelay(insert, delay);
            MetadataUtil.setEventTime(insert, fetchTimestamp);
            collector.collect(insert);
        } else if (operation == Envelope.Operation.DELETE) {
            SeaTunnelRow delete = extractBeforeRow(converters, record, messageStruct, valueSchema);
            delete.setRowKind(RowKind.DELETE);
            delete.setTableId(tableId);
            MetadataUtil.setDelay(delete, delay);
            MetadataUtil.setEventTime(delete, fetchTimestamp);
            collector.collect(delete);
        } else if (operation == Envelope.Operation.UPDATE) {
            SeaTunnelRow before = extractBeforeRow(converters, record, messageStruct, valueSchema);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            before.setTableId(tableId);
            MetadataUtil.setDelay(before, delay);
            MetadataUtil.setEventTime(before, fetchTimestamp);
            collector.collect(before);

            SeaTunnelRow after = extractAfterRow(converters, record, messageStruct, valueSchema);
            after.setRowKind(RowKind.UPDATE_AFTER);
            after.setTableId(tableId);
            MetadataUtil.setDelay(after, delay);
            MetadataUtil.setEventTime(after, fetchTimestamp);
            collector.collect(after);
        } else {
            log.warn("Received {} operation, skip", operation);
        }
    }

    private SeaTunnelRow extractAfterRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return runtimeConverter.convert(record, after, afterSchema);
    }

    private SeaTunnelRow extractBeforeRow(
            SeaTunnelRowDebeziumDeserializationConverters runtimeConverter,
            SourceRecord record,
            Struct value,
            Schema valueSchema)
            throws Exception {

        Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct before = value.getStruct(Envelope.FieldName.BEFORE);
        return runtimeConverter.convert(record, before, beforeSchema);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return resultTypeInfo;
    }

    @Override
    public SchemaChangeResolver getSchemaChangeResolver() {
        return schemaChangeResolver;
    }

    @Override
    public void restoreCheckpointProducedType(SeaTunnelDataType<SeaTunnelRow> checkpointDataType) {
        // If checkpointDataType is null, it indicates that DDL changes are not supported.
        // Therefore, we need to use the latest table structure to ensure that data from newly added
        // columns can be parsed correctly.
        if (schemaChangeResolver == null) {
            return;
        }
        if (SqlType.ROW.equals(checkpointDataType.getSqlType())
                && SqlType.MULTIPLE_ROW.equals(resultTypeInfo.getSqlType())) {
            // TODO: Older versions may have this issue
            log.warn(
                    "Skip incompatible restore type. produced type: {}, checkpoint type: {}",
                    resultTypeInfo,
                    checkpointDataType);
            return;
        }
        if (checkpointDataType instanceof MultipleRowType) {
            MultipleRowType latestDataType = (MultipleRowType) resultTypeInfo;
            Map<String, SeaTunnelRowType> newRowTypeMap = new HashMap<>();
            for (Map.Entry<String, SeaTunnelRowType> entry : latestDataType) {
                newRowTypeMap.put(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String, SeaTunnelRowType> entry : (MultipleRowType) checkpointDataType) {
                SeaTunnelRowType oldDataType = latestDataType.getRowType(entry.getKey());
                if (oldDataType == null) {
                    log.info("Ignore restore table[{}] datatype has been deleted.", entry.getKey());
                    continue;
                }

                log.info("Table[{}] datatype restore before: {}", entry.getKey(), oldDataType);
                newRowTypeMap.put(entry.getKey(), entry.getValue());
                log.info("Table[{}] datatype restore after: {}", entry.getKey(), entry.getValue());
            }
            resultTypeInfo = new MultipleRowType(newRowTypeMap);
        } else {
            log.info("Table datatype restore before: {}", resultTypeInfo);
            resultTypeInfo = checkpointDataType;
            log.info("Table datatype restore after: {}", checkpointDataType);
        }
        tableRowConverters =
                createTableRowConverters(
                        resultTypeInfo,
                        metadataConverters,
                        serverTimeZone,
                        userDefinedConverterFactory);
    }

    private static Map<String, SeaTunnelRowDebeziumDeserializationConverters>
            createTableRowConverters(
                    SeaTunnelDataType<SeaTunnelRow> inputDataType,
                    MetadataConverter[] metadataConverters,
                    ZoneId serverTimeZone,
                    DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters =
                new HashMap<>();
        if (inputDataType instanceof MultipleRowType) {
            for (Map.Entry<String, SeaTunnelRowType> item : (MultipleRowType) inputDataType) {
                SeaTunnelRowDebeziumDeserializationConverters itemRowConverter =
                        new SeaTunnelRowDebeziumDeserializationConverters(
                                item.getValue(),
                                metadataConverters,
                                serverTimeZone,
                                userDefinedConverterFactory);
                tableRowConverters.put(item.getKey(), itemRowConverter);
            }
            return tableRowConverters;
        }

        SeaTunnelRowDebeziumDeserializationConverters tableRowConverter =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        (SeaTunnelRowType) inputDataType,
                        metadataConverters,
                        serverTimeZone,
                        userDefinedConverterFactory);
        tableRowConverters.put(DEFAULT_TABLE_NAME_KEY, tableRowConverter);
        return tableRowConverters;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private SeaTunnelDataType<SeaTunnelRow> physicalRowType;
        private SeaTunnelDataType<SeaTunnelRow> resultTypeInfo;
        private MetadataConverter[] metadataConverters = new MetadataConverter[0];
        private ZoneId serverTimeZone = ZoneId.systemDefault();
        private DebeziumDeserializationConverterFactory userDefinedConverterFactory =
                DebeziumDeserializationConverterFactory.DEFAULT;
        private SchemaChangeResolver schemaChangeResolver;

        public SeaTunnelRowDebeziumDeserializeSchema build() {
            return new SeaTunnelRowDebeziumDeserializeSchema(
                    physicalRowType,
                    metadataConverters,
                    resultTypeInfo,
                    serverTimeZone,
                    userDefinedConverterFactory,
                    schemaChangeResolver);
        }
    }
}
