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

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventHandler;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    private final TableSchemaChangeEventHandler tableSchemaChangeHandler;
    private List<CatalogTable> tables;
    private Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters;

    SeaTunnelRowDebeziumDeserializeSchema(
            MetadataConverter[] metadataConverters,
            List<CatalogTable> tables,
            ZoneId serverTimeZone,
            DebeziumDeserializationConverterFactory userDefinedConverterFactory,
            SchemaChangeResolver schemaChangeResolver) {
        this.metadataConverters = metadataConverters;
        this.serverTimeZone = serverTimeZone;
        this.userDefinedConverterFactory = userDefinedConverterFactory;
        this.tables = checkNotNull(tables);
        this.schemaChangeResolver = schemaChangeResolver;
        this.tableSchemaChangeHandler = new TableSchemaChangeEventDispatcher();
        this.tableRowConverters =
                createTableRowConverters(
                        tables, metadataConverters, serverTimeZone, userDefinedConverterFactory);
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
        SchemaChangeEvent schemaChangeEvent = schemaChangeResolver.resolve(record, null);
        if (schemaChangeEvent == null) {
            log.warn("Unsupported resolve schemaChangeEvent {}, just skip.", record);
            return;
        }
        boolean tableExist = false;
        for (int i = 0; i < tables.size(); i++) {
            CatalogTable changeBefore = tables.get(i);
            if (!schemaChangeEvent.tablePath().equals(changeBefore.getTablePath())) {
                continue;
            }

            tableExist = true;
            log.debug(
                    "Table[{}] change before: {}",
                    schemaChangeEvent.tablePath(),
                    changeBefore.getTableSchema());

            CatalogTable changeAfter = null;
            if (EventType.SCHEMA_CHANGE_UPDATE_COLUMNS.equals(schemaChangeEvent.getEventType())) {
                AlterTableColumnsEvent alterTableColumnsEvent =
                        (AlterTableColumnsEvent) schemaChangeEvent;
                for (AlterTableColumnEvent event : alterTableColumnsEvent.getEvents()) {
                    TableSchema changeAfterSchema =
                            tableSchemaChangeHandler
                                    .reset(changeBefore.getTableSchema())
                                    .apply(event);
                    changeAfter =
                            CatalogTable.of(
                                    changeBefore.getTableId(),
                                    changeAfterSchema,
                                    changeBefore.getOptions(),
                                    changeBefore.getPartitionKeys(),
                                    changeBefore.getComment());
                    event.setChangeAfter(changeAfter);

                    changeBefore = changeAfter;
                }
            } else {
                TableSchema changeAfterSchema =
                        tableSchemaChangeHandler
                                .reset(changeBefore.getTableSchema())
                                .apply(schemaChangeEvent);
                changeAfter =
                        CatalogTable.of(
                                changeBefore.getTableId(),
                                changeAfterSchema,
                                changeBefore.getOptions(),
                                changeBefore.getPartitionKeys(),
                                changeBefore.getComment());
            }
            tables.set(i, changeAfter);
            schemaChangeEvent.setChangeAfter(changeAfter);
            log.debug(
                    "Table[{}] change after: {}",
                    schemaChangeEvent.tablePath(),
                    changeAfter.getTableSchema());
            break;
        }
        if (!tableExist) {
            log.error(
                    "Not found table {}, skip schema change event {}",
                    schemaChangeEvent.tablePath());
        }
        tableRowConverters =
                createTableRowConverters(
                        tables, metadataConverters, serverTimeZone, userDefinedConverterFactory);
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
        if (tables.size() > 1) {
            converters = tableRowConverters.get(tableId);
            if (converters == null) {
                log.debug("Ignore newly added table {}", tableId);
                return;
            }
        } else {
            converters = tableRowConverters.get(DEFAULT_TABLE_NAME_KEY);
        }

        if (operation == Envelope.Operation.CREATE || operation == Envelope.Operation.READ) {
            SeaTunnelRow insert = extractAfterRow(converters, record, messageStruct, valueSchema);
            insert.setRowKind(RowKind.INSERT);
            insert.setTableId(tableId);
            collector.collect(insert);
        } else if (operation == Envelope.Operation.DELETE) {
            SeaTunnelRow delete = extractBeforeRow(converters, record, messageStruct, valueSchema);
            delete.setRowKind(RowKind.DELETE);
            delete.setTableId(tableId);
            collector.collect(delete);
        } else if (operation == Envelope.Operation.UPDATE) {
            SeaTunnelRow before = extractBeforeRow(converters, record, messageStruct, valueSchema);
            before.setRowKind(RowKind.UPDATE_BEFORE);
            before.setTableId(tableId);
            collector.collect(before);

            SeaTunnelRow after = extractAfterRow(converters, record, messageStruct, valueSchema);
            after.setRowKind(RowKind.UPDATE_AFTER);
            after.setTableId(tableId);
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
    public List<CatalogTable> getProducedType() {
        return tables;
    }

    @Override
    public SchemaChangeResolver getSchemaChangeResolver() {
        return schemaChangeResolver;
    }

    @Override
    public void restoreCheckpointProducedType(List<CatalogTable> checkpointDataType) {
        // If checkpointDataType is null, it indicates that DDL changes are not supported.
        // Therefore, we need to use the latest table structure to ensure that data from newly added
        // columns can be parsed correctly.
        if (schemaChangeResolver == null) {
            return;
        }

        Map<TablePath, CatalogTable> latestTableMap =
                this.tables.stream().collect(Collectors.toMap(CatalogTable::getTablePath, t -> t));
        Map<TablePath, CatalogTable> restoreTableMap =
                checkpointDataType.stream()
                        .collect(Collectors.toMap(CatalogTable::getTablePath, t -> t));
        for (TablePath tablePath : restoreTableMap.keySet()) {
            CatalogTable latestTable = latestTableMap.get(tablePath);
            CatalogTable restoreTable = restoreTableMap.get(tablePath);
            if (latestTable == null) {
                log.info("Ignore restore table[{}] has been deleted.", tablePath);
                continue;
            }

            log.info("Table[{}] restore before: {}", tablePath, latestTable.getSeaTunnelRowType());
            latestTableMap.put(tablePath, restoreTable);
            log.info("Table[{}] restore after: {}", tablePath, restoreTable.getSeaTunnelRowType());
        }
        this.tables = new ArrayList<>(latestTableMap.values());
        this.tableRowConverters =
                createTableRowConverters(
                        tables, metadataConverters, serverTimeZone, userDefinedConverterFactory);
    }

    private static Map<String, SeaTunnelRowDebeziumDeserializationConverters>
            createTableRowConverters(
                    List<CatalogTable> tables,
                    MetadataConverter[] metadataConverters,
                    ZoneId serverTimeZone,
                    DebeziumDeserializationConverterFactory userDefinedConverterFactory) {
        Map<String, SeaTunnelRowDebeziumDeserializationConverters> tableRowConverters =
                new HashMap<>();
        if (tables.size() > 1) {
            for (CatalogTable table : tables) {
                SeaTunnelRowDebeziumDeserializationConverters itemRowConverter =
                        new SeaTunnelRowDebeziumDeserializationConverters(
                                table.getSeaTunnelRowType(),
                                metadataConverters,
                                serverTimeZone,
                                userDefinedConverterFactory);
                tableRowConverters.put(table.getTablePath().toString(), itemRowConverter);
            }
            return tableRowConverters;
        }

        SeaTunnelRowDebeziumDeserializationConverters tableRowConverter =
                new SeaTunnelRowDebeziumDeserializationConverters(
                        tables.get(0).getSeaTunnelRowType(),
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
        private List<CatalogTable> tables;
        private MetadataConverter[] metadataConverters = new MetadataConverter[0];
        private ZoneId serverTimeZone = ZoneId.systemDefault();
        private DebeziumDeserializationConverterFactory userDefinedConverterFactory =
                DebeziumDeserializationConverterFactory.DEFAULT;
        private SchemaChangeResolver schemaChangeResolver;

        public SeaTunnelRowDebeziumDeserializeSchema build() {
            return new SeaTunnelRowDebeziumDeserializeSchema(
                    metadataConverters,
                    tables,
                    serverTimeZone,
                    userDefinedConverterFactory,
                    schemaChangeResolver);
        }
    }
}
