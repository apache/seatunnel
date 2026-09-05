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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.operation.event.TableOperationEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;
import io.debezium.relational.history.HistoryRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SeaTunnelRowDebeziumDeserializeSchemaTableOperationTest {

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct().name("io.debezium.connector.mysql.SchemaChangeKey").build();
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .field(AbstractSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                    .build();
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                    .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.OPTIONAL_STRING_SCHEMA)
                    .field(
                            HistoryRecord.Fields.TABLE_CHANGES,
                            SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                    .build();

    @Test
    void skipsAlterWhenResolverDoesNotSupportIt() throws Exception {
        SchemaChangeResolver resolver =
                new SchemaChangeResolver() {
                    @Override
                    public boolean support(SourceRecord record) {
                        return false;
                    }

                    @Override
                    public SchemaChangeEvent resolve(
                            SourceRecord record, List<CatalogTable> catalogTables) {
                        throw new AssertionError("ALTER must not be resolved");
                    }
                };
        SeaTunnelRowDebeziumDeserializeSchema schema = schema(resolver);
        RecordingCollector collector = new RecordingCollector();

        schema.deserialize(ddlRecord("shop", "ALTER TABLE products ADD COLUMN c INT"), collector);

        Assertions.assertTrue(collector.events.isEmpty());
    }

    @Test
    void failsWhenTruncateResolveThrows() {
        SchemaChangeResolver resolver =
                new SchemaChangeResolver() {
                    @Override
                    public boolean support(SourceRecord record) {
                        return true;
                    }

                    @Override
                    public SchemaChangeEvent resolve(
                            SourceRecord record, List<CatalogTable> catalogTables) {
                        return null;
                    }

                    @Override
                    public TableOperationEvent resolveTableOperation(
                            SourceRecord record, List<CatalogTable> catalogTables) {
                        throw new IllegalStateException("parse failed");
                    }
                };
        SeaTunnelRowDebeziumDeserializeSchema schema = schema(resolver);

        SeaTunnelException thrown =
                Assertions.assertThrows(
                        SeaTunnelException.class,
                        () ->
                                schema.deserialize(
                                        ddlRecord("shop", "TRUNCATE TABLE products"),
                                        new RecordingCollector()));
        Assertions.assertTrue(thrown.getMessage().contains("TRUNCATE"));
        Assertions.assertInstanceOf(IllegalStateException.class, thrown.getCause());
    }

    private static SeaTunnelRowDebeziumDeserializeSchema schema(SchemaChangeResolver resolver) {
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("mysql", "shop", "products"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 11L, false, null, ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);
        return SeaTunnelRowDebeziumDeserializeSchema.builder()
                .setTables(Collections.singletonList(table))
                .setSchemaChangeResolver(resolver)
                .build();
    }

    private static SourceRecord ddlRecord(String database, String ddl) {
        Struct value = new Struct(VALUE_SCHEMA);
        Struct source = new Struct(SOURCE_SCHEMA);
        source.put(AbstractSourceInfo.DATABASE_NAME_KEY, database);
        value.put(Envelope.FieldName.SOURCE, source);
        value.put(HistoryRecord.Fields.DDL_STATEMENTS, ddl);
        value.put(HistoryRecord.Fields.TABLE_CHANGES, Collections.emptyList());
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "topic",
                null,
                KEY_SCHEMA,
                new Struct(KEY_SCHEMA),
                VALUE_SCHEMA,
                value);
    }

    private static final class RecordingCollector implements Collector<SeaTunnelRow> {
        private final List<Object> events = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            events.add(record);
        }

        @Override
        public void collect(SchemaChangeEvent event) {
            events.add(event);
        }

        @Override
        public void collect(TableOperationEvent event) {
            events.add(event);
        }

        @Override
        public Object getCheckpointLock() {
            return this;
        }
    }
}
