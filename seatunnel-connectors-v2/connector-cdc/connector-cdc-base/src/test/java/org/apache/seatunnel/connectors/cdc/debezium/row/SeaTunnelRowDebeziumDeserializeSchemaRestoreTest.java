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
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;

import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

class SeaTunnelRowDebeziumDeserializeSchemaRestoreTest {

    @Test
    void emitsRestoredSchemaBeforeFirstRecordOnly() throws Exception {
        CatalogTable initialTable = table(false);
        CatalogTable restoredTable = table(true);
        SeaTunnelRowDebeziumDeserializeSchema schema = schema(initialTable);
        @SuppressWarnings("unchecked")
        Collector<SeaTunnelRow> collector = Mockito.mock(Collector.class);

        schema.restoreCheckpointProducedType(Collections.singletonList(restoredTable));
        schema.deserialize(unsupportedRecord(), collector);
        schema.deserialize(unsupportedRecord(), collector);

        Mockito.verify(collector, Mockito.times(1))
                .collect(Mockito.any(RestoreTableSchemaEvent.class));
    }

    @Test
    void doesNotEmitRestoreEventWhenSchemaDidNotChange() throws Exception {
        CatalogTable initialTable = table(false);
        SeaTunnelRowDebeziumDeserializeSchema schema = schema(initialTable);
        @SuppressWarnings("unchecked")
        Collector<SeaTunnelRow> collector = Mockito.mock(Collector.class);

        schema.restoreCheckpointProducedType(Collections.singletonList(initialTable));
        schema.deserialize(unsupportedRecord(), collector);

        Mockito.verify(collector, Mockito.never())
                .collect(Mockito.any(RestoreTableSchemaEvent.class));
    }

    private static SeaTunnelRowDebeziumDeserializeSchema schema(CatalogTable table) {
        return SeaTunnelRowDebeziumDeserializeSchema.builder()
                .setTables(Collections.singletonList(table))
                .setSchemaChangeResolver(Mockito.mock(SchemaChangeResolver.class))
                .build();
    }

    private static SourceRecord unsupportedRecord() {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "unsupported",
                null,
                null,
                null,
                null);
    }

    private static CatalogTable table(boolean includeEmail) {
        TableSchema.Builder schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, false, null, ""))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, true, null, ""));
        if (includeEmail) {
            schema.column(PhysicalColumn.of("email", BasicType.STRING_TYPE, 128L, true, null, ""));
        }
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "customers"),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }
}
