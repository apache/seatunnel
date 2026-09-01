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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.List;

class IncrementalSourceReaderTest {

    @Test
    void shouldRestoreCheckpointTablesOnlyForSchemaAwareDeserializer() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        Mockito.when(schema.getSchemaChangeResolver())
                .thenReturn(Mockito.mock(SchemaChangeResolver.class));
        List<CatalogTable> checkpointTables =
                Collections.singletonList(Mockito.mock(CatalogTable.class));
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointTables,
                        Collections.emptyMap());

        List<CatalogTable> restored = IncrementalSourceReader.restoreCheckpointState(split, schema);

        Assertions.assertEquals(checkpointTables, restored);
        Mockito.verify(schema).restoreCheckpointProducedType(checkpointTables);
    }

    @Test
    void shouldIgnoreEmptyCheckpointTables() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        Mockito.when(schema.getSchemaChangeResolver())
                .thenReturn(Mockito.mock(SchemaChangeResolver.class));
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap());

        Assertions.assertTrue(
                IncrementalSourceReader.restoreCheckpointState(split, schema).isEmpty());
        Mockito.verify(schema, Mockito.never()).restoreCheckpointProducedType(Mockito.anyList());
    }

    @Test
    void shouldRestoreLegacyCheckpointDataType() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        Mockito.when(schema.getSchemaChangeResolver())
                .thenReturn(Mockito.mock(SchemaChangeResolver.class));
        SeaTunnelRowType checkpointRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(new TableId("catalog", "database", "customers")),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointRowType);

        IncrementalSourceReader.restoreCheckpointState(split, schema);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<CatalogTable>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(schema).restoreCheckpointProducedType(captor.capture());
        List<CatalogTable> restoredTables = captor.getValue();
        Assertions.assertEquals(1, restoredTables.size());
        Assertions.assertEquals(
                "catalog.database.customers", restoredTables.get(0).getTablePath().getFullName());
        Assertions.assertArrayEquals(
                checkpointRowType.getFieldNames(),
                restoredTables.get(0).getSeaTunnelRowType().getFieldNames());
    }

    @Test
    void shouldSkipRestoreForResolverlessDeserializer() {
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        List<CatalogTable> checkpointTables =
                Collections.singletonList(Mockito.mock(CatalogTable.class));
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.emptyList(),
                        Mockito.mock(Offset.class),
                        Mockito.mock(Offset.class),
                        Collections.emptyList(),
                        checkpointTables,
                        Collections.emptyMap());

        Assertions.assertTrue(
                IncrementalSourceReader.restoreCheckpointState(split, schema).isEmpty());
        Mockito.verify(schema, Mockito.never()).restoreCheckpointProducedType(Mockito.anyList());
    }
}
