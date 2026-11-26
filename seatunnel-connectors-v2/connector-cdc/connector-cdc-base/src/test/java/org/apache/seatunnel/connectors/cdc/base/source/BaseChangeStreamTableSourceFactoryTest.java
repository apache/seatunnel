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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class BaseChangeStreamTableSourceFactoryTest {

    private final TestFactory factory = new TestFactory();

    @Test
    void testGetRestoreTableStructWithMultipleIncrementalSplitsPrefersSplitOwnerAndDeduplicates() {
        TableId t1 = new TableId("mysql_cdc", null, "t1");
        TableId t2 = new TableId("mysql_cdc", null, "t2");
        TableId t3 = new TableId("mysql_cdc", null, "t3");

        CatalogTable t1V1 = catalogTable(TablePath.of("mysql_cdc", "t1"), "v1");
        CatalogTable t1V2 = catalogTable(TablePath.of("mysql_cdc", "t1"), "v2");
        CatalogTable t2V1 = catalogTable(TablePath.of("mysql_cdc", "t2"), "v1");
        CatalogTable t2V2 = catalogTable(TablePath.of("mysql_cdc", "t2"), "v2");
        CatalogTable t3V1 = catalogTable(TablePath.of("mysql_cdc", "t3"), "v1");

        // Split 0 owns t1 and has evolved t1 (v2), while carrying stale t2 (v1).
        IncrementalSplit split0 =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(t1),
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList(t1V2, t2V1, t3V1),
                        Collections.emptyMap());
        // Split 1 owns t2 and has evolved t2 (v2), while carrying stale t1 (v1).
        IncrementalSplit split1 =
                new IncrementalSplit(
                        "incremental-split-1",
                        Collections.singletonList(t2),
                        null,
                        null,
                        Collections.emptyList(),
                        Arrays.asList(t1V1, t2V2, t3V1),
                        Collections.emptyMap());

        ChangeStreamTableSourceState<Serializable, SourceSplitBase> state =
                new ChangeStreamTableSourceState<>(
                        null,
                        Arrays.asList(
                                Collections.singletonList(split0),
                                Collections.singletonList(split1)));

        List<CatalogTable> restored = factory.getRestoreTableStruct(state);
        Assertions.assertEquals(3, restored.size());
        Map<TablePath, CatalogTable> byPath =
                restored.stream()
                        .collect(Collectors.toMap(CatalogTable::getTablePath, Function.identity()));
        Assertions.assertEquals("v2", byPath.get(TablePath.of("mysql_cdc", "t1")).getComment());
        Assertions.assertEquals("v2", byPath.get(TablePath.of("mysql_cdc", "t2")).getComment());
        Assertions.assertEquals("v1", byPath.get(TablePath.of("mysql_cdc", "t3")).getComment());

        // Merging with DB tables must succeed without duplicate key exceptions.
        List<CatalogTable> merged =
                factory.mergeTableStruct(Arrays.asList(t1V1, t2V1, t3V1), restored);
        Assertions.assertEquals(3, merged.size());
        Assertions.assertEquals("v2", merged.get(0).getComment());
        Assertions.assertEquals("v2", merged.get(1).getComment());
        Assertions.assertEquals("v1", merged.get(2).getComment());
    }

    @Test
    void testGetRestoreTableStructWithSingleSplitPreservesLegacyCheckpointDataType() {
        SeaTunnelRowType legacyRowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.INT_TYPE});
        @SuppressWarnings("deprecation")
        IncrementalSplit legacySplit =
                new IncrementalSplit(
                        "incremental-split-0",
                        Collections.singletonList(new TableId("mysql_cdc", null, "t1")),
                        null,
                        null,
                        Collections.emptyList(),
                        legacyRowType);
        ChangeStreamTableSourceState<Serializable, SourceSplitBase> state =
                new ChangeStreamTableSourceState<>(
                        null, Collections.singletonList(Collections.singletonList(legacySplit)));

        List<CatalogTable> restored = factory.getRestoreTableStruct(state);
        Assertions.assertEquals(1, restored.size());
        Assertions.assertEquals(
                TablePath.of("default", null, "default.default"), restored.get(0).getTablePath());
        Assertions.assertEquals(legacyRowType, restored.get(0).getSeaTunnelRowType());
    }

    @Test
    void testGetRestoreTableStructReturnsEmptyWhenNoIncrementalSplits() {
        ChangeStreamTableSourceState<Serializable, SourceSplitBase> emptyState =
                new ChangeStreamTableSourceState<>(null, Collections.emptyList());
        Assertions.assertTrue(factory.getRestoreTableStruct(emptyState).isEmpty());
        Assertions.assertTrue(factory.getRestoreTableStruct(null).isEmpty());
    }

    private static CatalogTable catalogTable(TablePath tablePath, String comment) {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.INT_TYPE, (Long) null, false, null, ""))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("MySQL", tablePath),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                comment);
    }

    private static class TestFactory extends BaseChangeStreamTableSourceFactory {

        @Override
        public String factoryIdentifier() {
            return "test-cdc";
        }

        @Override
        public OptionRule optionRule() {
            return OptionRule.builder().build();
        }

        @Override
        public Class<? extends SeaTunnelSource> getSourceClass() {
            return SeaTunnelSource.class;
        }

        @Override
        public <T, SplitT extends SourceSplit, StateT extends Serializable>
                TableSource<T, SplitT, StateT> restoreSource(
                        TableSourceFactoryContext context, List<CatalogTable> restoreTableStruct) {
            throw new UnsupportedOperationException();
        }
    }
}
