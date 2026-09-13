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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSplitGeneratorState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Unit tests for lazy even-split cursor semantics without contacting a database. */
class DynamicChunkSplitterLazyTest {

    @Test
    void testLazyEvenSplitsMatchArithmeticBoundaries() throws Exception {
        DynamicChunkSplitter splitter = newSplitter();
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceTable table = createTable(tablePath);

        // Matches splitEvenlySizedChunks(min=1, max=10, dynamicChunkSize=3).
        splitter.restoreGeneratorState(
                JdbcSplitGeneratorState.builder()
                        .tablePath(tablePath)
                        .mode(JdbcSplitGeneratorState.Mode.EVEN)
                        .splitKeyName("id")
                        .splitKeyType(BasicType.INT_TYPE)
                        .minValue(1)
                        .maxValue(10)
                        .currentBoundary(null)
                        .chunkSize(100)
                        .dynamicChunkSize(3)
                        .nextSplitIndex(0)
                        .finished(false)
                        .emitFinalOpenEnded(true)
                        .remainingQueue(new ArrayList<>())
                        .build(),
                table);

        List<JdbcSourceSplit> splits = drain(splitter);
        Assertions.assertEquals(4, splits.size());
        assertRange(splits.get(0), null, 4);
        assertRange(splits.get(1), 4, 7);
        assertRange(splits.get(2), 7, 10);
        assertRange(splits.get(3), 10, null);
        Assertions.assertFalse(splitter.hasNext());
    }

    @Test
    void testRestoreEvenCursorContinuesFromBoundary() throws Exception {
        DynamicChunkSplitter splitter = newSplitter();
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceTable table = createTable(tablePath);

        splitter.restoreGeneratorState(
                JdbcSplitGeneratorState.builder()
                        .tablePath(tablePath)
                        .mode(JdbcSplitGeneratorState.Mode.EVEN)
                        .splitKeyName("id")
                        .splitKeyType(BasicType.INT_TYPE)
                        .minValue(1)
                        .maxValue(10)
                        .currentBoundary(7)
                        .chunkSize(100)
                        .dynamicChunkSize(3)
                        .nextSplitIndex(2)
                        .finished(false)
                        .emitFinalOpenEnded(true)
                        .remainingQueue(new ArrayList<>())
                        .build(),
                table);

        List<JdbcSourceSplit> splits = drain(splitter);
        Assertions.assertEquals(2, splits.size());
        assertRange(splits.get(0), 7, 10);
        assertRange(splits.get(1), 10, null);
    }

    @Test
    void testSnapshotAndRestorePreservesEvenCursor() throws Exception {
        DynamicChunkSplitter splitter = newSplitter();
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceTable table = createTable(tablePath);

        splitter.restoreGeneratorState(
                JdbcSplitGeneratorState.builder()
                        .tablePath(tablePath)
                        .mode(JdbcSplitGeneratorState.Mode.EVEN)
                        .splitKeyName("id")
                        .splitKeyType(BasicType.INT_TYPE)
                        .minValue(1)
                        .maxValue(10)
                        .currentBoundary(null)
                        .chunkSize(100)
                        .dynamicChunkSize(3)
                        .nextSplitIndex(0)
                        .finished(false)
                        .emitFinalOpenEnded(true)
                        .remainingQueue(new ArrayList<>())
                        .build(),
                table);

        Assertions.assertTrue(splitter.hasNext());
        JdbcSourceSplit first = splitter.nextSplit();
        assertRange(first, null, 4);

        JdbcSplitGeneratorState snapshot = splitter.snapshotGeneratorState();
        DynamicChunkSplitter restored = newSplitter();
        restored.restoreGeneratorState(snapshot, table);

        List<JdbcSourceSplit> remaining = drain(restored);
        Assertions.assertEquals(3, remaining.size());
        assertRange(remaining.get(0), 4, 7);
        assertRange(remaining.get(1), 7, 10);
        assertRange(remaining.get(2), 10, null);
    }

    private static DynamicChunkSplitter newSplitter() {
        return new DynamicChunkSplitter(
                JdbcSourceConfig.builder()
                        .jdbcConnectionConfig(
                                JdbcConnectionConfig.builder()
                                        .url("jdbc:generic://localhost:0/test")
                                        .driverName("org.example.Driver")
                                        .build())
                        .build());
    }

    private static JdbcSourceTable createTable(TablePath tablePath) {
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.builder()
                                        .name("id")
                                        .dataType(BasicType.INT_TYPE)
                                        .sourceType("int")
                                        .build())
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("default", tablePath),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        return JdbcSourceTable.builder().tablePath(tablePath).catalogTable(catalogTable).build();
    }

    private static List<JdbcSourceSplit> drain(ChunkSplitter splitter) throws Exception {
        List<JdbcSourceSplit> splits = new ArrayList<>();
        while (splitter.hasNext()) {
            splits.add(splitter.nextSplit());
        }
        return splits;
    }

    private static void assertRange(JdbcSourceSplit split, Object start, Object end) {
        Assertions.assertEquals(start, split.getSplitStart());
        Assertions.assertEquals(end, split.getSplitEnd());
    }
}
