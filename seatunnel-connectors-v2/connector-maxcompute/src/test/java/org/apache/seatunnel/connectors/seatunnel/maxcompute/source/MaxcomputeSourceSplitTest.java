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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MaxcomputeSourceSplitTest {

    @Test
    public void testSplitIdUniquenessAndIndexDistribution() {
        int numReaders = 5;
        long recordCount = 105000;
        int splitRow = 10000;

        TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table");

        Map<TablePath, Long> tableRecordCounts = new HashMap<>();
        tableRecordCounts.put(tablePath, recordCount);

        CatalogTable catalogTable = mock(CatalogTable.class);
        when(catalogTable.getTablePath()).thenReturn(tablePath);

        SourceTableInfo tableInfo = new SourceTableInfo(catalogTable, null, splitRow);
        List<SourceTableInfo> tableInfos = new ArrayList<>();
        tableInfos.add(tableInfo);

        Set<MaxcomputeSourceSplit> splits =
                MaxcomputeSourceSplitEnumerator.computeSplits(
                        numReaders, tableInfos, tableRecordCounts);

        Set<String> splitIds = new HashSet<>();
        int[] indexCounts = new int[numReaders];

        for (MaxcomputeSourceSplit split : splits) {
            Assertions.assertTrue(
                    splitIds.add(split.splitId()), "Duplicate splitId found: " + split.splitId());
            indexCounts[split.getIndex()]++;
        }

        Assertions.assertEquals(11, splits.size());

        // Verify index distribution (round-robin):
        // 11 splits / 5 readers = 2 splits each, plus 1 remainder for reader 0.
        Assertions.assertEquals(3, indexCounts[0], "Reader 0 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[1], "Reader 1 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[2], "Reader 2 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[3], "Reader 3 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[4], "Reader 4 split count mismatch!");
    }

    @Test
    public void testMultiTableRoundRobinDistribution() {
        int numReaders = 3;
        int numTables = 5;
        long recordCountPerTable = 15000;
        int splitRow = 10000;

        Map<TablePath, Long> tableRecordCounts = new HashMap<>();
        List<SourceTableInfo> tableInfos = new ArrayList<>();

        for (int t = 0; t < numTables; t++) {
            TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table_" + t);
            tableRecordCounts.put(tablePath, recordCountPerTable);

            CatalogTable catalogTable = mock(CatalogTable.class);
            when(catalogTable.getTablePath()).thenReturn(tablePath);

            SourceTableInfo tableInfo = new SourceTableInfo(catalogTable, null, splitRow);
            tableInfos.add(tableInfo);
        }

        Set<MaxcomputeSourceSplit> splits =
                MaxcomputeSourceSplitEnumerator.computeSplits(
                        numReaders, tableInfos, tableRecordCounts);

        int[] indexCounts = new int[numReaders];

        for (MaxcomputeSourceSplit split : splits) {
            indexCounts[split.getIndex()]++;
        }

        Assertions.assertEquals(10, splits.size());

        // 10 splits / 3 readers = 3 splits each, plus 1 remainder for reader 0.
        Assertions.assertEquals(4, indexCounts[0], "Reader 0 split count mismatch!");
        Assertions.assertEquals(3, indexCounts[1], "Reader 1 split count mismatch!");
        Assertions.assertEquals(3, indexCounts[2], "Reader 2 split count mismatch!");
    }

    @Test
    public void testComputeSplitsAscendingRowStartOrder() {
        int numReaders = 3;
        int numTables = 2;
        Map<TablePath, Long> tableRecordCounts = new HashMap<>();
        List<SourceTableInfo> tableInfos = new ArrayList<>();

        for (int i = 0; i < numTables; i++) {
            TablePath path = TablePath.of("db", "schema", "table" + i);
            // 105,000 rows / 10,000 splitRow = 11 splits per table.
            // With 3 readers, each reader gets 3-4 splits PER TABLE.
            tableRecordCounts.put(path, 105000L);

            CatalogTable catalogTable = mock(CatalogTable.class);
            when(catalogTable.getTablePath()).thenReturn(path);

            SourceTableInfo tableInfo = new SourceTableInfo(catalogTable, null, 10000);
            tableInfos.add(tableInfo);
        }

        // Drive the actual production logic
        Set<MaxcomputeSourceSplit> splits =
                MaxcomputeSourceSplitEnumerator.computeSplits(
                        numReaders, tableInfos, tableRecordCounts);

        // Guard against HashSet regression: splits iterated from the Set MUST be in insertion
        // order,
        // which means for any given reader and table, the rowStart must be strictly ascending.
        Map<Integer, Map<TablePath, Long>> lastRowStarts = new HashMap<>();

        for (MaxcomputeSourceSplit split : splits) {
            int reader = split.getIndex();
            TablePath path = split.getTablePath();
            long rowStart = split.getRowStart();

            lastRowStarts.computeIfAbsent(reader, r -> new HashMap<>());
            Long lastStart = lastRowStarts.get(reader).get(path);

            if (lastStart != null) {
                Assertions.assertTrue(
                        rowStart > lastStart,
                        "HashSet Regression! Splits for reader "
                                + reader
                                + " and table "
                                + path
                                + " are not in ascending order!");
            }
            lastRowStarts.get(reader).put(path, rowStart);
        }

        Assertions.assertEquals(22, splits.size(), "Should have exactly 22 splits total");
    }
}
