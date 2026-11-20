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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import com.aliyun.odps.tunnel.TableTunnel;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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

    @Test
    public void testComputeSplitsWithEmptyOrMissingPartitionTable() {
        int numReaders = 3;
        TablePath tablePath1 = TablePath.of("test_db", "test_schema", "test_table_normal");
        TablePath tablePath2 =
                TablePath.of("test_db", "test_schema", "test_table_missing_partition");

        Map<TablePath, Long> tableRecordCounts = new HashMap<>();
        tableRecordCounts.put(tablePath1, 20000L);
        tableRecordCounts.put(tablePath2, 0L);

        CatalogTable catalogTable1 = mock(CatalogTable.class);
        when(catalogTable1.getTablePath()).thenReturn(tablePath1);
        CatalogTable catalogTable2 = mock(CatalogTable.class);
        when(catalogTable2.getTablePath()).thenReturn(tablePath2);

        SourceTableInfo tableInfo1 = new SourceTableInfo(catalogTable1, null, 10000);
        SourceTableInfo tableInfo2 =
                new SourceTableInfo(catalogTable2, "ds='2025-01-08'", 10000, true);

        List<SourceTableInfo> tableInfos = new ArrayList<>();
        tableInfos.add(tableInfo1);
        tableInfos.add(tableInfo2);

        Set<MaxcomputeSourceSplit> splits =
                MaxcomputeSourceSplitEnumerator.computeSplits(
                        numReaders, tableInfos, tableRecordCounts);

        // table1 has 2 splits, table2 has 0 splits -> total 2 splits
        Assertions.assertEquals(2, splits.size());
        for (MaxcomputeSourceSplit split : splits) {
            Assertions.assertEquals(tablePath1, split.getTablePath());
        }
    }

    @Test
    public void testEnumeratorDiscoveryBehavior() throws Exception {
        // Table 1: Opted-in absent partition -> session suppressed
        TablePath tablePath1 = TablePath.of("test_project", "test_schema", "t_absent");
        CatalogTable catalogTable1 = mock(CatalogTable.class);
        when(catalogTable1.getTablePath()).thenReturn(tablePath1);
        SourceTableInfo tableInfo1 =
                new SourceTableInfo(catalogTable1, "ds='20250101'", 10000, true);

        // Table 2: Second readable table with explicit non-default schema and present partition ->
        // session created
        TablePath tablePath2 = TablePath.of("test_project", "finance_schema", "t_readable");
        CatalogTable catalogTable2 = mock(CatalogTable.class);
        when(catalogTable2.getTablePath()).thenReturn(tablePath2);
        SourceTableInfo tableInfo2 =
                new SourceTableInfo(catalogTable2, "ds='20250102'", 10000, true);

        // Table 3: Default false -> hasPartition NOT checked, attempts session directly
        TablePath tablePath3 = TablePath.of("test_project", "test_schema", "t_default_false");
        CatalogTable catalogTable3 = mock(CatalogTable.class);
        when(catalogTable3.getTablePath()).thenReturn(tablePath3);
        SourceTableInfo tableInfo3 =
                new SourceTableInfo(catalogTable3, "ds='20250103'", 10000, false);

        Map<TablePath, SourceTableInfo> tableInfos = new HashMap<>();
        tableInfos.put(tablePath1, tableInfo1);
        tableInfos.put(tablePath2, tableInfo2);
        tableInfos.put(tablePath3, tableInfo3);

        SourceSplitEnumerator.Context<MaxcomputeSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(new HashMap<>());
        MaxcomputeSourceSplitEnumerator enumerator =
                new MaxcomputeSourceSplitEnumerator(context, readonlyConfig, tableInfos);

        TableTunnel.DownloadSession session2 = mock(TableTunnel.DownloadSession.class);
        when(session2.getRecordCount()).thenReturn(10000L);
        TableTunnel.DownloadSession session3 = mock(TableTunnel.DownloadSession.class);
        when(session3.getRecordCount()).thenReturn(5000L);

        try (MockedStatic<MaxcomputeUtil> mockedUtil = mockStatic(MaxcomputeUtil.class)) {
            // Table 1 partition missing
            mockedUtil
                    .when(
                            () ->
                                    MaxcomputeUtil.hasPartition(
                                            any(ReadonlyConfig.class),
                                            eq(tablePath1),
                                            eq("ds='20250101'")))
                    .thenReturn(false);
            // Table 2 partition present
            mockedUtil
                    .when(
                            () ->
                                    MaxcomputeUtil.hasPartition(
                                            any(ReadonlyConfig.class),
                                            eq(tablePath2),
                                            eq("ds='20250102'")))
                    .thenReturn(true);
            mockedUtil
                    .when(
                            () ->
                                    MaxcomputeUtil.getDownloadSession(
                                            any(), eq(tablePath2), eq("ds='20250102'")))
                    .thenReturn(session2);
            // Table 3 session
            mockedUtil
                    .when(
                            () ->
                                    MaxcomputeUtil.getDownloadSession(
                                            any(), eq(tablePath3), eq("ds='20250103'")))
                    .thenReturn(session3);

            enumerator.run();

            // Table 1: hasPartition called, download session suppressed (never called)
            mockedUtil.verify(
                    () ->
                            MaxcomputeUtil.hasPartition(
                                    any(ReadonlyConfig.class), eq(tablePath1), eq("ds='20250101'")),
                    times(1));
            mockedUtil.verify(
                    () -> MaxcomputeUtil.getDownloadSession(any(), eq(tablePath1), any()), never());

            // Table 2: hasPartition called with non-default schema preserved, session created
            ArgumentCaptor<TablePath> pathCaptor = ArgumentCaptor.forClass(TablePath.class);
            mockedUtil.verify(
                    () ->
                            MaxcomputeUtil.hasPartition(
                                    any(ReadonlyConfig.class),
                                    pathCaptor.capture(),
                                    eq("ds='20250102'")),
                    times(1));
            Assertions.assertEquals("finance_schema", pathCaptor.getValue().getSchemaName());
            mockedUtil.verify(
                    () ->
                            MaxcomputeUtil.getDownloadSession(
                                    any(), eq(tablePath2), eq("ds='20250102'")),
                    times(1));

            // Table 3: default false never checks hasPartition, proceeds to session directly
            mockedUtil.verify(
                    () ->
                            MaxcomputeUtil.hasPartition(
                                    any(ReadonlyConfig.class), eq(tablePath3), any()),
                    never());
            mockedUtil.verify(
                    () ->
                            MaxcomputeUtil.getDownloadSession(
                                    any(), eq(tablePath3), eq("ds='20250103'")),
                    times(1));

            // Reader assignment: splits from table 2 and 3 assigned in a single batch, table 1
            // skipped
            ArgumentCaptor<List<MaxcomputeSourceSplit>> splitCaptor =
                    ArgumentCaptor.forClass(List.class);
            verify(context, times(1)).assignSplit(eq(0), splitCaptor.capture());
            List<MaxcomputeSourceSplit> assigned = splitCaptor.getValue();
            Assertions.assertEquals(2, assigned.size());
            Set<TablePath> assignedTables = new HashSet<>();
            for (MaxcomputeSourceSplit split : assigned) {
                assignedTables.add(split.getTablePath());
            }
            Assertions.assertTrue(assignedTables.contains(tablePath2));
            Assertions.assertTrue(assignedTables.contains(tablePath3));
            Assertions.assertFalse(assignedTables.contains(tablePath1));
            verify(context).signalNoMoreSplits(0);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOptionInheritanceAndPerTableOverrideInSource() throws Exception {
        Map<String, Object> globalConfig = new HashMap<>();
        globalConfig.put("accessId", "ak");
        globalConfig.put("accesskey", "sk");
        globalConfig.put("endpoint", "http://service.odps.aliyun.com/api");
        globalConfig.put("project", "test_project");
        globalConfig.put("ignore_missing_partition", false);

        Map<String, Object> schemaMap =
                Collections.singletonMap("fields", Collections.singletonMap("id", "int"));

        Map<String, Object> table1 = new HashMap<>();
        table1.put("table_name", "table1");
        table1.put("partition_spec", "ds='20250101'");
        table1.put("ignore_missing_partition", true); // per-table true override
        table1.put("schema", schemaMap);

        Map<String, Object> table2 = new HashMap<>();
        table2.put("table_name", "table2");
        table2.put("partition_spec", "ds='20250102'");
        table2.put("ignore_missing_partition", false); // explicit false
        table2.put("schema", schemaMap);

        Map<String, Object> table3 = new HashMap<>();
        table3.put("table_name", "table3");
        table3.put("partition_spec", "ds='20250103'"); // inherits global false
        table3.put("schema", schemaMap);

        Map<String, Object> table4 = new HashMap<>();
        table4.put("table_name", "table4");
        table4.put("schema_name", "custom_schema"); // explicit non-default schema
        table4.put("partition_spec", "ds='20250104'");
        table4.put("ignore_missing_partition", true);
        table4.put("schema", schemaMap);

        globalConfig.put("table_list", Arrays.asList(table1, table2, table3, table4));
        MaxcomputeSource source = new MaxcomputeSource(ReadonlyConfig.fromMap(globalConfig));

        // Use reflection to verify internal sourceTableInfos without adding test-only getters to
        // production code
        Field field = MaxcomputeSource.class.getDeclaredField("sourceTableInfos");
        field.setAccessible(true);
        Map<TablePath, SourceTableInfo> sourceTableInfos =
                (Map<TablePath, SourceTableInfo>) field.get(source);

        TablePath path1 = TablePath.of("test_project", null, "table1");
        TablePath path2 = TablePath.of("test_project", null, "table2");
        TablePath path3 = TablePath.of("test_project", null, "table3");
        TablePath path4 = TablePath.of("test_project", "custom_schema", "table4");

        Assertions.assertTrue(
                sourceTableInfos.get(path1).isIgnoreMissingPartition(),
                "Table 1 should have overridden ignore_missing_partition to true");
        Assertions.assertFalse(
                sourceTableInfos.get(path2).isIgnoreMissingPartition(),
                "Table 2 should have explicit ignore_missing_partition as false");
        Assertions.assertFalse(
                sourceTableInfos.get(path3).isIgnoreMissingPartition(),
                "Table 3 should inherit global ignore_missing_partition as false");
        Assertions.assertTrue(
                sourceTableInfos.get(path4).isIgnoreMissingPartition(),
                "Table 4 should have explicit ignore_missing_partition as true");
        Assertions.assertEquals(
                "custom_schema",
                sourceTableInfos.get(path4).getCatalogTable().getTablePath().getSchemaName());

        // Reverse test: Global true, table_override false
        Map<String, Object> globalConfigTrue = new HashMap<>();
        globalConfigTrue.put("accessId", "ak");
        globalConfigTrue.put("accesskey", "sk");
        globalConfigTrue.put("endpoint", "http://service.odps.aliyun.com/api");
        globalConfigTrue.put("project", "test_project");
        globalConfigTrue.put("ignore_missing_partition", true); // global true

        Map<String, Object> tableA = new HashMap<>();
        tableA.put("table_name", "tableA");
        tableA.put("partition_spec", "ds='20250101'");
        tableA.put("schema", schemaMap); // inherits global true

        Map<String, Object> tableB = new HashMap<>();
        tableB.put("table_name", "tableB");
        tableB.put("partition_spec", "ds='20250102'");
        tableB.put("ignore_missing_partition", false); // overrides to false
        tableB.put("schema", schemaMap);

        globalConfigTrue.put("table_list", Arrays.asList(tableA, tableB));
        MaxcomputeSource sourceTrue =
                new MaxcomputeSource(ReadonlyConfig.fromMap(globalConfigTrue));
        Map<TablePath, SourceTableInfo> infosTrue =
                (Map<TablePath, SourceTableInfo>) field.get(sourceTrue);

        Assertions.assertTrue(
                infosTrue
                        .get(TablePath.of("test_project", null, "tableA"))
                        .isIgnoreMissingPartition(),
                "Table A should inherit global true");
        Assertions.assertFalse(
                infosTrue
                        .get(TablePath.of("test_project", null, "tableB"))
                        .isIgnoreMissingPartition(),
                "Table B should override global true to false");
    }

    @Test
    public void testReaderCompletionWhenNoSplitsAssigned() throws Exception {
        SourceReader.Context readerContext = mock(SourceReader.Context.class);
        when(readerContext.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        MaxcomputeSourceReader reader =
                new MaxcomputeSourceReader(
                        ReadonlyConfig.fromMap(new HashMap<>()),
                        readerContext,
                        Collections.emptyMap());

        // When 0 splits are assigned (e.g. partition missing and ignored)
        reader.handleNoMoreSplits();
        Collector<org.apache.seatunnel.api.table.type.SeaTunnelRow> collector =
                mock(Collector.class);
        reader.pollNext(collector);

        // Reader must immediately signal no more elements
        verify(readerContext, times(1)).signalNoMoreElement();
        verify(collector, never())
                .collect(any(org.apache.seatunnel.api.table.type.SeaTunnelRow.class));
    }
}
