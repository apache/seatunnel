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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.clickhouse.client.ClickHouseNode;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@Slf4j
public class ClickhouseValueReaderTest {

    private ClickhouseProxy mockProxy;

    private ClickhouseValueReader reader;
    private ClickhouseSourceSplit split;
    private SeaTunnelRowType rowType;
    private ClickhouseSourceTable sourceTable;
    private static final int BATCH_SIZE = 10;

    @BeforeEach
    public void init() {
        String[] fieldNames = new String[] {"id", "name", "age"};
        SeaTunnelDataType<?>[] fieldTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                };
        rowType = new SeaTunnelRowType(fieldNames, fieldTypes);

        sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of("test_db", "test_table"))
                        .batchSize(BATCH_SIZE)
                        .build();

        ClickHouseNode node = ClickHouseNode.builder().host("localhost").port(8123).build();

        Shard shard = new Shard(1, 1, node);

        ClickhousePart part1 = new ClickhousePart("part1", "test_db", "test_table", shard);
        ClickhousePart part2 = new ClickhousePart("part2", "test_db", "test_table", shard);
        Set<ClickhousePart> parts = new TreeSet<>(Comparator.comparing(ClickhousePart::getName));
        parts.add(part1);
        parts.add(part2);

        split =
                new ClickhouseSourceSplit(
                        TablePath.of("test_db", "test_table"),
                        TablePath.of("test_db", "test_table"),
                        parts,
                        shard,
                        "split-1");

        mockProxy = Mockito.mock(ClickhouseProxy.class);

        reader = new ClickhouseValueReader(split, rowType, sourceTable);
        try {
            Field proxyField = ClickhouseValueReader.class.getDeclaredField("proxy");
            proxyField.setAccessible(true);
            proxyField.set(reader, mockProxy);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set mock proxy", e);
        }
    }

    @Test
    public void testHasNextWithFullBatch() {
        List<SeaTunnelRow> mockRows = createMockRows(BATCH_SIZE);

        when(mockProxy.getDataFromSplit(
                        any(ClickhousePart.class), eq(rowType), eq(sourceTable), anyInt()))
                .thenReturn(mockRows);

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result.size());
        Assertions.assertEquals(0, reader.currentPartIndex);

        // Make sure the offset has been updated but the part has not been marked as eos
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertEquals(BATCH_SIZE, parts.get(0).getOffset());
        Assertions.assertFalse(parts.get(0).isEos());
    }

    @Test
    public void testHasNextWithPartialBatch() {
        // Create mock data
        int partialSize = BATCH_SIZE - 2;
        List<SeaTunnelRow> mockRows = createMockRows(partialSize);

        when(mockProxy.getDataFromSplit(
                        any(ClickhousePart.class), eq(rowType), eq(sourceTable), anyInt()))
                .thenReturn(mockRows);

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(partialSize, result.size());

        // Make sure the offset has been updated and mark part as eos
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertEquals(partialSize, parts.get(0).getOffset());
        Assertions.assertTrue(parts.get(0).isEos());

        Assertions.assertEquals(1, reader.currentPartIndex);
    }

    @Test
    public void testHasNextWithEmptyBatch() {
        // create empty test data
        List<SeaTunnelRow> mockRows = new ArrayList<>();

        when(mockProxy.getDataFromSplit(
                        any(ClickhousePart.class), eq(rowType), eq(sourceTable), anyInt()))
                .thenReturn(mockRows);

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(0, result.size());

        // Make sure that part is marked as eos
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertTrue(parts.get(0).isEos());

        Assertions.assertEquals(2, reader.currentPartIndex);
    }

    @Test
    public void testHasNextWithMultipleParts() {
        List<SeaTunnelRow> mockRows1 = createMockRows(BATCH_SIZE);

        int partialSize = 5;
        List<SeaTunnelRow> mockRows2 = createMockRows(partialSize);

        // Return different data for different parts
        when(mockProxy.getDataFromSplit(
                        any(ClickhousePart.class), eq(rowType), eq(sourceTable), anyInt()))
                .thenAnswer(
                        invocation -> {
                            ClickhousePart part = invocation.getArgument(0);
                            if ("part1".equals(part.getName())) {
                                return part.getOffset() != BATCH_SIZE
                                        ? mockRows1
                                        : new ArrayList<>();
                            }
                            return mockRows2;
                        });

        // First part - Full Batch
        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result1 = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result1.size());
        Assertions.assertEquals(0, reader.currentPartIndex);

        // Second part - Some Batches
        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result2 = reader.next();
        Assertions.assertEquals(partialSize, result2.size());
        Assertions.assertEquals(2, reader.currentPartIndex);

        // All parts have been processed. hasNext should return false
        Assertions.assertFalse(reader.hasNext());
    }

    private List<SeaTunnelRow> createMockRows(int size) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            SeaTunnelRow row = new SeaTunnelRow(3);
            row.setField(0, (long) i);
            row.setField(1, "name" + i);
            row.setField(2, 20 + i);
            rows.add(row);
        }
        return rows;
    }
}
