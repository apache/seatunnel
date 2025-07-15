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
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.data.ClickHouseIntegerValue;
import com.clickhouse.client.data.ClickHouseLongValue;
import com.clickhouse.client.data.ClickHouseStringValue;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
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
    public void init() throws ClickHouseException {
        String[] fieldNames = new String[] {"id", "name", "age"};
        SeaTunnelDataType<?>[] fieldTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.LONG_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                };
        rowType = new SeaTunnelRowType(fieldNames, fieldTypes);

        ClickhouseTable mockClickhouseTable = Mockito.mock(ClickhouseTable.class);
        when(mockClickhouseTable.getSortingKey()).thenReturn("id");

        sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of("test_db", "test_table"))
                        .batchSize(BATCH_SIZE)
                        .clickhouseTable(mockClickhouseTable)
                        .build();

        ClickHouseNode node = ClickHouseNode.builder().host("localhost").port(8123).build();

        Shard shard = new Shard(1, 1, node);

        ClickhousePart part1 = new ClickhousePart("part1", "test_db", "test_table", shard);
        ClickhousePart part2 = new ClickhousePart("part2", "test_db", "test_table", shard);
        List<ClickhousePart> parts = Arrays.asList(part1, part2);

        split =
                new ClickhouseSourceSplit(
                        TablePath.of("test_db", "test_table"),
                        TablePath.of("test_db", "test_table"),
                        new ArrayList<>(parts),
                        shard,
                        "",
                        0,
                        "split-1");

        mockProxy = Mockito.mock(ClickhouseProxy.class, Mockito.RETURNS_DEEP_STUBS);

        initStreamValueReaderMock();

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

        when(mockProxy.batchFetchRecords(any(), eq(rowType))).thenReturn(mockRows);

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result.size());
        Assertions.assertEquals(0, reader.currentPartIndex);

        // Make sure the offset has been updated but the part has not been marked as end of part
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertEquals(BATCH_SIZE, parts.get(0).getOffset());
        Assertions.assertFalse(parts.get(0).isEndOfPart());
    }

    @Test
    public void testHasNextWithPartialBatch() {
        // Create mock data
        int partialSize = BATCH_SIZE - 2;
        List<SeaTunnelRow> mockRows = createMockRows(partialSize);

        when(mockProxy.batchFetchRecords(any(), eq(rowType))).thenReturn(mockRows);

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(partialSize, result.size());

        // Make sure the offset has been updated
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertEquals(partialSize, parts.get(0).getOffset());

        Assertions.assertTrue(reader.hasNext());
    }

    @Test
    public void testHasNextWithEmptyBatch() {
        // create empty test data
        List<SeaTunnelRow> mockRows = new ArrayList<>();

        when(mockProxy.batchFetchRecords(any(), eq(rowType))).thenReturn(mockRows);

        Assertions.assertFalse(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(0, result.size());

        // Make sure that part is marked as end of part
        List<ClickhousePart> parts = new ArrayList<>(split.getParts());
        Assertions.assertTrue(parts.get(0).isEndOfPart());
        Assertions.assertTrue(parts.get(0).isEndOfPart());

        Assertions.assertEquals(2, reader.currentPartIndex);
    }

    @Test
    public void testHasNextWithMultipleParts() {
        List<SeaTunnelRow> mockRows1 = createMockRows(BATCH_SIZE);

        int partialSize = 5;
        List<SeaTunnelRow> mockRows2 = createMockRows(partialSize);

        List<ClickhousePart> parts = split.getParts();

        // Return different data for different parts
        when(mockProxy.batchFetchRecords(any(), eq(rowType)))
                .thenAnswer(
                        invocation -> {
                            ClickhousePart part = parts.get(reader.currentPartIndex);
                            if ("part1".equals(part.getName())) {
                                return part.getOffset() == 0 ? mockRows1 : new ArrayList<>();
                            } else {
                                return part.getOffset() == 0 ? mockRows2 : new ArrayList<>();
                            }
                        });

        // First part - Full Batch
        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result1 = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result1.size());
        Assertions.assertEquals(0, reader.currentPartIndex);

        // Second part - Some Batches
        Assertions.assertTrue(reader.hasNext());
        Assertions.assertTrue(parts.get(0).isEndOfPart());

        List<SeaTunnelRow> result2 = reader.next();
        Assertions.assertEquals(partialSize, result2.size());
        Assertions.assertEquals(1, reader.currentPartIndex);

        // All parts have been processed. hasNext should return false
        Assertions.assertFalse(reader.hasNext());
        Assertions.assertTrue(parts.get(1).isEndOfPart());
    }

    @Test
    public void testPartStrategyReadWithNoSortingKey() {
        when(sourceTable.getClickhouseTable().getSortingKey()).thenReturn("");

        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result.size());

        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> nextResult = reader.next();
        Assertions.assertEquals(BATCH_SIZE, nextResult.size());

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testSqlStrategyReadWithNoSortingKey() {
        try {
            Field sqlStrategyField =
                    ClickhouseSourceTable.class.getDeclaredField("isSqlStrategyRead");
            sqlStrategyField.setAccessible(true);
            sqlStrategyField.set(sourceTable, true);
        } catch (Exception e) {
            Assertions.fail("Failed to set isSqlStrategyRead field", e);
        }

        when(sourceTable.getClickhouseTable().getSortingKey()).thenReturn("");

        Assertions.assertTrue(reader.hasNext());

        List<SeaTunnelRow> result = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result.size());

        Assertions.assertFalse(reader.hasNext());
    }

    @Test
    public void testSqlStrategyReadWithSortingKey() {
        try {
            Field sqlStrategyField =
                    ClickhouseSourceTable.class.getDeclaredField("isSqlStrategyRead");
            sqlStrategyField.setAccessible(true);
            sqlStrategyField.set(sourceTable, true);
        } catch (Exception e) {
            Assertions.fail("Failed to set isSqlStrategyRead field", e);
        }

        when(sourceTable.getClickhouseTable().getSortingKey()).thenReturn("id");

        List<SeaTunnelRow> firstBatch = createMockRows(BATCH_SIZE);
        List<SeaTunnelRow> secondBatch = createMockRows(5);
        List<SeaTunnelRow> emptyBatch = new ArrayList<>();

        when(mockProxy.batchFetchRecords(any(), eq(rowType)))
                .thenAnswer(
                        x ->
                                split.getSqlOffset() == 0
                                        ? firstBatch
                                        : split.getSqlOffset() == BATCH_SIZE
                                                ? secondBatch
                                                : emptyBatch);

        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result1 = reader.next();
        Assertions.assertEquals(BATCH_SIZE, result1.size());

        Assertions.assertTrue(reader.hasNext());
        List<SeaTunnelRow> result2 = reader.next();
        Assertions.assertEquals(5, result2.size());

        Assertions.assertFalse(reader.hasNext());

        Mockito.verify(mockProxy, Mockito.times(3)).batchFetchRecords(any(), any());
    }

    private void initStreamValueReaderMock() throws ClickHouseException {
        // mock ClickHouseResponse
        ClickHouseRequest mockRequest = Mockito.mock(ClickHouseRequest.class);
        ClickHouseRequest mockQueryRequest = Mockito.mock(ClickHouseRequest.class);
        ClickHouseResponse mockResponse = Mockito.mock(ClickHouseResponse.class);

        when(mockProxy.getClickhouseConnection()).thenReturn(mockRequest);
        when(mockRequest.query(any(String.class))).thenReturn(mockQueryRequest);
        when(mockQueryRequest.executeAndWait()).thenReturn(mockResponse);

        // create multiple batches of mock data
        List<ClickHouseRecord> mockRecords = createMockClickHouseRecords();
        when(mockResponse.records()).thenReturn(mockRecords);
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

    private List<ClickHouseRecord> createMockClickHouseRecords() {
        List<ClickHouseRecord> records = new ArrayList<>();

        for (int i = 0; i < BATCH_SIZE; i++) {
            ClickHouseRecord mockRecord = Mockito.mock(ClickHouseRecord.class);

            when(mockRecord.getValue(0)).thenReturn(ClickHouseLongValue.of((long) i));
            when(mockRecord.getValue(1)).thenReturn(ClickHouseStringValue.of("name" + i));
            when(mockRecord.getValue(2)).thenReturn(ClickHouseIntegerValue.of(20 + i));
            records.add(mockRecord);
        }
        return records;
    }
}
