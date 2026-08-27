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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhousePart;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.clickhouse.client.ClickHouseNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartStrategySplitterTest {

    @Mock private ClickhouseTable mockTable;

    private PartStrategySplitter splitter;
    private static final String DATABASE_NAME = "test_db";
    private static final String TABLE_NAME = "test_table";

    @BeforeEach
    public void init() {
        MockitoAnnotations.openMocks(this);

        Mockito.when(mockTable.getDatabase()).thenReturn(DATABASE_NAME);
        Mockito.when(mockTable.getTableName()).thenReturn(TABLE_NAME);
        Mockito.when(mockTable.getLocalDatabase()).thenReturn(DATABASE_NAME);
        Mockito.when(mockTable.getLocalTableName()).thenReturn(TABLE_NAME);

        splitter = new PartStrategySplitter();
    }

    @Test
    public void testPartCountLimitForOneSplit() {
        // Test the specified partition size
        ClickhouseSourceTable sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .splitSize(5)
                        .build();

        int partSize = splitter.partCountLimitForOneSplit(sourceTable);
        Assertions.assertEquals(5, partSize);

        // Test the partition size that is smaller than the minimum value
        sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .splitSize(0)
                        .build();

        partSize = splitter.partCountLimitForOneSplit(sourceTable);
        Assertions.assertEquals(ClickhouseSourceOptions.CLICKHOUSE_SPLIT_SIZE_MIN, partSize);

        // The partition size was not set in the test
        sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .build();

        partSize = splitter.partCountLimitForOneSplit(sourceTable);
        Assertions.assertEquals(ClickhouseSourceOptions.CLICKHOUSE_SPLIT_SIZE_DEFAULT, partSize);
    }

    @Test
    public void testPartMapToSplits() {
        ClickHouseNode node = ClickHouseNode.builder().host("localhost").port(8123).build();

        Shard shard = new Shard(1, 1, node);

        List<ClickhousePart> parts = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            parts.add(new ClickhousePart("part" + i, DATABASE_NAME, TABLE_NAME, shard));
        }

        Map<Shard, List<ClickhousePart>> shardToParts = new HashMap<>();
        shardToParts.put(shard, parts);

        ClickhouseSourceTable sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .splitSize(6)
                        .clickhouseTable(mockTable)
                        .build();

        List<ClickhouseSourceSplit> splits = splitter.partMapToSplits(sourceTable, shardToParts);

        Assertions.assertEquals(3, splits.size());
        Assertions.assertEquals(6, splits.get(0).getParts().size());
        Assertions.assertEquals(6, splits.get(1).getParts().size());
        Assertions.assertEquals(3, splits.get(2).getParts().size());
    }

    @Test
    public void testPartMapToSplitsWithMultipleShards() {
        ClickHouseNode node1 = ClickHouseNode.builder().host("localhost").port(8123).build();

        ClickHouseNode node2 = ClickHouseNode.builder().host("localhost").port(8124).build();

        Shard shard1 = new Shard(1, 1, node1);
        Shard shard2 = new Shard(2, 1, node2);

        List<ClickhousePart> parts1 = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            parts1.add(new ClickhousePart("part" + i, DATABASE_NAME, TABLE_NAME, shard1));
        }

        List<ClickhousePart> parts2 = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            parts2.add(new ClickhousePart("part" + (i + 10), DATABASE_NAME, TABLE_NAME, shard2));
        }

        Map<Shard, List<ClickhousePart>> shardToParts = new HashMap<>();
        shardToParts.put(shard1, parts1);
        shardToParts.put(shard2, parts2);

        ClickhouseSourceTable sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .splitSize(5)
                        .clickhouseTable(mockTable)
                        .build();

        List<ClickhouseSourceSplit> splits = splitter.partMapToSplits(sourceTable, shardToParts);

        Assertions.assertEquals(5, splits.size());

        int shard1SplitCount = 0;
        int shard2SplitCount = 0;

        for (ClickhouseSourceSplit split : splits) {
            if (split.getShard().equals(shard1)) {
                shard1SplitCount++;
            } else if (split.getShard().equals(shard2)) {
                shard2SplitCount++;
            }
        }

        Assertions.assertEquals(2, shard1SplitCount);
        Assertions.assertEquals(3, shard2SplitCount);
    }

    @Test
    public void testPartMapToSplitsWithDuplicateParts() {
        ClickHouseNode node = ClickHouseNode.builder().host("localhost").port(8123).build();

        Shard shard = new Shard(1, 1, node);

        List<ClickhousePart> parts = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            parts.add(new ClickhousePart("part" + i, DATABASE_NAME, TABLE_NAME, shard));
            // add duplicate part
            parts.add(new ClickhousePart("part" + i, DATABASE_NAME, TABLE_NAME, shard));
        }

        Map<Shard, List<ClickhousePart>> shardToParts = new HashMap<>();
        shardToParts.put(shard, parts);

        ClickhouseSourceTable sourceTable =
                ClickhouseSourceTable.builder()
                        .tablePath(TablePath.of(DATABASE_NAME, TABLE_NAME))
                        .splitSize(4)
                        .clickhouseTable(mockTable)
                        .build();

        List<ClickhouseSourceSplit> splits = splitter.partMapToSplits(sourceTable, shardToParts);

        Assertions.assertEquals(2, splits.size());
        Assertions.assertEquals(4, splits.get(0).getParts().size());
        Assertions.assertEquals(2, splits.get(1).getParts().size());
    }
}
