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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ClickhouseChunkSplitterTest {

    private static final String QUERY_SQL = "select * from student";

    private final ClickhouseChunkSplitter splitter = new ClickhouseChunkSplitter();

    private final CatalogTable catalogTable = getCatalogTable();

    @Test
    public void testSplitWithoutPartitionColumn() throws Exception {
        ClickhouseSourceConfig sourceConfig = getSourceConfig(null, null, null, null);
        CatalogTable catalogTable = getCatalogTable();

        Collection<ClickHouseSourceSplit> splits =
                splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(1, splits.size());

        ClickHouseSourceSplit split = splits.iterator().next();
        Assertions.assertEquals("select * from student", split.getSplitQuery());
    }

    @Test
    public void testSplitWithPartitionColumnAndWithBoundNumberColumn() throws Exception {
        // 1 partition test
        ClickhouseSourceConfig sourceConfig = getSourceConfig("id", "1", "30", 1);

        List<ClickHouseSourceSplit> splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(1, splits.size());
        String expectedQuery =
                String.format(
                        "SELECT * FROM (%s) st_clickhouse_splitter WHERE id BETWEEN %s and %s",
                        QUERY_SQL, 1, 30);
        Assertions.assertEquals(expectedQuery, splits.get(0).getSplitQuery());

        // 3 partitions test
        sourceConfig = getSourceConfig("id", "1", "30", 3);

        splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(3, splits.size());

        List<Pair<Integer, Integer>> boundValues =
                ImmutableList.of(Pair.of(1, 10), Pair.of(11, 20), Pair.of(21, 30));
        for (int i = 0; i < splits.size(); i++) {
            Pair<Integer, Integer> boundValue = boundValues.get(i);
            expectedQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE id BETWEEN %s and %s",
                            QUERY_SQL, boundValue.getLeft(), boundValue.getRight());
            String splitQuery = splits.get(i).getSplitQuery();
            Assertions.assertEquals(expectedQuery, splitQuery);
        }
    }

    @Test
    public void testSplitWithPartitionColumnAndWithBoundDateColumn() throws Exception {
        // 1 partition test
        ClickhouseSourceConfig sourceConfig =
                getSourceConfig("enrollment_date", "2025-05-01", "2025-06-08", 1);

        List<ClickHouseSourceSplit> splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(1, splits.size());

        LocalDate baseDate = LocalDate.of(1970, 1, 1);
        LocalDate lowerBoundDate = LocalDate.of(2025, 5, 1);
        LocalDate upperBoundDate = LocalDate.of(2025, 6, 8);
        String expectedQuery =
                String.format(
                        "SELECT * FROM (%s) st_clickhouse_splitter WHERE enrollment_date BETWEEN toDate(%s) and toDate(%s)",
                        QUERY_SQL,
                        ChronoUnit.DAYS.between(baseDate, lowerBoundDate),
                        ChronoUnit.DAYS.between(baseDate, upperBoundDate));
        Assertions.assertEquals(expectedQuery, splits.get(0).getSplitQuery());

        // 3 partition test
        sourceConfig = getSourceConfig("enrollment_date", "2025-05-01", "2025-06-06", 3);
        splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(3, splits.size());

        List<Pair<LocalDate, LocalDate>> boundValues =
                ImmutableList.of(
                        Pair.of(LocalDate.of(2025, 5, 1), LocalDate.of(2025, 5, 13)),
                        Pair.of(LocalDate.of(2025, 5, 14), LocalDate.of(2025, 5, 26)),
                        Pair.of(LocalDate.of(2025, 5, 27), LocalDate.of(2025, 6, 6)));
        for (int i = 0; i < splits.size(); i++) {
            Pair<LocalDate, LocalDate> boundValue = boundValues.get(i);
            expectedQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE enrollment_date BETWEEN toDate(%s) and toDate(%s)",
                            QUERY_SQL,
                            ChronoUnit.DAYS.between(baseDate, boundValue.getLeft()),
                            ChronoUnit.DAYS.between(baseDate, boundValue.getRight()));
            String splitQuery = splits.get(i).getSplitQuery();
            Assertions.assertEquals(expectedQuery, splitQuery);
        }
    }

    @Test
    public void testSplitWithPartitionColumnAndWithBoundDateTimeColumn() throws Exception {
        // 1 partition test
        ClickhouseSourceConfig sourceConfig =
                getSourceConfig("created_at", "2025-05-01 12:30:00", "2025-06-06 15:30:00", 1);

        List<ClickHouseSourceSplit> splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(1, splits.size());

        LocalDateTime lowerBoundDateTime = LocalDateTime.of(2025, 5, 1, 12, 30, 0);
        LocalDateTime upperBoundDateTime = LocalDateTime.of(2025, 6, 6, 15, 30, 0);
        String expectedQuery =
                String.format(
                        "SELECT * FROM (%s) st_clickhouse_splitter WHERE created_at BETWEEN toDateTime64(%s, 3) and toDateTime64(%s, 3)",
                        QUERY_SQL,
                        lowerBoundDateTime.atZone(ZoneId.systemDefault()).toEpochSecond(),
                        upperBoundDateTime.atZone(ZoneId.systemDefault()).toEpochSecond());
        Assertions.assertEquals(expectedQuery, splits.get(0).getSplitQuery());

        // 3 partition test
        sourceConfig =
                getSourceConfig("created_at", "2025-05-01 12:30:00", "2025-06-06 15:30:00", 3);
        splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(3, splits.size());

        List<Pair<LocalDateTime, LocalDateTime>> boundValues =
                ImmutableList.of(
                        Pair.of(
                                LocalDateTime.of(2025, 5, 1, 12, 30, 0),
                                LocalDateTime.of(2025, 5, 13, 13, 30, 0)),
                        Pair.of(
                                LocalDateTime.of(2025, 5, 13, 13, 30, 1),
                                LocalDateTime.of(2025, 5, 25, 14, 30, 1)),
                        Pair.of(
                                LocalDateTime.of(2025, 5, 25, 14, 30, 2),
                                LocalDateTime.of(2025, 6, 6, 15, 30, 0)));
        for (int i = 0; i < splits.size(); i++) {
            Pair<LocalDateTime, LocalDateTime> boundValue = boundValues.get(i);
            expectedQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE created_at BETWEEN toDateTime64(%s, 3) and toDateTime64(%s, 3)",
                            QUERY_SQL,
                            boundValue.getLeft().atZone(ZoneId.systemDefault()).toEpochSecond(),
                            boundValue.getRight().atZone(ZoneId.systemDefault()).toEpochSecond());
            String splitQuery = splits.get(i).getSplitQuery();
            Assertions.assertEquals(expectedQuery, splitQuery);
        }
    }

    @Test
    public void testSplitWithPartitionColumnAndWithBoundStringColumn() throws Exception {
        // 1 partition test
        ClickhouseSourceConfig sourceConfig = getSourceConfig("name", null, null, 1);

        List<ClickHouseSourceSplit> splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(1, splits.size());

        String expectedQuery =
                String.format(
                        "SELECT * FROM (%s) st_clickhouse_splitter WHERE xxHash32(coalesce(`name`, '')) %% 1 = 0",
                        QUERY_SQL);
        Assertions.assertEquals(expectedQuery, splits.get(0).getSplitQuery());

        // 3 partition test
        sourceConfig = getSourceConfig("name", null, null, 3);

        splits = splitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(3, splits.size());

        List<Integer> boundValues = ImmutableList.of(0, 1, 2);
        for (int i = 0; i < splits.size(); i++) {
            expectedQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE xxHash32(coalesce(`name`, '')) %% 3 = %s",
                            QUERY_SQL, boundValues.get(i));
            String splitQuery = splits.get(i).getSplitQuery();
            Assertions.assertEquals(expectedQuery, splitQuery);
        }
    }

    // This test make sure queryMinMax method will work as expected,
    // and no need to add all column type test.
    // Note that there is no equal division here, and negative numbers are also included,
    // but the split algorithm is still effective.
    @Test
    public void testSplitWithPartitionColumnAndWithoutBound() throws Exception {
        ClickhouseChunkSplitter spySplitter = Mockito.spy(splitter);
        ClickhouseSourceConfig sourceConfig = getSourceConfig("id", null, null, 3);
        // mock the queryMinMax method
        Pair<BigDecimal, BigDecimal> queryBoundValues =
                Pair.of(new BigDecimal(-3), new BigDecimal(31));
        Mockito.doReturn(queryBoundValues).when(spySplitter).queryMinMax(sourceConfig, "id");

        List<ClickHouseSourceSplit> splits = spySplitter.generateSplits(sourceConfig, catalogTable);
        Assertions.assertEquals(3, splits.size());

        List<Pair<Integer, Integer>> boundValues =
                ImmutableList.of(Pair.of(-3, 8), Pair.of(9, 20), Pair.of(21, 31));
        for (int i = 0; i < splits.size(); i++) {
            Pair<Integer, Integer> boundValue = boundValues.get(i);
            String expectedQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE id BETWEEN %s and %s",
                            QUERY_SQL, boundValue.getLeft(), boundValue.getRight());
            String splitQuery = splits.get(i).getSplitQuery();
            Assertions.assertEquals(expectedQuery, splitQuery);
        }
    }

    private ClickhouseSourceConfig getSourceConfig(
            String partitionColumn,
            String partitionLowerBound,
            String partitionUpperBound,
            Integer partitionNum) {
        return ClickhouseSourceConfig.builder()
                .serverTimeZone(ZoneId.systemDefault().getId())
                .sql(QUERY_SQL)
                .partitionColumn(partitionColumn)
                .partitionLowerBound(partitionLowerBound)
                .partitionUpperBound(partitionUpperBound)
                .partitionNum(partitionNum)
                .build();
    }

    private CatalogTable getCatalogTable() {
        TableIdentifier tableId =
                new TableIdentifier("clickhouse_catalog", "test", null, "student");
        TableSchema tableSchema = getTableSchema();

        return CatalogTable.of(
                tableId,
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "",
                "clickhouse_catalog");
    }

    private TableSchema getTableSchema() {
        PhysicalColumn id = new PhysicalColumn("id", BasicType.LONG_TYPE, 4L, 0, false, null, null);
        PhysicalColumn name =
                new PhysicalColumn("name", BasicType.STRING_TYPE, 1L, 0, false, null, null);
        PhysicalColumn enrollmentDate =
                new PhysicalColumn(
                        "enrollment_date", LocalTimeType.LOCAL_DATE_TYPE, 2L, 0, false, null, null);
        PhysicalColumn createdAt =
                new PhysicalColumn(
                        "created_at", LocalTimeType.LOCAL_DATE_TIME_TYPE, 2L, 0, false, null, null);
        List<Column> columns = new ArrayList<>();
        columns.add(id);
        columns.add(name);
        columns.add(enrollmentDate);
        columns.add(createdAt);

        return TableSchema.builder().columns(columns).build();
    }
}
