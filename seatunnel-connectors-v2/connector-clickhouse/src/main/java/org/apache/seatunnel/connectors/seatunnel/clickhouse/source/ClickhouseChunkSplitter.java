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

import com.clickhouse.client.*;
import com.clickhouse.client.data.ClickHouseDateTimeValue;
import com.clickhouse.client.data.ClickHouseDateValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Parallel reading shard splitting strategy, mainly divided into two categories according to the type of partition field:
 *
 * <p>1. Numeric types
 *
 * <p>Numeric types include pure numeric types and date types:
 *
 * <p>(1) Pure numeric types
 * <p>Calculate the partition size based on the lower and upper bounds, and split according to the number of partitions
 * (the last partition may be smaller than the partition size).
 *
 * <p>(2) Time types
 * <p>Time types mainly include two categories: Date and DateTime. Regardless of the category, they will first be
 * converted to their numerical values, and then the splitting algorithm is the same as that for pure numeric types.
 * After splitting into partitions, if the field is of type Date, ClickHouse's toDate() function will be used to
 * convert the partition values. If it is of type DateTime, the toDateTime64() function will be used instead.
 *
 * <p>Regardless of whether it is a pure numeric type or a time type, if the lower or upper bound is not specified,
 * the database will be requested to obtain the maximum and minimum values.
 *
 * <p>2. String types
 * <p>For strings, specifying upper and lower bounds is invalid. The splitting algorithm will take the modulus of
 * the partition field according to the number of partitions to split the data.
 */
@Slf4j
public class ClickhouseChunkSplitter {

    public List<ClickHouseSourceSplit> generateSplits(
            ClickhouseSourceConfig sourceConfig, CatalogTable table) throws Exception {
        log.info("Start splitting table {} into chunks...", table.getTablePath());
        long start = System.currentTimeMillis();

        List<ClickHouseSourceSplit> splits;
        Optional<SeaTunnelRowType> splitKeyOptional = findSplitKey(sourceConfig, table);
        if (!splitKeyOptional.isPresent()) {
            ClickHouseSourceSplit split = createSingleSplit(sourceConfig, table);
            splits = Collections.singletonList(split);
        } else {
            if (splitKeyOptional.get().getTotalFields() != 1) {
                throw new UnsupportedOperationException("Currently, only support one split key");
            }
            splits = createSplits(sourceConfig, table, splitKeyOptional.get());
        }

        long end = System.currentTimeMillis();
        log.info(
                "Split table {} into {} chunks, time cost: {}ms.",
                table.getTablePath(),
                splits.size(),
                end - start);
        return splits;
    }

    private List<ClickHouseSourceSplit> createSplits(
            ClickhouseSourceConfig sourceConfig, CatalogTable table, SeaTunnelRowType splitKey)
            throws ClickHouseException {
        String splitKeyName = splitKey.getFieldNames()[0];
        SeaTunnelDataType<?> splitKeyType = splitKey.getFieldType(0);

        if (SqlType.STRING == splitKeyType.getSqlType()) {
            return createStringColumnSplits(sourceConfig, table, splitKeyName);
        }
        return getNumberColumnSplits(sourceConfig, table, splitKeyType, splitKeyName);
    }

    private List<ClickHouseSourceSplit> getNumberColumnSplits(
            ClickhouseSourceConfig sourceConfig,
            CatalogTable table,
            SeaTunnelDataType<?> splitKeyType,
            String splitKeyName)
            throws ClickHouseException {
        Pair<BigDecimal, BigDecimal> partitionBoundValue =
                getPartitionBoundValue(sourceConfig, splitKeyType);
        BigDecimal partitionStart = partitionBoundValue.getLeft();
        BigDecimal partitionEnd = partitionBoundValue.getRight();
        if (partitionStart == null || partitionEnd == null) {
            Pair<BigDecimal, BigDecimal> range = queryMinMax(sourceConfig, splitKeyName);
            partitionStart = range.getLeft();
            partitionEnd = range.getRight();
        }
        if (partitionStart == null || partitionEnd == null) {
            ClickHouseSourceSplit split = createSingleSplit(sourceConfig, table);
            return Collections.singletonList(split);
        }

        return createNumberColumnSplits(
                sourceConfig, table, splitKeyType, splitKeyName, partitionStart, partitionEnd);
    }

    private List<ClickHouseSourceSplit> createNumberColumnSplits(
            ClickhouseSourceConfig sourceConfig,
            CatalogTable table,
            SeaTunnelDataType<?> splitKeyType,
            String splitKeyName,
            BigDecimal partitionStart,
            BigDecimal partitionEnd) {
        ClickhouseNumericBetweenParametersProvider numericBetweenParametersProvider =
                new ClickhouseNumericBetweenParametersProvider(partitionStart, partitionEnd)
                        .ofBatchNum(sourceConfig.getPartitionNum());
        Serializable[][] parameterValues = numericBetweenParametersProvider.getParameterValues();
        List<ClickHouseSourceSplit> splits = new ArrayList<>(sourceConfig.getPartitionNum());
        for (int i = 0; i < parameterValues.length; i++) {
            Serializable splitStart = parameterValues[i][0];
            Serializable splitEnd = parameterValues[i][1];
            Pair<Object, Object> formattedSplitRange =
                    formatSplitRange(splitStart, splitEnd, splitKeyType);

            String splitQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE %s BETWEEN %s and %s",
                            sourceConfig.getSql(),
                            splitKeyName,
                            formattedSplitRange.getLeft(),
                            formattedSplitRange.getRight());

            ClickHouseSourceSplit split =
                    new ClickHouseSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            splitQuery);
            splits.add(split);
        }
        return splits;
    }

    private Pair<Object, Object> formatSplitRange(
            Serializable splitStart, Serializable splitEnd, SeaTunnelDataType<?> splitKeyType) {
        if (splitKeyType instanceof LocalTimeType) {
            if (SqlType.DATE == splitKeyType.getSqlType()) {
                Serializable dateSplitStart = String.format("toDate(%s)", splitStart);
                Serializable dateSplitEnd = String.format("toDate(%s)", splitEnd);
                return Pair.of(dateSplitStart, dateSplitEnd);
            } else {
                Serializable dateSplitStart = String.format("toDateTime64(%s, 3)", splitStart);
                Serializable dateSplitEnd = String.format("toDateTime64(%s, 3)", splitEnd);
                return Pair.of(dateSplitStart, dateSplitEnd);
            }
        }
        return Pair.of(splitStart, splitEnd);
    }

    protected Pair<BigDecimal, BigDecimal> queryMinMax(
            ClickhouseSourceConfig sourceConfig, String columnName) throws ClickHouseException {
        String sqlQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM (%s) tmp",
                        columnName, columnName, sourceConfig.getSql());
        log.info("Split table, query min max: {}", sqlQuery);

        List<ClickHouseNode> nodes = sourceConfig.getNodes();
        ClickHouseNode currentServer = nodes.get(ThreadLocalRandom.current().nextInt(nodes.size()));
        try (ClickHouseClient client = ClickHouseClient.newInstance(currentServer.getProtocol());
             ClickHouseResponse response =
                        client.connect(currentServer)
                                .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                                .query(sqlQuery)
                                .executeAndWait()) {

            List<ClickHouseRecord> records = response.stream().collect(Collectors.toList());
            if (records.isEmpty()) {
                return Pair.of(null, null);
            } else {
                ClickHouseRecord values = records.get(0);
                return Pair.of(
                        values.getValue(0).asBigDecimal(), values.getValue(1).asBigDecimal());
            }
        }
    }

    private Pair<BigDecimal, BigDecimal> getPartitionBoundValue(
            ClickhouseSourceConfig sourceConfig, SeaTunnelDataType<?> splitKeyType) {
        Function<String, BigDecimal> dateTimeTranslator =
                value -> ClickHouseDateTimeValue.of(value, 3,
                        TimeZone.getTimeZone(sourceConfig.getServerTimeZone())).asBigDecimal();
        Map<SqlType, Function<String, BigDecimal>> timeTranslatorMap =
                new HashMap<SqlType, Function<String, BigDecimal>>() {
                    {
                        // Clickhouse Type: Date
                        put(SqlType.DATE, value ->
                                ClickHouseDateValue.of(LocalDate.parse(value)).asBigDecimal());
                        // Clickhouse Type: DateTime
                        put(SqlType.TIME, dateTimeTranslator);
                        put(SqlType.TIMESTAMP, dateTimeTranslator);
                        put(SqlType.TIMESTAMP_TZ, dateTimeTranslator);
                    }
                };

        BigDecimal partitionStart = null;
        BigDecimal partitionEnd = null;
        try {
            if (sourceConfig.getPartitionLowerBound() != null) {
                partitionStart =
                        timeTranslatorMap
                                .getOrDefault(splitKeyType.getSqlType(), BigDecimal::new)
                                .apply(sourceConfig.getPartitionLowerBound());
            }
            if (sourceConfig.getPartitionUpperBound() != null) {
                partitionEnd =
                        timeTranslatorMap
                                .getOrDefault(splitKeyType.getSqlType(), BigDecimal::new)
                                .apply(sourceConfig.getPartitionUpperBound());
            }
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Translate partition bound value failed.", e);
        }

        return Pair.of(partitionStart, partitionEnd);
    }

    private List<ClickHouseSourceSplit> createStringColumnSplits(
            ClickhouseSourceConfig sourceConfig, CatalogTable table, String splitKeyName) {

        List<ClickHouseSourceSplit> splits = new ArrayList<>(sourceConfig.getPartitionNum());
        for (int i = 0; i < sourceConfig.getPartitionNum(); i++) {
            String splitQuery =
                    String.format(
                            "SELECT * FROM (%s) st_clickhouse_splitter WHERE %s",
                            sourceConfig.getSql(),
                            getSplitHashClause(splitKeyName, sourceConfig.getPartitionNum(), i));
            ClickHouseSourceSplit split =
                    new ClickHouseSourceSplit(
                            table.getTablePath(),
                            createSplitId(table.getTablePath(), i),
                            splitQuery);

            splits.add(split);
        }
        return splits;
    }

    private Optional<SeaTunnelRowType> findSplitKey(
            ClickhouseSourceConfig sourceConfig, CatalogTable table) {
        TableSchema schema = table.getTableSchema();
        List<Column> columns = schema.getColumns();
        Map<String, Column> columnMap =
                columns.stream()
                        .collect(
                                Collectors.toMap(
                                        Column::getName, column -> column, (c1, c2) -> c1));
        if (sourceConfig.getPartitionColumn() != null) {
            String partitionColumn = sourceConfig.getPartitionColumn();
            Column column = columnMap.get(partitionColumn);
            if (column == null) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        String.format(
                                "Partitioned column(%s) don't exist in the table columns",
                                partitionColumn));
            }
            if (!isSupportSplitColumn(column)) {
                throw new ClickhouseConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        String.format("%s is not numeric/string type", partitionColumn));
            }
            return Optional.of(
                    new SeaTunnelRowType(
                            new String[] {partitionColumn},
                            new SeaTunnelDataType[] {column.getDataType()}));
        }

        log.warn("No split key found for table {}", table.getTablePath());
        return Optional.empty();
    }

    private boolean isSupportSplitColumn(Column splitColumn) {
        SeaTunnelDataType<?> dataType = splitColumn.getDataType();
        // currently, we only support these types.
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TIMESTAMP_TZ:
                return true;
            default:
                return false;
        }
    }

    private ClickHouseSourceSplit createSingleSplit(
            ClickhouseSourceConfig sourceConfig, CatalogTable table) {
        return new ClickHouseSourceSplit(
                table.getTablePath(),
                createSplitId(table.getTablePath(), 0),
                sourceConfig.getSql());
    }

    private String createSplitId(TablePath tablePath, int index) {
        return String.format("%s-%s", tablePath, index);
    }

    private String getSplitHashClause(String fieldName, int partitionNum, int index) {
        return String.format(
                "xxHash32(coalesce(`%s`, '')) %% %s = %s", fieldName, partitionNum, index);
    }
}
