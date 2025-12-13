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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * ClickhouseValueReader is responsible for reading data from ClickHouse database. It supports two
 * reading modes determined by {@link #shouldUseStreamReader()}:
 *
 * <p>1. Stream Mode: Used when the query is complex, no sorting key exists, or not all sorting key
 * columns are included in the query fields.
 *
 * <p>2. Batch Mode: Used keyset pagination approach by tracking the last row's sorting key values
 * from each batch. This mode requires {@link #isAllSortKeyInRowType()} to be true, meaning all
 * sorting key columns must be included in the query fields.
 */
@Slf4j
public class ClickhouseValueReader implements Serializable {
    private static final long serialVersionUID = 4588012013447713463L;

    private static final DateTimeFormatter TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ClickhouseSourceSplit clickhouseSourceSplit;
    private final SeaTunnelRowType rowTypeInfo;
    private final ClickhouseSourceTable clickhouseSourceTable;
    private StreamValueReader streamValueReader;
    private ClickhouseProxy proxy;
    private final boolean shouldUseStreamReader;

    protected int currentPartIndex = 0;

    private List<SeaTunnelRow> rowBatch;

    // SQL strategy keyset order values
    private List<Object> sqlLastOrderingKeyValues;

    public ClickhouseValueReader(
            ClickhouseSourceSplit clickhouseSourceSplit,
            SeaTunnelRowType seaTunnelRowType,
            ClickhouseSourceTable clickhouseSourceTable) {
        this.clickhouseSourceSplit = clickhouseSourceSplit;
        this.rowTypeInfo = seaTunnelRowType;
        this.clickhouseSourceTable = clickhouseSourceTable;
        this.proxy = new ClickhouseProxy(clickhouseSourceSplit.getShard().getNode());
        this.shouldUseStreamReader = shouldUseStreamReader();
    }

    public boolean hasNext() {
        if (shouldUseStreamReader) {
            if (streamValueReader == null) {
                streamValueReader = new StreamValueReader();
            }
            return streamValueReader.hasNext();
        } else if (clickhouseSourceTable.isSqlStrategyRead()) {
            return sqlBatchStrategyRead();
        } else {
            return partBatchStrategyRead();
        }
    }

    public List<SeaTunnelRow> next() {
        if (rowBatch == null) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.SHOULD_NEVER_HAPPEN, "never happen error !");
        }

        return rowBatch;
    }

    private boolean partBatchStrategyRead() {
        List<ClickhousePart> parts = clickhouseSourceSplit.getParts();
        int partSize = parts.size();

        if (currentPartIndex >= partSize) {
            return false;
        }

        ClickhousePart currentPart = parts.get(currentPartIndex);

        // If current part has been processed, move to the next part
        if (currentPart.isEndOfPart()) {
            currentPartIndex++;
            return currentPartIndex < partSize && partBatchStrategyRead();
        }

        try {
            String query = buildBatchPartQuery(currentPart);
            rowBatch =
                    proxy.batchFetchRecords(
                            query, clickhouseSourceTable.getTablePath(), rowTypeInfo);

            log.debug(
                    "SplitId: {}, partName: {} read rowBatch size: {}",
                    clickhouseSourceSplit.getSplitId(),
                    currentPart.getName(),
                    rowBatch.size());

            if (rowBatch.isEmpty()) {
                currentPart.setEndOfPart(true);
                currentPartIndex++;
                return currentPartIndex < partSize && partBatchStrategyRead();
            }

            // update Keyset cursor (last ordering key values)
            String sortingKey = clickhouseSourceTable.getClickhouseTable().getSortingKey();

            SeaTunnelRow lastRow = rowBatch.get(rowBatch.size() - 1);
            List<Object> keyValues = extractOrderingKeyValuesFromRow(lastRow, sortingKey);
            log.debug("lastRow: {}, extract ordering key values from row: {}", lastRow, keyValues);

            currentPart.setLastOrderingKeyValues(keyValues);

            return true;
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                    String.format(
                            "Failed to read data from part %s, shard: %s, splitId: %s, message: %s",
                            currentPart.getName(),
                            currentPart.getShard().getNode(),
                            clickhouseSourceSplit.getSplitId(),
                            e.getMessage()),
                    e);
        }
    }

    private boolean sqlBatchStrategyRead() {
        String query = buildBatchSqlQuery();

        try {
            rowBatch =
                    proxy.batchFetchRecords(
                            query, clickhouseSourceTable.getTablePath(), rowTypeInfo);

            String sortingKey = clickhouseSourceTable.getClickhouseTable().getSortingKey();

            if (rowBatch.isEmpty()) {
                return false;
            }
            SeaTunnelRow lastRow = rowBatch.get(rowBatch.size() - 1);

            sqlLastOrderingKeyValues = extractOrderingKeyValuesFromRow(lastRow, sortingKey);

            log.debug(
                    "lastRow: {}, extract ordering key values from row: {}",
                    lastRow,
                    sqlLastOrderingKeyValues);

            return !rowBatch.isEmpty();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                    String.format(
                            "Failed to read data from sql %s, shard: %s, splitId %s, message: %s",
                            query,
                            clickhouseSourceSplit.getShard().getNode(),
                            clickhouseSourceSplit.getSplitId(),
                            e.getMessage()),
                    e);
        }
    }

    public void close() {
        if (proxy != null) {
            proxy.close();
        }
        if (streamValueReader != null) {
            streamValueReader.close();
        }
    }

    private boolean shouldUseStreamReader() {
        return clickhouseSourceTable.isComplexSql()
                || StringUtils.isEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey())
                || !isAllSortKeyInRowType();
    }

    /** Verify if all sorting key exists in roTypeInfo */
    private boolean isAllSortKeyInRowType() {
        ClickhouseTable clickhouseTable = clickhouseSourceTable.getClickhouseTable();
        if (clickhouseTable == null || StringUtils.isEmpty(clickhouseTable.getSortingKey())) {
            return false;
        }
        String sortingKey = clickhouseTable.getSortingKey();
        List<String> sortingKeyList =
                Arrays.stream(sortingKey.split(",")).map(String::trim).collect(Collectors.toList());

        // check all sort key exists in rowTypeInfo
        Optional<String> sortKeyNotExistOpt =
                sortingKeyList.stream()
                        .filter(key -> rowTypeInfo.indexOf(key, false) == -1)
                        .findAny();

        return !sortKeyNotExistOpt.isPresent();
    }

    private String buildBatchPartQuery(ClickhousePart part) {
        TablePath tablePath = TablePath.of(part.getDatabase(), part.getTable());

        String whereClause = String.format("_part = '%s'", part.getName());
        if (StringUtils.isNotEmpty(clickhouseSourceTable.getFilterQuery())) {
            whereClause += " AND (" + clickhouseSourceTable.getFilterQuery() + ")";
        }

        String sortingKey = clickhouseSourceTable.getClickhouseTable().getSortingKey();

        String orderByClause = " ORDER BY " + sortingKey;

        String keysetWhere = "";
        // Key cursor mode pagination: when sorting key exists, use tuple comparison on
        // lastOrderingKeyValues
        if (part.getLastOrderingKeyValues() != null) {
            keysetWhere = buildKeysetWhereCondition(sortingKey, part.getLastOrderingKeyValues());
            if (!keysetWhere.isEmpty()) {
                whereClause += " AND (" + keysetWhere + ")";
            }
        }

        String sql;

        if (part.getLastOrderingKeyValues() != null) {
            // key cursor mode: no OFFSET, only LIMIT
            sql =
                    String.format(
                            "SELECT * FROM %s.%s WHERE %s %s LIMIT %d WITH TIES",
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            whereClause,
                            orderByClause,
                            clickhouseSourceTable.getBatchSize());
        } else {
            // for the first sql creation, lastOrderingKeyValues is null
            sql =
                    String.format(
                            "SELECT * FROM %s.%s WHERE %s %s LIMIT %d, %d WITH TIES",
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            whereClause,
                            orderByClause,
                            0,
                            clickhouseSourceTable.getBatchSize());
        }

        log.info("generate batch part sql: {}", sql);

        return sql;
    }

    private String buildBatchSqlQuery() {
        String base =
                String.format("SELECT * FROM (%s) AS t", clickhouseSourceSplit.getSplitQuery());

        String sortingKey = clickhouseSourceTable.getClickhouseTable().getSortingKey();

        String whereClause = "";
        if (sqlLastOrderingKeyValues != null) {
            String keyset = buildKeysetWhereCondition(sortingKey, sqlLastOrderingKeyValues);
            if (!keyset.isEmpty()) {
                whereClause = " WHERE (" + keyset + ")";
            }
        }

        // Add filter_query support for SQL batch strategy
        if (StringUtils.isNotEmpty(clickhouseSourceTable.getFilterQuery())) {
            if (whereClause.isEmpty()) {
                whereClause = " WHERE (" + clickhouseSourceTable.getFilterQuery() + ")";
            } else {
                whereClause += " AND (" + clickhouseSourceTable.getFilterQuery() + ")";
            }
        }

        String orderByClause = " ORDER BY " + sortingKey;

        String sql;
        if (sqlLastOrderingKeyValues != null) {
            // key cursor mode: no OFFSET, only LIMIT
            sql =
                    String.format(
                            "%s %s %s LIMIT %d WITH TIES",
                            base, whereClause, orderByClause, clickhouseSourceTable.getBatchSize());
        } else {
            // for the first sql creation, sqlLastOrderingKeyValues is null
            sql =
                    String.format(
                            "%s %s LIMIT %d, %d WITH TIES",
                            base, orderByClause, 0, clickhouseSourceTable.getBatchSize());
        }

        log.info("generate batch query sql: {}", sql);

        return sql;
    }

    /**
     * Build WHERE condition using the sorting key and last key values. Supports single or composite
     * keys, and generates lexicographic tuple comparison.
     */
    private String buildKeysetWhereCondition(String sortingKey, List<Object> lastKeyValues) {
        List<String> keyCols =
                Arrays.stream(sortingKey.split(",")).map(String::trim).collect(Collectors.toList());
        if (lastKeyValues == null
                || lastKeyValues.isEmpty()
                || keyCols.size() != lastKeyValues.size()) {
            return "";
        }

        // Build tuple comparison (c1, c2, ...) > (v1, v2, ...)
        String left = "(" + String.join(", ", keyCols) + ")";

        // Convert lastKeyValues to SQL literals based on rowTypeInfo
        String inlinedRight = "(" + buildSqlLiteralsForKeyValues(keyCols, lastKeyValues) + ")";

        return left + " > " + inlinedRight;
    }

    private String buildSqlLiteralsForKeyValues(List<String> keyCols, List<Object> values) {
        List<String> literals = new ArrayList<>();
        for (int i = 0; i < keyCols.size(); i++) {
            String col = keyCols.get(i);
            Object v = values.get(i);
            literals.add(toSqlLiteral(col, v));
        }
        return String.join(", ", literals);
    }

    private String toSqlLiteral(String column, Object value) {
        if (value == null) {
            return "NULL";
        }
        int idx = rowTypeInfo.indexOf(column, false);
        if (idx < 0) {
            // fallback: quote as string
            return quoteString(value.toString());
        }
        SeaTunnelDataType<?> t = rowTypeInfo.getFieldType(idx);
        switch (t.getSqlType()) {
            case STRING:
                return quoteString(value.toString());
            case BOOLEAN:
                return Boolean.TRUE.equals(value) ? "1" : "0";
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return value.toString();
            case DATE:
                if (value instanceof LocalDate) {
                    return quoteString(value.toString());
                }
                return quoteString(String.valueOf(value));
            case TIMESTAMP:
                if (value instanceof LocalDateTime) {
                    return quoteString(TS_FORMATTER.format((LocalDateTime) value));
                }
                return quoteString(String.valueOf(value));
            default:
                return quoteString(String.valueOf(value));
        }
    }

    private List<Object> extractOrderingKeyValuesFromRow(SeaTunnelRow row, String sortingKey) {
        List<String> keyCols =
                Arrays.stream(sortingKey.split(",")).map(String::trim).collect(Collectors.toList());
        List<Object> keyValues = new ArrayList<>(keyCols.size());
        for (String col : keyCols) {
            int idx = rowTypeInfo.indexOf(col, false);
            keyValues.add(row.getField(idx));
        }
        return keyValues;
    }

    private String quoteString(String s) {
        String escaped = s.replace("\\", "\\\\").replace("'", "''");
        return "'" + escaped + "'";
    }

    private class StreamValueReader implements Serializable {
        private static final long serialVersionUID = -7037116446966849773L;

        private final BlockingQueue<SeaTunnelRow> rowQueue;
        private AtomicBoolean eos = new AtomicBoolean(false);
        private final List<String> sqlList;

        public StreamValueReader() {
            this.rowQueue = new LinkedBlockingDeque<>(clickhouseSourceTable.getBatchSize());
            this.sqlList = buildSqlList();
            asyncReadThread.start();

            log.info("StreamValueReader start.");
        }

        private final Thread asyncReadThread =
                new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                String executeSql = "";
                                try {
                                    for (String sql : sqlList) {
                                        executeSql = sql;
                                        log.info("execute stream sql: {}", executeSql);
                                        try (ClickHouseResponse response =
                                                proxy.getClickhouseConnection()
                                                        .query(sql)
                                                        .executeAndWait()) {
                                            response.records()
                                                    .forEach(
                                                            record -> {
                                                                SeaTunnelRow seaTunnelRow =
                                                                        ClickhouseUtil
                                                                                .convertToSeaTunnelRow(
                                                                                        record,
                                                                                        rowTypeInfo,
                                                                                        clickhouseSourceTable
                                                                                                .getTablePath()
                                                                                                .getFullName());
                                                                try {
                                                                    rowQueue.put(seaTunnelRow);
                                                                } catch (InterruptedException e) {
                                                                    throw new ClickhouseConnectorException(
                                                                            ClickhouseConnectorErrorCode
                                                                                    .ROW_BATCH_GET_FAILED,
                                                                            e);
                                                                }
                                                            });
                                        }
                                    }
                                } catch (ClickHouseException e) {
                                    throw new ClickhouseConnectorException(
                                            ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                                            String.format(
                                                    "Failed to execute query: %s", executeSql),
                                            e);
                                } finally {
                                    eos.set(true);
                                    log.info("StreamValueReader finished reading data");
                                }
                            }
                        },
                        "clickhouse-stream-reader-" + clickhouseSourceSplit.getSplitId());

        public boolean hasNext() {
            List<SeaTunnelRow> rows = new ArrayList<>();
            while (!eos.get() || !rowQueue.isEmpty()) {
                if (!rowQueue.isEmpty()) {
                    try {
                        SeaTunnelRow seaTunnelRow = rowQueue.take();
                        rows.add(seaTunnelRow);
                        if (rows.size() >= clickhouseSourceTable.getBatchSize()) {
                            rowBatch = rows;
                            return true;
                        }
                    } catch (InterruptedException e) {
                        throw new ClickhouseConnectorException(
                                ClickhouseConnectorErrorCode.ROW_BATCH_GET_FAILED, e);
                    }
                } else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            if (!rows.isEmpty()) {
                rowBatch = rows;
                return true;
            }

            return false;
        }

        private List<String> buildSqlList() {
            if (clickhouseSourceTable.isSqlStrategyRead()) {
                return Collections.singletonList(clickhouseSourceSplit.getSplitQuery());
            } else {
                return clickhouseSourceSplit.getParts().stream()
                        .map(this::buildStreamPartQuery)
                        .collect(Collectors.toList());
            }
        }

        private String buildStreamPartQuery(ClickhousePart part) {
            TablePath tablePath = TablePath.of(part.getDatabase(), part.getTable());

            String whereClause = String.format("_part = '%s'", part.getName());
            if (StringUtils.isNotEmpty(clickhouseSourceTable.getFilterQuery())) {
                whereClause += " AND (" + clickhouseSourceTable.getFilterQuery() + ")";
            }

            return String.format(
                    "SELECT * FROM %s.%s WHERE %s",
                    tablePath.getDatabaseName(), tablePath.getTableName(), whereClause);
        }

        public void close() {
            if (rowQueue != null) {
                rowQueue.clear();
            }
            eos.set(true);
        }
    }
}
