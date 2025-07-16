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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import org.apache.commons.lang3.StringUtils;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class ClickhouseValueReader implements Serializable {
    private static final long serialVersionUID = 4588012013447713463L;

    private final ClickhouseSourceSplit clickhouseSourceSplit;
    private final SeaTunnelRowType rowTypeInfo;
    private final ClickhouseSourceTable clickhouseSourceTable;
    private StreamValueReader streamValueReader;
    private ClickhouseProxy proxy;

    protected int currentPartIndex = 0;

    private List<SeaTunnelRow> rowBatch;

    public ClickhouseValueReader(
            ClickhouseSourceSplit clickhouseSourceSplit,
            SeaTunnelRowType seaTunnelRowType,
            ClickhouseSourceTable clickhouseSourceTable) {
        this.clickhouseSourceSplit = clickhouseSourceSplit;
        this.rowTypeInfo = seaTunnelRowType;
        this.clickhouseSourceTable = clickhouseSourceTable;
        this.proxy = new ClickhouseProxy(clickhouseSourceSplit.getShard().getNode());
    }

    public boolean hasNext() {
        if (shouldUseStreamReader()) {
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
            String query = buildPartQuery(currentPart);
            rowBatch = proxy.batchFetchRecords(query, rowTypeInfo);

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

            // update part offset
            currentPart.setOffset(currentPart.getOffset() + rowBatch.size());
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
        String query = buildSqlQuery();

        try {
            rowBatch = proxy.batchFetchRecords(query, rowTypeInfo);

            clickhouseSourceSplit.setSqlOffset(
                    clickhouseSourceSplit.getSqlOffset() + rowBatch.size());

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
                || StringUtils.isEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey());
    }

    private String buildPartQuery(ClickhousePart part) {
        TablePath tablePath = TablePath.of(part.getDatabase(), part.getTable());

        String whereClause = String.format("_part = '%s'", part.getName());
        if (StringUtils.isNotEmpty(clickhouseSourceTable.getFilterQuery())) {
            whereClause += " AND (" + clickhouseSourceTable.getFilterQuery() + ")";
        }

        String orderByClause = "";
        if (StringUtils.isNotEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey())) {
            orderByClause =
                    " ORDER BY " + clickhouseSourceTable.getClickhouseTable().getSortingKey();
        }

        String sql;
        if (StringUtils.isNotEmpty(orderByClause)) {
            sql =
                    String.format(
                            "SELECT * FROM %s.%s WHERE %s %s LIMIT %d, %d WITH TIES",
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            whereClause,
                            orderByClause,
                            part.getOffset(),
                            clickhouseSourceTable.getBatchSize());
        } else {
            sql =
                    String.format(
                            "SELECT * FROM %s.%s WHERE %s",
                            tablePath.getDatabaseName(), tablePath.getTableName(), whereClause);
        }

        return sql;
    }

    private String buildSqlQuery() {
        String orderByClause = "";
        if (StringUtils.isNotEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey())) {
            orderByClause =
                    " ORDER BY " + clickhouseSourceTable.getClickhouseTable().getSortingKey();
        }

        String executeSql;
        if (StringUtils.isNotEmpty(orderByClause)) {
            executeSql =
                    String.format(
                            "SELECT * FROM (%s) AS t %s LIMIT %d, %d WITH TIES",
                            clickhouseSourceSplit.getSplitQuery(),
                            orderByClause,
                            clickhouseSourceSplit.getSqlOffset(),
                            clickhouseSourceTable.getBatchSize());
        } else {
            executeSql =
                    String.format("SELECT * FROM (%s) AS t", clickhouseSourceSplit.getSplitQuery());
        }

        return executeSql;
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
                        .map(ClickhouseValueReader.this::buildPartQuery)
                        .collect(Collectors.toList());
            }
        }

        public void close() {
            if (rowQueue != null) {
                rowQueue.clear();
            }
            eos.set(true);
        }
    }
}
