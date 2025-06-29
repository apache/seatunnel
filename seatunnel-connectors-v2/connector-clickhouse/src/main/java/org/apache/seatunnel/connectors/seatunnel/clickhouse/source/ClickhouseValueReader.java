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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;

import org.apache.commons.lang3.StringUtils;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseRecord;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
        if (clickhouseSourceTable.isComplexSql()) {
            this.streamValueReader =
                    new StreamValueReader(proxy, clickhouseSourceSplit.getSplitQuery());
        }
    }

    public boolean hasNext() {
        if (clickhouseSourceTable.isComplexSql()) {
            return streamValueReader.hasNext();
        } else if (clickhouseSourceTable.isSqlStrategyRead()) {
            return sqlStrategyRead();
        } else {
            return partStrategyRead();
        }
    }

    public List<SeaTunnelRow> next() {
        if (rowBatch == null) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.SHOULD_NEVER_HAPPEN, "never happen error !");
        }

        return rowBatch;
    }

    private boolean partStrategyRead() {
        List<ClickhousePart> parts = clickhouseSourceSplit.getParts();
        int partSize = parts.size();

        if (currentPartIndex >= partSize) {
            return false;
        }

        ClickhousePart currentPart = parts.get(currentPartIndex);

        if (StringUtils.isEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey())
                && currentPart.getOffset() != 0) {
            log.debug("Sorting key is empty, the part will be only read once.");
            currentPartIndex++;
            return currentPartIndex < partSize && partStrategyRead();
        }

        // If current part has been processed, move to the next part
        if (currentPart.isEos()) {
            currentPartIndex++;
            return currentPartIndex < partSize && partStrategyRead();
        }

        try {
            rowBatch =
                    proxy.queryDataFromPart(
                            currentPart,
                            rowTypeInfo,
                            clickhouseSourceTable,
                            currentPart.getOffset());

            log.debug(
                    "SplitId: {}, partName: {} read rowBatch size: {}",
                    clickhouseSourceSplit.getSplitId(),
                    currentPart.getName(),
                    rowBatch.size());

            if (rowBatch.isEmpty()) {
                currentPart.setEos(true);
                currentPartIndex++;
                return currentPartIndex < partSize && partStrategyRead();
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

    private boolean sqlStrategyRead() {
        String splitQuery = clickhouseSourceSplit.getSplitQuery();

        if (StringUtils.isEmpty(clickhouseSourceTable.getClickhouseTable().getSortingKey())
                && clickhouseSourceSplit.getSqlOffset() != 0) {
            log.debug("Sorting key is empty, the query will be only execute once.");
            return false;
        }

        try {
            int batchSize = clickhouseSourceTable.getBatchSize();
            rowBatch =
                    proxy.queryDataFromSql(
                            splitQuery,
                            rowTypeInfo,
                            clickhouseSourceTable.getClickhouseTable(),
                            batchSize,
                            clickhouseSourceSplit.getSqlOffset());

            clickhouseSourceSplit.setSqlOffset(
                    clickhouseSourceSplit.getSqlOffset() + rowBatch.size());

            return !rowBatch.isEmpty();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                    String.format(
                            "Failed to read data from sql %s, shard: %s, splitId %s, message: %s",
                            splitQuery,
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

    private class StreamValueReader implements Serializable {
        private static final long serialVersionUID = -7037116446966849773L;

        private final ClickHouseResponse clickHouseResponse;

        public StreamValueReader(ClickhouseProxy proxy, String sql) {
            try {
                clickHouseResponse = proxy.getClickhouseConnection().query(sql).executeAndWait();
            } catch (ClickHouseException e) {
                throw new ClickhouseConnectorException(
                        ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                        String.format("Failed to execute query: %s", sql),
                        e);
            }
        }

        public boolean hasNext() {
            Iterator<ClickHouseRecord> recordIterator = clickHouseResponse.records().iterator();

            if (recordIterator.hasNext()) {
                SeaTunnelRow seaTunnelRow =
                        ClickhouseUtil.convertToSeaTunnelRow(
                                recordIterator.next(),
                                rowTypeInfo,
                                clickhouseSourceTable.getTablePath().getFullName());

                rowBatch = Collections.singletonList(seaTunnelRow);
                return true;
            }

            return false;
        }

        public void close() {
            if (clickHouseResponse != null) {
                clickHouseResponse.close();
            }
        }
    }
}
