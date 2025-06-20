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

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;

@Slf4j
public class ClickhouseValueReader implements Serializable {

    private static final long serialVersionUID = 4588012013447713463L;

    private final ClickhouseSourceSplit clickhouseSourceSplit;
    private final SeaTunnelRowType rowTypeInfo;
    private final ClickhouseSourceTable clickhouseSourceTable;
    private ClickhouseProxy proxy;

    protected int currentPartIndex = 0;
    protected int sqlOffset = 0;

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
        if (clickhouseSourceTable.isSqlStrategyRead()) {
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
                            "Failed to read data from part %s.  shard: %s, splitId: %s",
                            currentPart.getName(),
                            currentPart.getShard().getNode(),
                            clickhouseSourceSplit.getSplitId()),
                    e);
        }
    }

    private boolean sqlStrategyRead() {
        String splitQuery = clickhouseSourceSplit.getSplitQuery();
        log.info("Sql strategy read split query: {}", splitQuery);

        try {
            int batchSize = clickhouseSourceTable.getBatchSize();
            rowBatch =
                    proxy.queryDataFromSql(
                            splitQuery,
                            rowTypeInfo,
                            clickhouseSourceTable.getClickhouseTable(),
                            batchSize,
                            sqlOffset);

            sqlOffset += rowBatch.size();

            return !rowBatch.isEmpty();
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_DATA_ERROR,
                    String.format(
                            "Failed to read data from sql %s, splitId %s ",
                            splitQuery, clickhouseSourceSplit.getSplitId()));
        }
    }

    public void close() {
        if (proxy != null) {
            proxy.close();
        }
    }
}
