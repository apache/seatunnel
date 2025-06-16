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
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseProxy;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ClickhouseValueReader implements Serializable {

    private static final long serialVersionUID = 4588012013447713463L;

    private final ClickhouseSourceSplit clickhouseSourceSplit;
    private final SeaTunnelRowType rowTypeInfo;
    private final ClickhouseSourceTable clickhouseSourceTable;
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

        Shard shard = clickhouseSourceSplit.getShard();
        this.proxy = new ClickhouseProxy(shard.getNode());
    }

    public boolean hasNext() {
        boolean hasNext = false;
        List<ClickhousePart> parts = new ArrayList<>(clickhouseSourceSplit.getParts());
        int partSize = parts.size();
        int batchSize = clickhouseSourceTable.getBatchSize();

        try {
            if (currentPartIndex < partSize) {
                ClickhousePart currentPart = parts.get(currentPartIndex);

                log.debug(
                        "partName: {}, offset: {}, partSize: {}, currentPartIndex: {}",
                        currentPart.getName(),
                        currentPart.getOffset(),
                        partSize,
                        currentPartIndex);

                if (currentPart.isEos()) {
                    currentPartIndex++;
                    if (currentPartIndex >= partSize) {
                        return hasNext;
                    }
                    return hasNext();
                }

                hasNext = true;
                // read data in batch
                rowBatch =
                        proxy.getDataFromSplit(
                                currentPart,
                                rowTypeInfo,
                                clickhouseSourceTable,
                                currentPart.getOffset());

                // 设置表ID
                for (SeaTunnelRow row : rowBatch) {
                    row.setTableId(clickhouseSourceTable.getTablePath().toString());
                }

                if (rowBatch.isEmpty()) {
                    currentPart.setEos(true);
                    currentPartIndex++;
                    if (currentPartIndex < partSize) {
                        return hasNext();
                    }
                } else {
                    // update part offset
                    currentPart.setOffset(currentPart.getOffset() + rowBatch.size());
                    if (rowBatch.size() < batchSize) {
                        currentPart.setEos(true);
                        currentPartIndex++;
                    }
                }
            }
        } catch (Exception e) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.QUERY_WITH_PART_ERROR,
                    "Failed to read data from clickhouse split: "
                            + clickhouseSourceSplit.getSplitId());
        }

        return hasNext;
    }

    public List<SeaTunnelRow> next() {
        if (rowBatch == null) {
            throw new ClickhouseConnectorException(
                    ClickhouseConnectorErrorCode.SHOULD_NEVER_HAPPEN, "never happen error !");
        }

        return rowBatch;
    }

    public void close() {
        if (proxy != null) {
            proxy.close();
        }
    }
}
