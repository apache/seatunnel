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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.split.ClickhouseSourceSplit;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class ClickhouseSourceReader implements SourceReader<SeaTunnelRow, ClickhouseSourceSplit> {

    private final Map<TablePath, List<ClickHouseNode>> servers;
    private ClickHouseClient client;
    private final Context context;
    private volatile boolean noMoreSplits;
    private final Queue<ClickhouseSourceSplit> splitQueue;
    private final Map<TablePath, ClickhouseSourceTable> tables;

    ClickhouseSourceReader(
            Map<TablePath, List<ClickHouseNode>> servers,
            Context readerContext,
            Map<TablePath, ClickhouseSourceTable> tables) {
        this.servers = servers;
        this.context = readerContext;
        this.splitQueue = new ArrayDeque<>();
        this.tables = tables;
    }

    @Override
    public void open() {}

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            ClickhouseSourceSplit split = splitQueue.poll();
            if (split != null) {
                ClickhouseValueReader clickhouseValueReader = null;
                try {
                    ClickhouseSourceTable clickhouseSourceTable =
                            tables.get(split.getConfigTablePath());
                    if (clickhouseSourceTable == null) {
                        throw new ClickhouseConnectorException(
                                ClickhouseConnectorErrorCode.TABLE_NOT_FOUND_ERROR,
                                String.format(
                                        "Table %s.%s not found in table list of job configuration.",
                                        split.getConfigTablePath().getDatabaseName(),
                                        split.getConfigTablePath().getTableName()));
                    }

                    CatalogTable catalogTable = clickhouseSourceTable.getCatalogTable();

                    clickhouseValueReader =
                            new ClickhouseValueReader(
                                    split,
                                    catalogTable.getSeaTunnelRowType(),
                                    clickhouseSourceTable);
                    while (clickhouseValueReader.hasNext()) {
                        List<SeaTunnelRow> next = clickhouseValueReader.next();
                        next.forEach(output::collect);
                    }
                } finally {
                    if (clickhouseValueReader != null) {
                        clickhouseValueReader.close();
                    }
                }
            } else if (noMoreSplits && splitQueue.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                signalNoMoreElement();
            }
        }
    }

    @Override
    public List<ClickhouseSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splitQueue);
    }

    @Override
    public void addSplits(List<ClickhouseSourceSplit> splits) {
        this.splitQueue.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplits = true;
    }

    private void signalNoMoreElement() {
        log.info("Closed the bounded ClickHouse source");
        this.context.signalNoMoreElement();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
