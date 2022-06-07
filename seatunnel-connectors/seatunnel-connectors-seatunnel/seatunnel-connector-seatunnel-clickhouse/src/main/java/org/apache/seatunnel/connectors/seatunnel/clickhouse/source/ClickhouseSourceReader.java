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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClickhouseSourceReader implements SourceReader<SeaTunnelRow, ClickhouseSourceSplit> {

    private final ClickHouseNode server;
    private ClickHouseClient client;
    private SeaTunnelRowTypeInfo rowTypeInfo;
    private SourceReader.Context readerContext;
    private ClickHouseRequest request;
    private String sql;

    private final List<ClickhouseSourceSplit> splits;

    ClickhouseSourceReader(ClickHouseNode server, SourceReader.Context readerContext,
                           SeaTunnelRowTypeInfo rowTypeInfo, String sql) {
        this.server = server;
        this.readerContext = readerContext;
        this.rowTypeInfo = rowTypeInfo;
        this.sql = sql;
        this.splits = new ArrayList<>();
    }

    @Override
    public void open() {
        client = ClickHouseClient.newInstance(server.getProtocol());
        request = client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (!splits.isEmpty()) {
            try (ClickHouseResponse response = this.request.query(sql).executeAndWait()) {
                response.stream().forEach(record -> {
                    Object[] values = new Object[this.rowTypeInfo.getFieldNames().length];
                    for (int i = 0; i < record.size(); i++) {
                        values[i] = TypeConvertUtil.valueUnwrap(
                                this.rowTypeInfo.getSeaTunnelDataTypes()[i], record.getValue(i));
                    }
                    output.collect(new SeaTunnelRow(values));
                });
            }
            this.readerContext.signalNoMoreElement();
        }
    }

    @Override
    public List<ClickhouseSourceSplit> snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void addSplits(List<ClickhouseSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
