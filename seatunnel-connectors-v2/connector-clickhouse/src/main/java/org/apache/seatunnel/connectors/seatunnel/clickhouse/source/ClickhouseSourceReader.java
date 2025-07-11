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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseFormat;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Random;

@Slf4j
public class ClickhouseSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final List<ClickHouseNode> servers;
    private ClickHouseClient client;
    private final SeaTunnelRowType rowTypeInfo;
    private final SingleSplitReaderContext readerContext;
    private ClickHouseRequest<?> request;
    private final String sql;

    ClickhouseSourceReader(
            List<ClickHouseNode> servers,
            SingleSplitReaderContext readerContext,
            String sql,
            SeaTunnelRowType rowTypeInfo) {
        this.servers = servers;
        this.readerContext = readerContext;
        this.sql = sql;
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public void open() {
        Random random = new Random();
        ClickHouseNode server = servers.get(random.nextInt(servers.size()));
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
        synchronized (output.getCheckpointLock()) {
            try (ClickHouseResponse response = this.request.query(sql).executeAndWait()) {
                response.stream()
                        .forEach(
                                record -> {
                                    Object[] values =
                                            new Object[this.rowTypeInfo.getFieldNames().length];
                                    for (int i = 0; i < record.size(); i++) {
                                        if (record.getValue(i).isNullOrEmpty()) {
                                            values[i] = null;
                                        } else {
                                            values[i] =
                                                    TypeConvertUtil.valueUnwrap(
                                                            this.rowTypeInfo.getFieldType(i),
                                                            record.getValue(i));
                                        }
                                    }
                                    output.collect(new SeaTunnelRow(values));
                                });
            }
            signalNoMoreElement();
        }
    }

    private void signalNoMoreElement() {
        log.info("Closed the bounded ClickHouse source");
        this.readerContext.signalNoMoreElement();
    }
}
