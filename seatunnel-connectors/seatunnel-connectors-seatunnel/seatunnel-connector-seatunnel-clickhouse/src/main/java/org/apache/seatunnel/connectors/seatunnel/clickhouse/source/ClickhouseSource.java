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
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.Config.*;

public class ClickhouseSource implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit, ClickhouseSourceState> {

    private ClickHouseNode server;
    private SeaTunnelRowTypeInfo rowTypeInfo;

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(config, NODE_ADDRESS, DATABASE, SQL);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        String[] nodeAndPort = config.getString(NODE_ADDRESS).split(":", 2);
        server = ClickHouseNode.of(nodeAndPort[0], ClickHouseProtocol.HTTP,
                Integer.parseInt(nodeAndPort[1]), config.getString(DATABASE));
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
             ClickHouseResponse response =
                     client.connect(server).format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                             .query(modifySQLToLimit1(config.getString(SQL))).executeAndWait()) {

            for (int i = 0; i < response.getColumns().size(); i++) {

            }
        } catch (ClickHouseException e) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, e.getMessage());
        }

    }

    private String modifySQLToLimit1(String sql) {
        return String.format("SELECT * FROM (%s) s LIMIT 1", sql);
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return null;
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new ClickhouseSourceReader();
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> createEnumerator(SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext) throws Exception {
        return new ClickhouseSourceSplitEnumerator();
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> restoreEnumerator(SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext, ClickhouseSourceState checkpointState) throws Exception {
        return new ClickhouseSourceSplitEnumerator();
    }

    @Override
    public Serializer<ClickhouseSourceState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
