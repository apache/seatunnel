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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSourceState;

import com.clickhouse.client.ClickHouseNode;

import java.util.List;

public class ClickhouseSource
        implements SeaTunnelSource<SeaTunnelRow, ClickhouseSourceSplit, ClickhouseSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private List<ClickHouseNode> servers;
    private SeaTunnelRowType rowTypeInfo;
    private String sql;

    public ClickhouseSource(
            List<ClickHouseNode> servers, SeaTunnelRowType rowTypeInfo, String sql) {
        this.servers = servers;
        this.rowTypeInfo = rowTypeInfo;
        this.sql = sql;
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, ClickhouseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new ClickhouseSourceReader(servers, readerContext, this.rowTypeInfo, sql);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<ClickhouseSourceSplit, ClickhouseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<ClickhouseSourceSplit> enumeratorContext,
            ClickhouseSourceState checkpointState)
            throws Exception {
        return new ClickhouseSourceSplitEnumerator(enumeratorContext);
    }
}
