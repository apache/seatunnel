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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.console.state.ConsoleState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(SeaTunnelSink.class)
public class ConsoleSink implements SeaTunnelSink<SeaTunnelRow, ConsoleState, ConsoleCommitInfo, ConsoleAggregatedCommitInfo> {

    private Config pluginConfig;
    private SeaTunnelContext seaTunnelContext;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, ConsoleCommitInfo, ConsoleState> createWriter(SinkWriter.Context context) {
        return new ConsoleSinkWriter(seaTunnelRowType);
    }

    @Override
    public SinkWriter<SeaTunnelRow, ConsoleCommitInfo, ConsoleState> restoreWriter(
        SinkWriter.Context context, List<ConsoleState> states) {
        return restoreWriter(context, states);
    }

    @Override
    public String getPluginName() {
        return "Console";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }
}
