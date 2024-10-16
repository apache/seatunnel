/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSinkState;

import java.io.IOException;
import java.util.Collections;

public class SlsSink
        implements SeaTunnelSink<
                SeaTunnelRow, SlsSinkState, SlsCommitInfo, SlsAggregatedCommitInfo> {
    private final ReadonlyConfig pluginConfig;
    private final SeaTunnelRowType seaTunnelRowType;

    public SlsSink(ReadonlyConfig pluginConfig, SeaTunnelRowType rowType) {
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = rowType;
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.sls.config.Config.CONNECTOR_IDENTITY;
    }

    @Override
    public SinkWriter<SeaTunnelRow, SlsCommitInfo, SlsSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new SlsSinkWriter(context, seaTunnelRowType, pluginConfig, Collections.emptyList());
    }
}
