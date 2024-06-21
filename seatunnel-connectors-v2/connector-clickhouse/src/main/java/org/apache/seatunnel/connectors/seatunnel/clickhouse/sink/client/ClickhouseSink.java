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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ClickhouseSink
        implements SeaTunnelSink<SeaTunnelRow, ClickhouseSinkState, CKCommitInfo, CKAggCommitInfo> {

    private ReaderOption option;

    public ClickhouseSink(ReaderOption option) {
        this.option = option;
    }

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new ClickhouseSinkWriter(option, context);
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> restoreWriter(
            SinkWriter.Context context, List<ClickhouseSinkState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<ClickhouseSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}
