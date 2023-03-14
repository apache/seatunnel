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

package org.apache.seatunnel.connectors.seatunnel.fts.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.commit.FlinkTableStoreAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.commit.FlinkTableStoreAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.commit.FlinkTableStoreCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.fts.sink.state.FlinkTableStoreState;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.Table;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.fts.config.FlinkTableStoreConfig.TABLE_PATH;

@AutoService(SeaTunnelSink.class)
public class FlinkTableStoreSink
        implements SeaTunnelSink<
                SeaTunnelRow,
                FlinkTableStoreState,
                FlinkTableStoreCommitInfo,
                FlinkTableStoreAggregatedCommitInfo> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "FlinkTableStore";

    private SeaTunnelRowType seaTunnelRowType;

    private Config pluginConfig;

    private Table table;

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, TABLE_PATH.key());
        if (!result.isSuccess()) {
            throw new FlinkTableStoreConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        // initialize flink table store
        String tablePath = pluginConfig.getString(TABLE_PATH.key());
        table = FileStoreTableFactory.create(new Path(tablePath));
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, FlinkTableStoreCommitInfo, FlinkTableStoreState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new FlinkTableStoreSinkWriter(context, table, seaTunnelRowType);
    }

    @Override
    public Optional<
                    SinkAggregatedCommitter<
                            FlinkTableStoreCommitInfo, FlinkTableStoreAggregatedCommitInfo>>
            createAggregatedCommitter() throws IOException {
        return Optional.of(new FlinkTableStoreAggregatedCommitter(table, "SeaTunnel"));
    }

    @Override
    public SinkWriter<SeaTunnelRow, FlinkTableStoreCommitInfo, FlinkTableStoreState> restoreWriter(
            SinkWriter.Context context, List<FlinkTableStoreState> states) throws IOException {
        return new FlinkTableStoreSinkWriter(context, table, seaTunnelRowType, states);
    }
}
