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

package org.apache.seatunnel.connectors.seatunnel.fts.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.fts.exception.FlinkTableStoreConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fts.utils.RowTypeConverter;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.Table;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.fts.config.FlinkTableStoreConfig.TABLE_PATH;

@AutoService(SeaTunnelSource.class)
public class FlinkTableStoreSource
        implements SeaTunnelSource<
                SeaTunnelRow, FlinkTableStoreSourceSplit, FlinkTableStoreSourceState> {

    private static final long serialVersionUID = 1L;

    public static final String PLUGIN_NAME = "FlinkTableStore";

    private Config pluginConfig;

    private SeaTunnelRowType seaTunnelRowType;

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
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        // initialize flink table store
        String tablePath = pluginConfig.getString(TABLE_PATH.key());
        table = FileStoreTableFactory.create(new Path(tablePath));

        // initialize seatunnel row type
        // TODO: Support column projection
        seaTunnelRowType = RowTypeConverter.convert(table.rowType());
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, FlinkTableStoreSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new FlinkTableStoreSourceReader(readerContext, table, seaTunnelRowType);
    }

    @Override
    public SourceSplitEnumerator<FlinkTableStoreSourceSplit, FlinkTableStoreSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<FlinkTableStoreSourceSplit> enumeratorContext)
                    throws Exception {
        return new FlinkTableStoreSourceSplitEnumerator(enumeratorContext, table);
    }

    @Override
    public SourceSplitEnumerator<FlinkTableStoreSourceSplit, FlinkTableStoreSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<FlinkTableStoreSourceSplit> enumeratorContext,
                    FlinkTableStoreSourceState checkpointState)
                    throws Exception {
        return new FlinkTableStoreSourceSplitEnumerator(enumeratorContext, table, checkpointState);
    }
}
