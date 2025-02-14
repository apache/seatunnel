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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.iotdb.config.SourceConfig.NODE_URLS;

@AutoService(SeaTunnelSource.class)
public class IoTDBSource
        implements SeaTunnelSource<SeaTunnelRow, IoTDBSourceSplit, IoTDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private JobContext jobContext;

    private SeaTunnelRowType typeInfo;

    private final Map<String, Object> configParams = new HashMap<>();

    @Override
    public String getPluginName() {
        return "IoTDB";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult urlCheckResult = CheckConfigUtil.checkAllExists(pluginConfig, NODE_URLS.key());
        CheckResult schemaCheckResult =
                CheckConfigUtil.checkAllExists(pluginConfig, ConnectorCommonOptions.SCHEMA.key());
        CheckResult mergedConfigCheck =
                CheckConfigUtil.mergeCheckResults(urlCheckResult, schemaCheckResult);
        if (!mergedConfigCheck.isSuccess()) {
            throw new IotdbConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, mergedConfigCheck.getMsg()));
        }
        this.typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        pluginConfig
                .entrySet()
                .forEach(entry -> configParams.put(entry.getKey(), entry.getValue().unwrapped()));
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, IoTDBSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new IoTDBSourceReader(configParams, readerContext, typeInfo);
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> createEnumerator(
            SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext) throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, configParams);
    }

    @Override
    public SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<IoTDBSourceSplit> enumeratorContext,
            IoTDBSourceState checkpointState)
            throws Exception {
        return new IoTDBSourceSplitEnumerator(enumeratorContext, configParams, checkpointState);
    }
}
