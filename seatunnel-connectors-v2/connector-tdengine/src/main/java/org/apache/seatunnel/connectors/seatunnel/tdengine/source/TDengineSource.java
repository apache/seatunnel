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

package org.apache.seatunnel.connectors.seatunnel.tdengine.source;

import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.LOWER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.PARTITIONS_NUM;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.STABLE;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.UPPER_BOUND;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.URL;
import static org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig.ConfigNames.USERNAME;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceReader.Context;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.tdengine.config.TDengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.tdengine.state.TDengineSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.SneakyThrows;

@AutoService(SeaTunnelSource.class)
public class TDengineSource implements SeaTunnelSource<SeaTunnelRow, TDengineSourceSplit, TDengineSourceState> {

    private SeaTunnelContext seaTunnelContext;

    private SeaTunnelRowType seaTunnelRowType;

    private TDengineSourceConfig tdengineSourceConfig;

    @Override
    public String getPluginName() {
        return "TDengine";
    }

    @SneakyThrows
    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, URL, DATABASE, STABLE, USERNAME, PASSWORD);
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "TDengine connection require url/database/stable/username/password. All of there must not be empty.");
        }

        tdengineSourceConfig = buildSourceConfig(pluginConfig);
        SeaTunnelSchema seatunnelSchema = SeaTunnelSchema.buildWithConfig(pluginConfig);
        this.seaTunnelRowType = seatunnelSchema.getSeaTunnelRowType();

    }

    private TDengineSourceConfig buildSourceConfig(Config pluginConfig) {
        TDengineSourceConfig tdengineSourceConfig = new TDengineSourceConfig();
        tdengineSourceConfig.setUrl(pluginConfig.getString(URL));
        tdengineSourceConfig.setDatabase(pluginConfig.getString(DATABASE));
        tdengineSourceConfig.setStable(pluginConfig.getString(STABLE));
        tdengineSourceConfig.setUsername(pluginConfig.getString(USERNAME));
        tdengineSourceConfig.setPassword(pluginConfig.getString(PASSWORD));
        tdengineSourceConfig.setUpperBound(pluginConfig.getLong(UPPER_BOUND));
        tdengineSourceConfig.setLowerBound(pluginConfig.getLong(LOWER_BOUND));
        tdengineSourceConfig.setPartitionsNum(pluginConfig.getInt(PARTITIONS_NUM));
        return tdengineSourceConfig;
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
    public SourceReader<SeaTunnelRow, TDengineSourceSplit> createReader(Context readerContext) {
        return new TDengineSourceReader(tdengineSourceConfig, readerContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> createEnumerator(SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext) {
        return new TDengineSourceSplitEnumerator(tdengineSourceConfig, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<TDengineSourceSplit, TDengineSourceState> restoreEnumerator(SourceSplitEnumerator.Context<TDengineSourceSplit> enumeratorContext,
        TDengineSourceState checkpointState) {
        return new TDengineSourceSplitEnumerator(tdengineSourceConfig, checkpointState, enumeratorContext);
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

}
