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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.CommonConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.CommonConfig.NODE_URLS;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.CommonConfig.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.CommonConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.starrocks.config.CommonConfig.USERNAME;

@AutoService(SeaTunnelSource.class)
public class StarRocksSource
        implements SeaTunnelSource<SeaTunnelRow, StarRocksSourceSplit, StarRocksSourceState> {

    private SeaTunnelRowType typeInfo;
    private SourceConfig sourceConfig;

    @Override
    public String getPluginName() {
        return "StarRocks";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        NODE_URLS.key(),
                        DATABASE.key(),
                        TABLE.key(),
                        USERNAME.key(),
                        PASSWORD.key());

        CheckResult schemaCheckResult =
                CheckConfigUtil.checkAllExists(pluginConfig, TableSchemaOptions.SCHEMA.key());
        CheckResult mergedConfigCheck =
                CheckConfigUtil.mergeCheckResults(checkResult, schemaCheckResult);
        if (!mergedConfigCheck.isSuccess()) {
            throw new StarRocksConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, mergedConfigCheck.getMsg()));
        }

        this.typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        this.sourceConfig = SourceConfig.loadConfig(pluginConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return typeInfo;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) {
        return new StarRocksSourceReader(readerContext, typeInfo, sourceConfig);
    }

    @Override
    public SourceSplitEnumerator<StarRocksSourceSplit, StarRocksSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<StarRocksSourceSplit> enumeratorContext,
            StarRocksSourceState checkpointState)
            throws Exception {
        return new StartRocksSourceSplitEnumerator(
                enumeratorContext, sourceConfig, typeInfo, checkpointState);
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) {
        return new StartRocksSourceSplitEnumerator(enumeratorContext, sourceConfig, typeInfo);
    }
}
