/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.constant.HbaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.hbase.exception.HbaseConnectorException;

import com.google.common.collect.Lists;

import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseConfig.ZOOKEEPER_QUORUM;

public class HbaseSource
        implements SeaTunnelSource<SeaTunnelRow, HbaseSourceSplit, HbaseSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private SeaTunnelRowType seaTunnelRowType;
    private HbaseParameters hbaseParameters;

    private CatalogTable catalogTable;

    @Override
    public String getPluginName() {
        return HbaseIdentifier.IDENTIFIER_NAME;
    }

    HbaseSource(Config pluginConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(pluginConfig, ZOOKEEPER_QUORUM.key(), TABLE.key());
        if (!result.isSuccess()) {
            throw new HbaseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.hbaseParameters = HbaseParameters.buildWithSourceConfig(pluginConfig);
        this.catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(catalogTable);
    }

    @Override
    public SourceReader<SeaTunnelRow, HbaseSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new HbaseSourceReader(hbaseParameters, readerContext, seaTunnelRowType);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> createEnumerator(
            SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext) throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext, hbaseParameters);
    }

    @Override
    public SourceSplitEnumerator<HbaseSourceSplit, HbaseSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<HbaseSourceSplit> enumeratorContext,
            HbaseSourceState checkpointState)
            throws Exception {
        return new HbaseSourceSplitEnumerator(enumeratorContext, hbaseParameters, checkpointState);
    }
}
