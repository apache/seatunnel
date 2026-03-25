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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.List;

/** RocketMq source */
public class RocketMqSource
        implements SeaTunnelSource<SeaTunnelRow, RocketMqSourceSplit, RocketMqSourceState>,
                SupportParallelism {

    private final RocketMqSourceConfig sourceConfig;
    private JobContext jobContext;

    public RocketMqSource(ReadonlyConfig pluginConfig) {
        this.sourceConfig = new RocketMqSourceConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Rocketmq";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return sourceConfig.getCatalogTables();
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SourceReader<SeaTunnelRow, RocketMqSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new RocketMqSourceReader(
                sourceConfig.getMetadata(), sourceConfig.getTopicConfigs(), readerContext);
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> createEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context) throws Exception {
        return new RocketMqSourceSplitEnumerator(
                sourceConfig.getMetadata(),
                sourceConfig.getTopicConfigs(),
                context,
                sourceConfig.getDiscoveryIntervalMillis());
    }

    @Override
    public SourceSplitEnumerator<RocketMqSourceSplit, RocketMqSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<RocketMqSourceSplit> context,
            RocketMqSourceState sourceState)
            throws Exception {
        return new RocketMqSourceSplitEnumerator(
                sourceConfig.getMetadata(),
                sourceConfig.getTopicConfigs(),
                context,
                sourceConfig.getDiscoveryIntervalMillis());
    }
}
