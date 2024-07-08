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

package org.apache.seatunnel.connectors.seatunnel.emqx.source;

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
import org.apache.seatunnel.connectors.seatunnel.emqx.config.Config;
import org.apache.seatunnel.connectors.seatunnel.emqx.state.EmqxSourceState;

import java.util.List;

public class EmqxSource
        implements SeaTunnelSource<SeaTunnelRow, EmqxSourceSplit, EmqxSourceState>,
                SupportParallelism {

    private JobContext jobContext;

    private final EmqxSourceConfig emqxSourceConfig;

    public EmqxSource(ReadonlyConfig readonlyConfig) {
        emqxSourceConfig = new EmqxSourceConfig(readonlyConfig);
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public String getPluginName() {
        return Config.CONNECTOR_IDENTITY;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return emqxSourceConfig.getCatalogTables();
    }

    @Override
    public SourceReader<SeaTunnelRow, EmqxSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new EmqxSourceReader(
                emqxSourceConfig.getMetadata(),
                emqxSourceConfig.getDeserializationSchema(),
                readerContext,
                emqxSourceConfig.getMessageFormatErrorHandleWay(),
                jobContext.getJobId());
    }

    @Override
    public SourceSplitEnumerator<EmqxSourceSplit, EmqxSourceState> createEnumerator(
            SourceSplitEnumerator.Context<EmqxSourceSplit> enumeratorContext) {
        return new EmqxSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<EmqxSourceSplit, EmqxSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<EmqxSourceSplit> enumeratorContext,
            EmqxSourceState checkpointState) {
        return new EmqxSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
