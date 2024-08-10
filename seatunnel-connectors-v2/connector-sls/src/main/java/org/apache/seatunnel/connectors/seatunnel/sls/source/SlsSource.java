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

package org.apache.seatunnel.connectors.seatunnel.sls.source;

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
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSourceState;

import com.google.common.collect.Lists;

import java.util.List;

public class SlsSource
        implements SeaTunnelSource<SeaTunnelRow, SlsSourceSplit, SlsSourceState>,
                SupportParallelism {

    private JobContext jobContext;

    private final SlsSourceConfig slsSourceConfig;

    public SlsSource(ReadonlyConfig readonlyConfig) {
        this.slsSourceConfig = new SlsSourceConfig(readonlyConfig);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, SlsSourceSplit> createReader(SourceReader.Context readContext)
            throws Exception {
        return new SlsSourceReader(slsSourceConfig, readContext);
    }

    @Override
    public SourceSplitEnumerator<SlsSourceSplit, SlsSourceState> createEnumerator(
            SourceSplitEnumerator.Context<SlsSourceSplit> enumeratorContext) throws Exception {
        return new SlsSourceSplitEnumerator(slsSourceConfig, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SlsSourceSplit, SlsSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<SlsSourceSplit> enumeratorContext,
            SlsSourceState checkpointState)
            throws Exception {
        return new SlsSourceSplitEnumerator(slsSourceConfig, enumeratorContext, checkpointState);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(slsSourceConfig.getCatalogTable());
    }

    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.sls.config.Config.CONNECTOR_IDENTITY;
    }
}
