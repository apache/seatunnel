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

package org.apache.seatunnel.connectors.seatunnel.fake.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.exception.FakeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.Collections;

@AutoService(SeaTunnelSource.class)
public class FakeSource implements SeaTunnelSource<SeaTunnelRow, FakeSourceSplit, FakeSourceState> {

    private JobContext jobContext;
    private SeaTunnelSchema schema;
    private FakeConfig fakeConfig;

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return schema.getSeaTunnelRowType();
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> createEnumerator(SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext) throws Exception {
        return new FakeSourceSplitEnumerator(enumeratorContext, fakeConfig, Collections.emptySet());
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> restoreEnumerator(SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext, FakeSourceState checkpointState) throws Exception {
        return new FakeSourceSplitEnumerator(enumeratorContext, fakeConfig, checkpointState.getAssignedSplits());
    }

    @Override
    public SourceReader<SeaTunnelRow, FakeSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new FakeSourceReader(readerContext, schema, fakeConfig);
    }

    @Override
    public String getPluginName() {
        return "FakeSource";
    }

    @Override
    public void prepare(Config pluginConfig) {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, SeaTunnelSchema.SCHEMA.key());
        if (!result.isSuccess()) {
            throw new FakeConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.schema = SeaTunnelSchema.buildWithConfig(pluginConfig.getConfig(SeaTunnelSchema.SCHEMA.key()));
        this.fakeConfig = FakeConfig.buildWithConfig(pluginConfig);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
