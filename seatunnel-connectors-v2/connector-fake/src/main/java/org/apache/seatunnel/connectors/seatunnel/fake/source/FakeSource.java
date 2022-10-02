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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.fake.config.FakeConfig;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeSourceState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

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
        return new FakeSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeSourceState> restoreEnumerator(SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext, FakeSourceState checkpointState) throws Exception {
        return new FakeSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceReader<SeaTunnelRow, FakeSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new FakeSourceReader(readerContext, new FakeDataGenerator(schema, fakeConfig));
    }

    @Override
    public String getPluginName() {
        return "FakeSource";
    }

    @Override
    public void prepare(Config pluginConfig) {
        assert pluginConfig.hasPath(FakeDataGenerator.SCHEMA);
        this.schema = SeaTunnelSchema.buildWithConfig(pluginConfig.getConfig(FakeDataGenerator.SCHEMA));
        this.fakeConfig = FakeConfig.buildWithConfig(pluginConfig);
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }
}
