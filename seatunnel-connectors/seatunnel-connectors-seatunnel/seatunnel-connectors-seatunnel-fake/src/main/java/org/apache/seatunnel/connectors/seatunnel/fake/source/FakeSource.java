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

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.fake.state.FakeState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class FakeSource implements SeaTunnelSource<SeaTunnelRow, FakeSourceSplit, FakeState> {

    private Config pluginConfig;
    private Boundedness boundedness;

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return new SeaTunnelRowTypeInfo(
            new String[]{"name", "age", "timestamp"},
            new SeaTunnelDataType<?>[]{BasicType.STRING, BasicType.INTEGER, BasicType.LONG});
    }

    @Override
    public SourceReader<SeaTunnelRow, FakeSourceSplit> createReader(SourceReader.Context readerContext) {
        return new FakeSourceReader(readerContext);
    }

    @Override
    public Serializer<FakeSourceSplit> getSplitSerializer() {
        return new DefaultSerializer<>();
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeState> createEnumerator(
        SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext) {
        return new FakeSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<FakeSourceSplit, FakeState> restoreEnumerator(
        SourceSplitEnumerator.Context<FakeSourceSplit> enumeratorContext, FakeState checkpointState) {
        // todo:
        return null;
    }

    @Override
    public Serializer<FakeState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }

    @Override
    public String getPluginName() {
        return "FakeSource";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.pluginConfig = pluginConfig;
        this.boundedness = JobMode.STREAMING.equals(SeaTunnelContext.getContext().getJobMode()) ?
            Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }
}
