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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class MaxcomputeSource implements SeaTunnelSource<SeaTunnelRow, MaxcomputeSourceSplit, MaxcomputeSourceState> {
    private SeaTunnelRowType typeInfo;
    private Config pluginConfig;

    @Override
    public String getPluginName() {
        return "Maxcompute";
    }

    @Override
    public void prepare(Config pluginConfig) {
        this.typeInfo = MaxcomputeTypeMapper.getSeaTunnelRowType(pluginConfig);
        this.pluginConfig = pluginConfig;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, MaxcomputeSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new MaxcomputeSourceReader(this.pluginConfig, readerContext, this.typeInfo);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> createEnumerator(SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext) throws Exception {
        return new MaxcomputeSourceSplitEnumerator(enumeratorContext, this.pluginConfig);
    }

    @Override
    public SourceSplitEnumerator<MaxcomputeSourceSplit, MaxcomputeSourceState> restoreEnumerator(SourceSplitEnumerator.Context<MaxcomputeSourceSplit> enumeratorContext, MaxcomputeSourceState checkpointState) throws Exception {
        return new MaxcomputeSourceSplitEnumerator(enumeratorContext, this.pluginConfig, checkpointState);
    }
}
