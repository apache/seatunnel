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

package org.apache.seatunnel.connectors.seatunnel.fts.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class FlinkTableStoreSource
        implements SeaTunnelSource<
                SeaTunnelRow, FlinkTableStoreSourceSplit, FlinkTableStoreSourceState> {

    public static final String PLUGIN_NAME = "FlinkTableStore";

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {}

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }

    @Override
    public SourceReader<SeaTunnelRow, FlinkTableStoreSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<FlinkTableStoreSourceSplit, FlinkTableStoreSourceState>
            createEnumerator(
                    SourceSplitEnumerator.Context<FlinkTableStoreSourceSplit> enumeratorContext)
                    throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<FlinkTableStoreSourceSplit, FlinkTableStoreSourceState>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<FlinkTableStoreSourceSplit> enumeratorContext,
                    FlinkTableStoreSourceState checkpointState)
                    throws Exception {
        return null;
    }
}
