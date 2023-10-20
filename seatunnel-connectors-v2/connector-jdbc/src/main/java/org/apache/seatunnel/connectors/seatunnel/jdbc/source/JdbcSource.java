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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSourceState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

@AutoService(SeaTunnelSource.class)
@NoArgsConstructor
public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private JdbcSourceConfig jdbcSourceConfig;

    private JdbcInputFormat inputFormat;
    private PartitionParameter partitionParameter;

    public JdbcSource(
            JdbcSourceConfig jdbcSourceConfig,
            JdbcInputFormat inputFormat,
            PartitionParameter partitionParameter) {
        this.jdbcSourceConfig = jdbcSourceConfig;
        this.inputFormat = inputFormat;
        this.partitionParameter = partitionParameter;
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, JdbcSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new JdbcSourceReader(inputFormat, readerContext);
    }

    @Override
    public Serializer<JdbcSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext) {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter);
    }

    @Override
    public SourceSplitEnumerator<JdbcSourceSplit, JdbcSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JdbcSourceSplit> enumeratorContext,
            JdbcSourceState checkpointState) {
        return new JdbcSourceSplitEnumerator(
                enumeratorContext, jdbcSourceConfig, partitionParameter, checkpointState);
    }
}
