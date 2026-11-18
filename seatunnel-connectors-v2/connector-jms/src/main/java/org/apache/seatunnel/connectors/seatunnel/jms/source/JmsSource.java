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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Collections;
import java.util.List;

/**
 * Streaming source for one JMS queue. Parallel readers are competing consumers; the broker keeps
 * each message until the checkpoint that covers it completes.
 */
public class JmsSource
        implements SeaTunnelSource<SeaTunnelRow, JmsSourceSplit, JmsSourceState>,
                SupportParallelism {
    private static final long serialVersionUID = 1L;

    private final ReadonlyConfig config;
    private final CatalogTable table;
    private final DeserializationSchema<SeaTunnelRow> deserializer;
    private JobContext jobContext;

    public JmsSource(
            ReadonlyConfig config,
            CatalogTable table,
            DeserializationSchema<SeaTunnelRow> deserializer) {
        JmsSourceFactory.validate(config);
        this.config = config;
        this.table = table;
        this.deserializer = deserializer;
    }

    @Override
    public String getPluginName() {
        return JmsSourceFactory.IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null
                && (jobContext.getJobMode() != JobMode.STREAMING
                        || !jobContext.isEnableCheckpoint())) {
            throw new IllegalArgumentException(
                    "The Jms source requires a streaming job with checkpointing enabled");
        }
        return Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, JmsSourceSplit> createReader(SourceReader.Context context) {
        getBoundedness();
        return new JmsSourceReader(config, deserializer);
    }

    @Override
    public SourceSplitEnumerator<JmsSourceSplit, JmsSourceState> createEnumerator(
            SourceSplitEnumerator.Context<JmsSourceSplit> context) {
        return new JmsSourceSplitEnumerator(context);
    }

    @Override
    public SourceSplitEnumerator<JmsSourceSplit, JmsSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<JmsSourceSplit> context, JmsSourceState state) {
        return new JmsSourceSplitEnumerator(context, state);
    }
}
