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

package org.apache.seatunnel.connectors.seatunnel.activemq.source;

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
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.Split;
import org.apache.seatunnel.connectors.seatunnel.activemq.source.ActivemqSourceEnumerator.State;

import java.util.Collections;
import java.util.List;

/** Competing consumers of one queue; the broker retains messages until checkpoint completion. */
public class ActivemqSource
        implements SeaTunnelSource<SeaTunnelRow, Split, State>, SupportParallelism {
    private final ReadonlyConfig config;
    private final CatalogTable table;
    private final DeserializationSchema<SeaTunnelRow> deserializer;
    private JobContext jobContext;

    public ActivemqSource(
            ReadonlyConfig config,
            CatalogTable table,
            DeserializationSchema<SeaTunnelRow> deserializer) {
        ActivemqSourceFactory.validate(config);
        this.config = config;
        this.table = table;
        this.deserializer = deserializer;
    }

    @Override
    public String getPluginName() {
        return "ActiveMQ";
    }

    @Override
    public Boundedness getBoundedness() {
        if (jobContext != null
                && (jobContext.getJobMode() != JobMode.STREAMING
                        || !jobContext.isEnableCheckpoint())) {
            throw new IllegalArgumentException(
                    "ActiveMQ source requires a streaming job with checkpointing enabled");
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
    public SourceReader<SeaTunnelRow, Split> createReader(SourceReader.Context context) {
        getBoundedness();
        return new ActivemqSourceReader(config, deserializer);
    }

    @Override
    public SourceSplitEnumerator<Split, State> createEnumerator(
            SourceSplitEnumerator.Context<Split> context) {
        return new ActivemqSourceEnumerator(context);
    }

    @Override
    public SourceSplitEnumerator<Split, State> restoreEnumerator(
            SourceSplitEnumerator.Context<Split> context, State state) {
        return new ActivemqSourceEnumerator(context, state);
    }
}
