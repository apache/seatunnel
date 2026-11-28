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

package org.apache.seatunnel.connectors.seatunnel.udp;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Collections;
import java.util.List;

/**
 * Exposes independently assigned UDP listener ports through the existing Source and state APIs.
 *
 * <p>A positive inactivity setting permits batch completion but does not make network delivery
 * reliable.
 */
public final class UdpSource
        implements SeaTunnelSource<SeaTunnelRow, UdpSplit, UdpEnumerator.State>,
                SupportParallelism {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "UDP";

    private final UdpConfig config;
    private final CatalogTable table;

    UdpSource(UdpConfig config) {
        this.config = config;
        table = CatalogTableUtil.getCatalogTable(IDENTIFIER, config.format.schema);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return config.idleMs == 0 ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.BATCH.equals(context.getJobMode()) && config.idleMs == 0) {
            throw UdpConfig.invalid("BATCH requires a positive idle_timeout_ms");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, UdpSplit> createReader(SourceReader.Context context) {
        return new UdpReader(config, context, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<UdpSplit, UdpEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<UdpSplit> context) {
        return new UdpEnumerator(context, config.ports, null);
    }

    @Override
    public SourceSplitEnumerator<UdpSplit, UdpEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<UdpSplit> context, UdpEnumerator.State state) {
        return new UdpEnumerator(context, config.ports, state);
    }
}
