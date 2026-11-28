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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import java.util.Collections;
import java.util.List;

/** An OPC UA client source for values and address-space metadata. */
public final class OpcUaSource
        implements SeaTunnelSource<SeaTunnelRow, OpcUaSplit, OpcUaEnumerator.State> {
    static final String IDENTIFIER = "OpcUa";
    private static final long serialVersionUID = 1L;
    private final OpcUaSettings settings;
    private final CatalogTable table;

    OpcUaSource(OpcUaSettings settings) {
        this.settings = settings;
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of("opcua")),
                        OpcUaRows.schema(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "OPC UA events and nodes");
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return settings.bounded() ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public void setJobContext(JobContext context) {
        if (JobMode.BATCH.equals(context.getJobMode()) && !settings.bounded()) {
            throw OpcUaSettings.invalid("BATCH value reading requires positive max_records");
        }
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, OpcUaSplit> createReader(SourceReader.Context context) {
        return new OpcUaReader(context, settings, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<OpcUaSplit, OpcUaEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<OpcUaSplit> context) {
        return new OpcUaEnumerator(context, settings.stateKey, null);
    }

    @Override
    public SourceSplitEnumerator<OpcUaSplit, OpcUaEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<OpcUaSplit> context, OpcUaEnumerator.State state) {
        return new OpcUaEnumerator(context, settings.stateKey, state);
    }
}
