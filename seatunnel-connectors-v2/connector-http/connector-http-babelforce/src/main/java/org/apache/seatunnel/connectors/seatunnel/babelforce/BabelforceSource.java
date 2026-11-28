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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.Map;

/**
 * Bounded read-only source for the Babelforce simple call report.
 *
 * <p>Calls and their delivery references remain one structured payload per provider record.
 */
public final class BabelforceSource
        implements SeaTunnelSource<SeaTunnelRow, BabelforceCheckpoint, BabelforceEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Babelforce";

    private final BabelforceOptions options;
    private final CatalogTable table;

    BabelforceSource(BabelforceOptions options) {
        this.options = options;
        TableSchema columns =
                TableSchema.builder()
                        .column(string("call_id", false, "Original provider call ID"))
                        .column(string("parent_id", true, "Provider parent call ID when present"))
                        .column(string("date_created", false, "UTC creation timestamp"))
                        .column(string("payload", false, "Full call-report JSON object"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, "babelforce", "calls"),
                        columns,
                        Map.of(),
                        List.of(),
                        "Babelforce simple call report");
    }

    private static PhysicalColumn string(String name, boolean nullable, String description) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, nullable, null, description);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return List.of(table);
    }

    /**
     * Creates a reader whose caller metadata is emitted through the single catalog table.
     *
     * <p>The worker opens HTTPS only after receiving the restored or initial split.
     */
    @Override
    public SourceReader<SeaTunnelRow, BabelforceCheckpoint> createReader(
            SourceReader.Context context) {
        return new BabelforceReader(context, options, table.getTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BabelforceCheckpoint, BabelforceEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<BabelforceCheckpoint> context) {
        return new BabelforceEnumerator(context, options, null);
    }

    /**
     * Reuses the original creation window and reader handoff state.
     *
     * <p>Restoration must not choose a newer implicit end date.
     */
    @Override
    public SourceSplitEnumerator<BabelforceCheckpoint, BabelforceEnumerator.State>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<BabelforceCheckpoint> context,
                    BabelforceEnumerator.State state) {
        return new BabelforceEnumerator(context, options, state);
    }
}
