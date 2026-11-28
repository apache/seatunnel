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
package org.apache.seatunnel.connectors.seatunnel.bluetally;

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
 * Bounded source for BlueTally asset and employee records.
 *
 * <p>Each selected resource has a distinct provider offset in the ordered scan.
 */
public final class BlueTallySource
        implements SeaTunnelSource<SeaTunnelRow, BlueTallySplit, BlueTallyEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "BlueTally";
    private final BlueTallyConfig options;
    private final CatalogTable table;

    BlueTallySource(BlueTallyConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", "BlueTally resource name"))
                        .column(
                                text(
                                        "record_id",
                                        "Provider system id, independent of the asset label"))
                        .column(
                                PhysicalColumn.of(
                                        "tenant_id",
                                        BasicType.INT_TYPE,
                                        null,
                                        true,
                                        null,
                                        "Configured tenant ID when present"))
                        .column(text("updated_at", "BlueTally update time in UTC ISO 8601"))
                        .column(text("payload", "Complete provider record as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "bluetally", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "BlueTally asset and employee records");
    }

    private static PhysicalColumn text(String name, String description) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, false, null, description);
    }

    @Override
    public String getPluginName() {
        return NAME;
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
     * Builds the worker that acquires resource pages and updates collector progress.
     *
     * <p>No HTTPS connection is opened until a split is assigned.
     */
    @Override
    public SourceReader<SeaTunnelRow, BlueTallySplit> createReader(SourceReader.Context context) {
        return new BlueTallyReader(
                context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BlueTallySplit, BlueTallyEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BlueTallySplit> context) {
        return new BlueTallyEnumerator(context, options, null);
    }

    /**
     * Restores the coordinator-owned list scan before the reader receives it.
     *
     * <p>After first assignment, the reader owns its own saved progress.
     */
    @Override
    public SourceSplitEnumerator<BlueTallySplit, BlueTallyEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BlueTallySplit> context,
            BlueTallyEnumerator.Snapshot checkpoint) {
        return new BlueTallyEnumerator(context, options, checkpoint);
    }
}
