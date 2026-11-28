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
package org.apache.seatunnel.connectors.seatunnel.beamer;

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
 * Bounded source for Beamer NPS feedback.
 *
 * <p>Each row contains the provider NPS object and convenient fields for its identity and time. The
 * source keeps a single ordered split for the configured creation window.
 */
public final class BeamerSource
        implements SeaTunnelSource<SeaTunnelRow, BeamerSplit, BeamerEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Beamer";
    private final BeamerConfig config;
    private final CatalogTable table;

    BeamerSource(BeamerConfig config) {
        this.config = config;
        TableSchema rowSchema =
                TableSchema.builder()
                        .column(stringColumn("record_id", true, "Beamer NPS response identifier"))
                        .column(stringColumn("created_at", false, "Creation time in UTC ISO-8601"))
                        .column(
                                stringColumn(
                                        "payload", false, "Complete NPS response encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, "beamer", "nps"),
                        rowSchema,
                        Map.of(),
                        List.of(),
                        "Beamer NPS responses");
    }

    private static PhysicalColumn stringColumn(String name, boolean nullable, String comment) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, nullable, null, comment);
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
     * Creates the reader for the NPS table.
     *
     * <p>The reader sets the catalog table identifier on every emitted row.
     */
    @Override
    public SourceReader<SeaTunnelRow, BeamerSplit> createReader(SourceReader.Context context) {
        return new BeamerReader(context, config, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BeamerSplit, BeamerEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BeamerSplit> context) {
        return new BeamerEnumerator(context, config, null);
    }

    /**
     * Recovers pending assignment from the coordinator snapshot.
     *
     * <p>Work that was already assigned is read from the worker's own checkpoint.
     */
    @Override
    public SourceSplitEnumerator<BeamerSplit, BeamerEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BeamerSplit> context,
            BeamerEnumerator.Snapshot snapshot) {
        return new BeamerEnumerator(context, config, snapshot);
    }
}
