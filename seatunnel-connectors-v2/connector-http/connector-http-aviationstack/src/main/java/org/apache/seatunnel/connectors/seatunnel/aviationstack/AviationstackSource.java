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
package org.apache.seatunnel.connectors.seatunnel.aviationstack;

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
 * Bounded source for selected Aviationstack catalogs.
 *
 * <p>One flat output schema retains the provider record object for every catalog. The ordered
 * catalog split stops at the first permanent account or response failure.
 */
public final class AviationstackSource
        implements SeaTunnelSource<
                SeaTunnelRow, AviationstackSplit, AviationstackEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Aviationstack";
    private final AviationstackConfig config;
    private final CatalogTable table;

    AviationstackSource(AviationstackConfig config) {
        this.config = config;
        TableSchema rowSchema =
                TableSchema.builder()
                        .column(stringColumn("resource", "Aviationstack catalog name"))
                        .column(stringColumn("record_id", "Identifier from the provider id field"))
                        .column(stringColumn("payload", "Full provider record encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, "aviationstack", "catalog"),
                        rowSchema,
                        Map.of(),
                        List.of(),
                        "Aviationstack directory catalogs");
    }

    private static PhysicalColumn stringColumn(String name, String comment) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, false, null, comment);
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
     * Creates the reader for the catalog table.
     *
     * <p>It uses the catalog identifier on every emitted row so sinks can route all resources.
     */
    @Override
    public SourceReader<SeaTunnelRow, AviationstackSplit> createReader(
            SourceReader.Context context) {
        return new AviationstackReader(
                context, config, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<AviationstackSplit, AviationstackEnumerator.Snapshot>
            createEnumerator(SourceSplitEnumerator.Context<AviationstackSplit> context) {
        return new AviationstackEnumerator(context, config, null);
    }

    /**
     * Recovers pending assignment from the coordinator snapshot.
     *
     * <p>Work that was already assigned is read from the worker's own checkpoint.
     */
    @Override
    public SourceSplitEnumerator<AviationstackSplit, AviationstackEnumerator.Snapshot>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AviationstackSplit> context,
                    AviationstackEnumerator.Snapshot snapshot) {
        return new AviationstackEnumerator(context, config, snapshot);
    }
}
