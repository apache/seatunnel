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
package org.apache.seatunnel.connectors.seatunnel.box;

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
 * Bounded source for Box account resources and document extraction.
 *
 * <p>Each row keeps the provider record together with its optional parent and extracted content.
 */
public final class BoxSource
        implements SeaTunnelSource<SeaTunnelRow, BoxSplit, BoxEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Box";
    private final BoxConfig options;
    private final CatalogTable table;

    BoxSource(BoxConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Selected Box resource"))
                        .column(text("record_id", false, "Provider record identity"))
                        .column(
                                text(
                                        "parent_id",
                                        true,
                                        "Parent collection, folder or file identity when applicable"))
                        .column(text("payload", false, "Full provider record or file detail JSON"))
                        .column(
                                text(
                                        "content",
                                        true,
                                        "Extracted text or Box AI response when applicable"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "box", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Box resource and extraction records");
    }

    private static PhysicalColumn text(String name, boolean nullable, String description) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, nullable, null, description);
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
    public SourceReader<SeaTunnelRow, BoxSplit> createReader(SourceReader.Context context) {
        return new BoxReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BoxSplit, BoxEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BoxSplit> context) {
        return new BoxEnumerator(context, options, null);
    }

    /**
     * Restores any coordinator-owned traversal before the reader receives it.
     *
     * <p>After first assignment, the reader owns its own saved progress.
     */
    @Override
    public SourceSplitEnumerator<BoxSplit, BoxEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BoxSplit> context, BoxEnumerator.Snapshot checkpoint) {
        return new BoxEnumerator(context, options, checkpoint);
    }
}
