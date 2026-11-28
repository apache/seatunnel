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
package org.apache.seatunnel.connectors.seatunnel.bitly;

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
 * Bounded Bitly source for organization, link and aggregate analytics records.
 *
 * <p>The single ordered split retains resource lineage throughout nested traversal.
 */
public final class BitlySource
        implements SeaTunnelSource<SeaTunnelRow, BitlySplit, BitlyEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Bitly";
    private final BitlyConfig options;
    private final CatalogTable table;

    BitlySource(BitlyConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "The selected Bitly resource"))
                        .column(text("record_id", false, "Record identifier within its parent"))
                        .column(text("organization_id", true, "The owning organization when known"))
                        .column(text("group_id", true, "The owning group when known"))
                        .column(
                                text(
                                        "bitlink_id",
                                        true,
                                        "The associated domain and hash when known"))
                        .column(text("payload", false, "Complete Bitly response record as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "bitly", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Bitly resource records");
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
    public SourceReader<SeaTunnelRow, BitlySplit> createReader(SourceReader.Context context) {
        return new BitlyReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BitlySplit, BitlyEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BitlySplit> context) {
        return new BitlyEnumerator(context, options, null);
    }

    /**
     * Restores any coordinator-owned traversal before the reader receives it.
     *
     * <p>After first assignment, the reader owns its own saved progress.
     */
    @Override
    public SourceSplitEnumerator<BitlySplit, BitlyEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BitlySplit> context,
            BitlyEnumerator.Snapshot checkpoint) {
        return new BitlyEnumerator(context, options, checkpoint);
    }
}
