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
package org.apache.seatunnel.connectors.seatunnel.buzzsprout;

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
 * Bounded source for Buzzsprout podcast and episode metadata.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class BuzzsproutSource
        implements SeaTunnelSource<SeaTunnelRow, BuzzsproutSplit, BuzzsproutEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "Buzzsprout";

    private final BuzzsproutConfig options;
    private final CatalogTable table;

    BuzzsproutSource(BuzzsproutConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "Buzzsprout resource name"))
                        .column(text("record_id", false, "Provider numeric ID encoded as text"))
                        .column(text("podcast_id", false, "Owning podcast ID"))
                        .column(text("published_at", true, "Episode publication timestamp in UTC"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "buzzsprout", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Buzzsprout podcasts and episodes");
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
     * Creates the worker without contacting Buzzsprout.
     *
     * <p>Each assigned split determines the specific company requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, BuzzsproutSplit> createReader(SourceReader.Context context) {
        return new BuzzsproutReader(
                context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BuzzsproutSplit, BuzzsproutEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BuzzsproutSplit> context) {
        return new BuzzsproutEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<BuzzsproutSplit, BuzzsproutEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BuzzsproutSplit> context,
            BuzzsproutEnumerator.Snapshot checkpoint) {
        return new BuzzsproutEnumerator(context, options, checkpoint);
    }
}
