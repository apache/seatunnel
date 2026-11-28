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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

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
 * Bounded source for BugSnag v2 monitoring, metadata and membership records.
 *
 * <p>All resource rows share a fixed envelope and retain their complete response object.
 */
public final class BugSnagSource
        implements SeaTunnelSource<SeaTunnelRow, BugSnagSplit, BugSnagEnumerator.Snapshot> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "BugSnag";

    private final BugSnagConfig options;
    private final CatalogTable table;

    BugSnagSource(BugSnagConfig options) {
        this.options = options;
        TableSchema schema =
                TableSchema.builder()
                        .column(text("resource", false, "BugSnag resource name"))
                        .column(text("record_id", false, "Provider record key within its scope"))
                        .column(text("organization_id", true, "BugSnag organization ID"))
                        .column(text("project_id", true, "BugSnag project ID"))
                        .column(text("search_id", true, "BugSnag saved search ID"))
                        .column(text("record_time", true, "Normalized UTC time when filtered"))
                        .column(
                                text(
                                        "payload",
                                        false,
                                        "Complete provider result object encoded as JSON"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "bugsnag", "records"),
                        schema,
                        Map.of(),
                        List.of(),
                        "BugSnag monitoring and account data");
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
     * Creates the worker without contacting BugSnag.
     *
     * <p>Each assigned split determines the specific company requests to make.
     */
    @Override
    public SourceReader<SeaTunnelRow, BugSnagSplit> createReader(SourceReader.Context context) {
        return new BugSnagReader(context, options, table.getTableId().toTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<BugSnagSplit, BugSnagEnumerator.Snapshot> createEnumerator(
            SourceSplitEnumerator.Context<BugSnagSplit> context) {
        return new BugSnagEnumerator(context, options, null);
    }

    /**
     * Restores coordinator ownership before considering another assignment.
     *
     * <p>Once a worker accepts the split, only its checkpoint owns the traversal.
     */
    @Override
    public SourceSplitEnumerator<BugSnagSplit, BugSnagEnumerator.Snapshot> restoreEnumerator(
            SourceSplitEnumerator.Context<BugSnagSplit> context,
            BugSnagEnumerator.Snapshot checkpoint) {
        return new BugSnagEnumerator(context, options, checkpoint);
    }
}
