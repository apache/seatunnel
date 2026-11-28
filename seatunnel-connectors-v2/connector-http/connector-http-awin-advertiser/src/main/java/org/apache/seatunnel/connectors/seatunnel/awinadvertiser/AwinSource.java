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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

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
 * Bounded source for read-only advertiser reports, transactions and account data.
 *
 * <p>All resources have a shared row shape with their original and derived fields kept in JSON.
 */
public final class AwinSource
        implements SeaTunnelSource<SeaTunnelRow, AwinCursor, AwinEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String NAME = "AwinAdvertiser";
    private final AwinSettings settings;
    private final CatalogTable table;

    AwinSource(AwinSettings settings) {
        this.settings = settings;
        TableSchema schema =
                TableSchema.builder()
                        .column(string("resource", false, "Selected Awin advertiser resource"))
                        .column(
                                string(
                                        "record_key",
                                        false,
                                        "Provider identifier or JSON report dimensions"))
                        .column(
                                string(
                                        "advertiser_id",
                                        false,
                                        "Advertiser program or advertiser account ID"))
                        .column(string("region", true, "Report region where present"))
                        .column(string("report_date", true, "UTC report or transaction date"))
                        .column(
                                string(
                                        "payload",
                                        false,
                                        "Provider record JSON and derived dimensions"))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(NAME, "awin", "advertiser"),
                        schema,
                        Map.of(),
                        List.of(),
                        "Awin advertiser records");
    }

    private static PhysicalColumn string(String name, boolean optional, String description) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, null, optional, null, description);
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
     * Opens the single source reader with the shared routing identity.
     *
     * <p>All advertiser resources flow through the same catalog table schema.
     */
    @Override
    public SourceReader<SeaTunnelRow, AwinCursor> createReader(SourceReader.Context context) {
        return new AwinReader(context, settings, table.getTablePath().getFullName());
    }

    @Override
    public SourceSplitEnumerator<AwinCursor, AwinEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AwinCursor> context) {
        return new AwinEnumerator(context, settings, null);
    }

    /**
     * Recovers the coordinated starting point without recapturing dates or reapplying lookback.
     *
     * <p>After assignment the worker restores the exact report or regional position.
     */
    @Override
    public SourceSplitEnumerator<AwinCursor, AwinEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AwinCursor> context, AwinEnumerator.State checkpoint) {
        return new AwinEnumerator(context, settings, checkpoint);
    }
}
