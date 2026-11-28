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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.util.List;
import java.util.Map;

/**
 * Bounded source for one Selling Partner API resource.
 *
 * <p>Report and direct API rows share an identity and payload schema without flattening provider
 * records.
 */
public final class AmazonSellerPartnerSource
        implements SeaTunnelSource<SeaTunnelRow, SpPosition, SpEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "AmazonSellerPartner";

    private final SpSettings settings;
    private final CatalogTable table;

    AmazonSellerPartnerSource(SpSettings settings) {
        this.settings = settings;
        TableSchema.Builder schema = TableSchema.builder();
        schema.column(PhysicalColumn.of("resource", BasicType.STRING_TYPE, 0, false, null, null));
        schema.column(PhysicalColumn.of("record_id", BasicType.STRING_TYPE, 0, false, null, null));
        schema.column(PhysicalColumn.of("parent_id", BasicType.STRING_TYPE, 0, true, null, null));
        schema.column(
                PhysicalColumn.of("marketplace_id", BasicType.STRING_TYPE, 0, true, null, null));
        schema.column(PhysicalColumn.of("payload", BasicType.STRING_TYPE, 0, false, null, null));
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of(settings.resource.name())),
                        schema.build(),
                        Map.of(),
                        List.of(),
                        "Amazon Selling Partner API records");
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
     * Creates the reader associated with this resource's output schema.
     *
     * <p>Network clients are created on the acquisition worker only after assignment.
     */
    @Override
    public SourceReader<SeaTunnelRow, SpPosition> createReader(SourceReader.Context context) {
        return new SpReader(context, settings, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<SpPosition, SpEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<SpPosition> context) {
        return new SpEnumerator(context, settings, null);
    }

    @Override
    public SourceSplitEnumerator<SpPosition, SpEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<SpPosition> context, SpEnumerator.State state) {
        return new SpEnumerator(context, settings, state);
    }
}
