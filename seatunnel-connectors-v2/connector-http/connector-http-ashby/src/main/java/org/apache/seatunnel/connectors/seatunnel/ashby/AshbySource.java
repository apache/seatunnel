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

package org.apache.seatunnel.connectors.seatunnel.ashby;

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
 * Ashby list resources through the provider's read-only RPC API.
 *
 * <p>The same stable row schema is used for a bounded pass and token-based streaming passes.
 */
public final class AshbySource
        implements SeaTunnelSource<SeaTunnelRow, AshbySplit, AshbyEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Ashby";
    private final AshbySettings settings;
    private final CatalogTable table;

    AshbySource(AshbySettings settings) {
        this.settings = settings;
        TableSchema.Builder schema = TableSchema.builder();
        for (String field : List.of("resource", "record_id", "parent_id", "payload")) {
            schema.column(
                    PhysicalColumn.of(
                            field,
                            BasicType.STRING_TYPE,
                            0,
                            "parent_id".equals(field),
                            null,
                            null));
        }
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of(settings.resource.key)),
                        schema.build(),
                        Map.of(),
                        List.of(),
                        "Ashby API records");
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Boundedness getBoundedness() {
        return settings.interval == 0 ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return List.of(table);
    }

    /**
     * Creates the worker that will acquire pages for this resource.
     *
     * <p>Its independent HTTPS client is initialized only when the first split starts.
     */
    @Override
    public SourceReader<SeaTunnelRow, AshbySplit> createReader(SourceReader.Context context) {
        return new AshbyReader(context, settings, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<AshbySplit, AshbyEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AshbySplit> context) {
        return new AshbyEnumerator(context, settings, null);
    }

    @Override
    public SourceSplitEnumerator<AshbySplit, AshbyEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AshbySplit> context, AshbyEnumerator.State checkpoint) {
        return new AshbyEnumerator(context, settings, checkpoint);
    }
}
