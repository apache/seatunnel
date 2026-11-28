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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

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
 * Reads one bounded Appfigures account resource.
 *
 * <p>Rows preserve the provider record and any enclosing dictionary keys through a fixed schema.
 */
public final class AppfiguresSource
        implements SeaTunnelSource<SeaTunnelRow, AppfiguresSplit, AppfiguresEnumerator.State> {
    private static final long serialVersionUID = 1L;
    static final String IDENTIFIER = "Appfigures";
    private final AppfiguresSettings settings;
    private final CatalogTable table;

    AppfiguresSource(AppfiguresSettings settings) {
        this.settings = settings;
        TableSchema.Builder schema = TableSchema.builder();
        for (String field : List.of("resource", "record_id", "dimension_keys", "payload")) {
            schema.column(PhysicalColumn.of(field, BasicType.STRING_TYPE, 0, false, null, null));
        }
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of(settings.resource.name)),
                        schema.build(),
                        Map.of(),
                        List.of(),
                        "Appfigures API records");
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
     * Creates a split reader with the resource's catalog table.
     *
     * <p>The HTTPS client is opened on the worker thread after a split is assigned.
     */
    @Override
    public SourceReader<SeaTunnelRow, AppfiguresSplit> createReader(SourceReader.Context context) {
        return new AppfiguresReader(context, settings, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<AppfiguresSplit, AppfiguresEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AppfiguresSplit> context) {
        return new AppfiguresEnumerator(context, settings, null);
    }

    @Override
    public SourceSplitEnumerator<AppfiguresSplit, AppfiguresEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AppfiguresSplit> context,
            AppfiguresEnumerator.State state) {
        return new AppfiguresEnumerator(context, settings, state);
    }
}
