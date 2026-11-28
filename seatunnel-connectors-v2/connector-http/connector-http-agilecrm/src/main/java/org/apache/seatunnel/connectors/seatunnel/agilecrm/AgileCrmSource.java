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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

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

import java.util.Collections;
import java.util.List;

/** Reads one bounded Agile CRM resource with its provider record and discovery scope. */
public final class AgileCrmSource
        implements SeaTunnelSource<SeaTunnelRow, AgileCrmTraversal, AgileCrmEnumerator.State> {
    static final String IDENTIFIER = "AgileCRM";
    private static final long serialVersionUID = 1L;
    private final AgileCrmSettings config;
    private final CatalogTable table;

    AgileCrmSource(AgileCrmSettings config) {
        this.config = config;
        TableSchema schema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "resource", BasicType.STRING_TYPE, 0, false, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "record_id", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "parent_id", BasicType.STRING_TYPE, 0, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "payload", BasicType.STRING_TYPE, 0, false, null, null))
                        .build();
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of(config.resource.name)),
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "Agile CRM records");
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
        return Collections.singletonList(table);
    }

    @Override
    public SourceReader<SeaTunnelRow, AgileCrmTraversal> createReader(
            SourceReader.Context context) {
        return new AgileCrmReader(config, context, table.getTableId().toTablePath().toString());
    }

    @Override
    public SourceSplitEnumerator<AgileCrmTraversal, AgileCrmEnumerator.State> createEnumerator(
            SourceSplitEnumerator.Context<AgileCrmTraversal> context) {
        return new AgileCrmEnumerator(context, config, null);
    }

    @Override
    public SourceSplitEnumerator<AgileCrmTraversal, AgileCrmEnumerator.State> restoreEnumerator(
            SourceSplitEnumerator.Context<AgileCrmTraversal> context,
            AgileCrmEnumerator.State state) {
        return new AgileCrmEnumerator(context, config, state);
    }
}
