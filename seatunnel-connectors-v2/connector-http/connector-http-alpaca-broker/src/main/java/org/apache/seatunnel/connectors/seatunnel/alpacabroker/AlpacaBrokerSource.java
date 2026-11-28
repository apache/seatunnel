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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

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
 * Bounded stateful source for one configured Alpaca Broker API resource.
 *
 * <p>The output uses a common schema and preserves each resource's JSON payload.
 */
public final class AlpacaBrokerSource
        implements SeaTunnelSource<
                SeaTunnelRow, AlpacaBrokerPosition, AlpacaBrokerEnumerator.State> {
    static final String IDENTIFIER = "AlpacaBroker";
    private static final long serialVersionUID = 1L;
    private final AlpacaBrokerSettings settings;
    private final CatalogTable table;

    AlpacaBrokerSource(AlpacaBrokerSettings settings) {
        this.settings = settings;
        TableSchema.Builder schema = TableSchema.builder();
        schema.column(PhysicalColumn.of("resource", BasicType.STRING_TYPE, 0, false, null, null));
        schema.column(PhysicalColumn.of("record_id", BasicType.STRING_TYPE, 0, false, null, null));
        schema.column(PhysicalColumn.of("account_id", BasicType.STRING_TYPE, 0, true, null, null));
        schema.column(PhysicalColumn.of("payload", BasicType.STRING_TYPE, 0, false, null, null));
        table =
                CatalogTable.of(
                        TableIdentifier.of(IDENTIFIER, TablePath.of(settings.resource.key)),
                        schema.build(),
                        Map.of(),
                        List.of(),
                        "Alpaca Broker API records");
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
     * Each assigned reader resumes one resource with the bounds saved in its split.
     *
     * <p>The reader can use the checkpoint without recalculating the provider's upper bound.
     */
    @Override
    public SourceReader<SeaTunnelRow, AlpacaBrokerPosition> createReader(
            SourceReader.Context context) {
        return new AlpacaBrokerReader(
                context, settings, table.getTableId().toTablePath().toString());
    }

    /**
     * Coordinator captures a consistent upper bound before the first request.
     *
     * <p>The resulting bound is part of the split, including work not yet assigned to a reader.
     */
    @Override
    public SourceSplitEnumerator<AlpacaBrokerPosition, AlpacaBrokerEnumerator.State>
            createEnumerator(SourceSplitEnumerator.Context<AlpacaBrokerPosition> context) {
        return new AlpacaBrokerEnumerator(context, settings, null);
    }

    /**
     * Restore the original upper bound, including a split whose assignment was checkpointed.
     *
     * <p>Restoring a terminal position completes the source without reissuing prior reads.
     */
    @Override
    public SourceSplitEnumerator<AlpacaBrokerPosition, AlpacaBrokerEnumerator.State>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<AlpacaBrokerPosition> context,
                    AlpacaBrokerEnumerator.State state) {
        return new AlpacaBrokerEnumerator(context, settings, state);
    }
}
