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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator.TiDBSourceCheckpointState;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator.TiDBSourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader.TiDBSourceReader;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import java.util.Collections;
import java.util.List;

public class TiDBSource
        implements SeaTunnelSource<SeaTunnelRow, TiDBSourceSplit, TiDBSourceCheckpointState>,
                SupportParallelism,
                SupportColumnProjection {

    static final String IDENTIFIER = "TiDB-CDC";

    private TiDBSourceConfig config;
    private final CatalogTable catalogTable;

    public TiDBSource(ReadonlyConfig config, CatalogTable catalogTable) {

        this.config =
                TiDBSourceConfig.builder()
                        .startupMode(config.get(TiDBSourceOptions.STARTUP_MODE))
                        .databaseName(config.get(TiDBSourceOptions.DATABASE_NAME))
                        .tableName(config.get(TiDBSourceOptions.TABLE_NAME))
                        .batchSize(config.get(TiDBSourceOptions.BATCH_SIZE_PER_SCAN))
                        .tiConfiguration(TiDBSourceOptions.getTiConfiguration(config))
                        .build();
        this.catalogTable = catalogTable;
    }

    /**
     * Returns a unique identifier among same factory interfaces.
     *
     * <p>For consistency, an identifier should be declared as one lower case word (e.g. {@code
     * kafka}). If multiple factories exist for different versions, a version should be appended
     * using "-" (e.g. {@code elasticsearch-7}).
     */
    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    /**
     * Get the boundedness of this source.
     *
     * @return the boundedness of this source.
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.UNBOUNDED;
    }

    /**
     * Create source reader, used to produce data.
     *
     * @param context reader context.
     * @return source reader.
     * @throws Exception when create reader failed.
     */
    @Override
    public SourceReader<SeaTunnelRow, TiDBSourceSplit> createReader(SourceReader.Context context)
            throws Exception {
        return new TiDBSourceReader(context, config, catalogTable);
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called only once
     * when start a source.
     *
     * @param context enumerator context.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceCheckpointState> createEnumerator(
            SourceSplitEnumerator.Context<TiDBSourceSplit> context) throws Exception {
        return new TiDBSourceSplitEnumerator(context, config);
    }

    /**
     * Create source split enumerator, used to generate splits. This method will be called when
     * restore from checkpoint.
     *
     * @param context enumerator context.
     * @param checkpointState checkpoint state.
     * @return source split enumerator.
     * @throws Exception when create enumerator failed.
     */
    @Override
    public SourceSplitEnumerator<TiDBSourceSplit, TiDBSourceCheckpointState> restoreEnumerator(
            SourceSplitEnumerator.Context<TiDBSourceSplit> context,
            TiDBSourceCheckpointState checkpointState)
            throws Exception {
        return new TiDBSourceSplitEnumerator(context, config, checkpointState);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }
}
