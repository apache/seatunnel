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

package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.bigtable.constant.BigtableIdentifier;
import org.apache.seatunnel.connectors.seatunnel.bigtable.state.BigtableAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.bigtable.state.BigtableCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.bigtable.state.BigtableSinkState;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class BigtableSink
        implements SeaTunnelSink<
                        SeaTunnelRow,
                        BigtableSinkState,
                        BigtableCommitInfo,
                        BigtableAggregatedCommitInfo>,
                SupportMultiTableSink {

    private final ReadonlyConfig config;
    private final CatalogTable catalogTable;
    private final BigtableParameters parameters;
    private final SeaTunnelRowType rowType;
    private final List<Integer> rowkeyColumnIndexes = new ArrayList<>();
    private int versionColumnIndex = -1;
    private final SchemaSaveMode schemaSaveMode;
    private final DataSaveMode dataSaveMode;

    public BigtableSink(ReadonlyConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.parameters = BigtableParameters.buildWithConfig(config);
        this.rowType = catalogTable.getSeaTunnelRowType();
        if (parameters.getVersionColumn() != null) {
            this.versionColumnIndex = rowType.indexOf(parameters.getVersionColumn());
        }
        this.schemaSaveMode = config.get(BigtableSinkOptions.SCHEMA_SAVE_MODE);
        this.dataSaveMode = config.get(BigtableSinkOptions.DATA_SAVE_MODE);
    }

    @Override
    public String getPluginName() {
        return BigtableIdentifier.IDENTIFIER_NAME;
    }

    @Override
    public BigtableSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        for (String rowkeyColumn : parameters.getRowkeyColumns()) {
            rowkeyColumnIndexes.add(rowType.indexOf(rowkeyColumn));
        }
        if (parameters.getVersionColumn() != null) {
            this.versionColumnIndex = rowType.indexOf(parameters.getVersionColumn());
        }
        handleSaveMode();
        return new BigtableSinkWriter(rowType, parameters, rowkeyColumnIndexes, versionColumnIndex);
    }

    /**
     * Applies the configured {@link SchemaSaveMode} and {@link DataSaveMode} before writing starts.
     *
     * <p>Currently only logs the configured modes. Full implementation requires a Bigtable Admin
     * client to create/truncate tables, which can be added in a follow-up iteration once a
     * BigtableCatalog is available.
     */
    private void handleSaveMode() {
        log.info("Bigtable sink save mode: schema={}, data={}", schemaSaveMode, dataSaveMode);
        if (schemaSaveMode == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
            log.info(
                    "schema_save_mode=CREATE_SCHEMA_WHEN_NOT_EXIST: "
                            + "Bigtable table creation via Admin API is not yet implemented. "
                            + "Please ensure the table and column families exist before running the job.");
        }
        if (dataSaveMode == DataSaveMode.DROP_DATA) {
            log.warn(
                    "data_save_mode=DROP_DATA: "
                            + "Bigtable table truncation via Admin API is not yet implemented. "
                            + "Existing data will NOT be dropped.");
        }
        if (dataSaveMode == DataSaveMode.ERROR_WHEN_DATA_EXISTS) {
            log.warn(
                    "data_save_mode=ERROR_WHEN_DATA_EXISTS: "
                            + "Bigtable data existence check is not yet implemented. "
                            + "The job will proceed regardless.");
        }
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
