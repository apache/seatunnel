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
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;
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
     * Validates and applies the configured {@link SchemaSaveMode} and {@link DataSaveMode}.
     *
     * <p>Only {@link SchemaSaveMode#RECREATE_SCHEMA} and {@link DataSaveMode#APPEND_DATA} are
     * currently supported. Unsupported modes throw immediately so users are never misled by
     * accepted-but-no-op settings. Full Admin API support (table creation / truncation) can be
     * added in a follow-up once a BigtableCatalog is available.
     */
    private void handleSaveMode() {
        if (schemaSaveMode == SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.TABLE_CREATE_FAILED,
                    "schema_save_mode=CREATE_SCHEMA_WHEN_NOT_EXIST is not yet supported by the "
                            + "Bigtable connector. Please create the table and column families "
                            + "manually and set schema_save_mode=RECREATE_SCHEMA.");
        }
        if (dataSaveMode == DataSaveMode.DROP_DATA) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.TABLE_TRUNCATE_FAILED,
                    "data_save_mode=DROP_DATA is not yet supported by the Bigtable connector. "
                            + "Please truncate the table manually or use data_save_mode=APPEND_DATA.");
        }
        if (dataSaveMode == DataSaveMode.ERROR_WHEN_DATA_EXISTS) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.TABLE_QUERY_FAILED,
                    "data_save_mode=ERROR_WHEN_DATA_EXISTS is not yet supported by the Bigtable "
                            + "connector. Please use data_save_mode=APPEND_DATA.");
        }
        log.info("Bigtable sink save mode: schema={}, data={}", schemaSaveMode, dataSaveMode);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
