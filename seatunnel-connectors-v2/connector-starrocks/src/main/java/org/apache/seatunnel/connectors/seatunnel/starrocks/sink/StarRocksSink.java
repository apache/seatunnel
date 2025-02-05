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

package org.apache.seatunnel.connectors.seatunnel.starrocks.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class StarRocksSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportSaveMode, SupportSchemaEvolutionSink, SupportMultiTableSink {

    private final TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final DataSaveMode dataSaveMode;
    private final SchemaSaveMode schemaSaveMode;
    private final CatalogTable catalogTable;

    public StarRocksSink(SinkConfig sinkConfig, CatalogTable catalogTable) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = catalogTable.getTableSchema();
        this.catalogTable = catalogTable;
        this.dataSaveMode = sinkConfig.getDataSaveMode();
        this.schemaSaveMode = sinkConfig.getSchemaSaveMode();
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
    }

    @Override
    public StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new StarRocksSinkWriter(sinkConfig, tableSchema, sinkTablePath);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName());
        Catalog catalog =
                new StarRocksCatalog(
                        "StarRocks",
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getSaveModeCreateTemplate());
        return Optional.of(
                new DefaultSaveModeHandler(
                        schemaSaveMode,
                        dataSaveMode,
                        catalog,
                        tablePath,
                        catalogTable,
                        sinkConfig.getCustomSql()));
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN);
    }
}
