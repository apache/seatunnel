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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.catalog.StarRocksCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class StarRocksSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportSaveMode, SupportSchemaEvolutionSink, SupportMultiTableSink {

    private final TableSchema tableSchema;
    private final SinkConfig sinkConfig;
    private final DataSaveMode dataSaveMode;
    private final SchemaSaveMode schemaSaveMode;
    private final CatalogTable catalogTable;
    private final Map<String, String> tableOptions;

    public StarRocksSink(
            SinkConfig sinkConfig, CatalogTable catalogTable, Map<String, String> tableOptions) {
        this.sinkConfig = sinkConfig;
        this.tableSchema = catalogTable.getTableSchema();
        this.catalogTable = catalogTable;
        this.tableOptions = tableOptions;
        this.dataSaveMode = sinkConfig.getDataSaveMode();
        this.schemaSaveMode = sinkConfig.getSchemaSaveMode();
        validateJsonColumnsAgainstLoadFormat(sinkConfig, this.tableSchema);
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
    }

    @Override
    public String getPluginName() {
        return StarRocksCatalogFactory.IDENTIFIER;
    }

    @Override
    public StarRocksSinkWriter createWriter(SinkWriter.Context context) {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new StarRocksSinkWriter(context, sinkConfig, tableSchema, sinkTablePath);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver {}", "com.mysql.cj.jdbc.Driver", e);
        }
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        catalogTable.getTableId().getTableName());
        Catalog catalog =
                new StarRocksCatalog(
                        StarRocksBaseOptions.CONNECTOR_IDENTITY,
                        sinkConfig.getUsername(),
                        sinkConfig.getPassword(),
                        sinkConfig.getJdbcUrl(),
                        sinkConfig.getSaveModeCreateTemplate(),
                        tableOptions);
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

    /**
     * CSV Stream Load cannot carry a JSON logical column (the CSV serializer rejects it on the
     * first flush), so the combination is rejected here, when the sink is created at job
     * submission, instead of after the job has started.
     */
    private static void validateJsonColumnsAgainstLoadFormat(
            SinkConfig sinkConfig, TableSchema tableSchema) {
        if (sinkConfig.getLoadFormat() != SinkConfig.StreamLoadFormat.CSV) {
            return;
        }
        List<String> jsonColumns =
                tableSchema.getColumns().stream()
                        .filter(column -> column.getDataType().getSqlType() == SqlType.JSON)
                        .map(Column::getName)
                        .collect(Collectors.toList());
        if (jsonColumns.isEmpty()) {
            return;
        }
        throw new StarRocksConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                String.format(
                        "StarRocks sink table %s.%s has native JSON column(s) %s, but the Stream Load "
                                + "format is CSV, which does not support JSON-formatted columns. Set "
                                + "`starrocks.config.format = \"JSON\"` for this sink, or convert the "
                                + "field(s) to STRING upstream (for example `CAST(field AS VARCHAR)` in a "
                                + "Calcite Transform).",
                        sinkConfig.getDatabase(), sinkConfig.getTable(), jsonColumns));
    }
}
