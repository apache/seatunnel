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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.IrisCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.savemode.IrisSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode.JdbcSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

@Slf4j
public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportSchemaEvolutionSink {

    private final TableSchema tableSchema;

    private JobContext jobContext;

    private final JdbcSinkConfig jdbcSinkConfig;

    private final JdbcDialect dialect;

    private final ReadonlyConfig config;

    private final DataSaveMode dataSaveMode;

    private final SchemaSaveMode schemaSaveMode;

    private final CatalogTable catalogTable;

    public JdbcSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable) {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (Exception e) {
            log.warn(
                    "Failed to load JDBC driver {}",
                    jdbcSinkConfig.getJdbcConnectionConfig().getDriverName(),
                    e);
        }
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.catalogTable = catalogTable;
        this.tableSchema = catalogTable.getTableSchema();
    }

    @Override
    public String getPluginName() {
        return "Jdbc";
    }

    @Override
    public AbstractJdbcSinkWriter createWriter(SinkWriter.Context context) {
        return createWriter(context, tableSchema, new ArrayList<>());
    }

    private AbstractJdbcSinkWriter createWriter(
            SinkWriter.Context context, TableSchema writerTableSchema, List<JdbcSinkState> states) {
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (Exception e) {
            log.warn(
                    "Failed to load JDBC driver {}",
                    jdbcSinkConfig.getJdbcConnectionConfig().getDriverName(),
                    e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        AbstractJdbcSinkWriter sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            List<JdbcSinkState> xidStates =
                    states.stream()
                            .filter(state -> state.getXid() != null)
                            .collect(Collectors.toList());
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            sinkTablePath,
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            writerTableSchema,
                            getDatabaseTableSchema().orElse(null),
                            xidStates);

        } else {
            Integer primaryKeyIndex = resolvePrimaryKeyIndex(writerTableSchema);
            boolean hasRestoredSchema =
                    states.stream().anyMatch(state -> state.getTableSchema() != null);
            sinkWriter =
                    new JdbcSinkWriter(
                            sinkTablePath,
                            context,
                            dialect,
                            jdbcSinkConfig,
                            writerTableSchema,
                            getDatabaseTableSchema().orElse(null),
                            primaryKeyIndex,
                            hasRestoredSchema);
        }
        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        TableSchema restoredTableSchema = resolveRestoredTableSchema(tableSchema, states);
        return createWriter(context, restoredTableSchema, states);
    }

    static TableSchema resolveRestoredTableSchema(
            TableSchema initialTableSchema, List<JdbcSinkState> states) {
        TableSchema restoredTableSchema = null;
        for (JdbcSinkState state : states) {
            TableSchema stateTableSchema = state.getTableSchema();
            if (stateTableSchema == null) {
                continue;
            }
            if (restoredTableSchema == null) {
                restoredTableSchema = stateTableSchema;
            } else if (!restoredTableSchema.equals(stateTableSchema)) {
                throw new IllegalStateException(
                        "JDBC sink cannot restore divergent table schemas from the same checkpoint");
            }
        }
        return restoredTableSchema == null ? initialTableSchema : restoredTableSchema;
    }

    private Integer resolvePrimaryKeyIndex(TableSchema writerTableSchema) {
        PrimaryKey primaryKey = writerTableSchema.getPrimaryKey();
        if (primaryKey == null) {
            primaryKey = tableSchema.getPrimaryKey();
            if (primaryKey != null) {
                log.warn(
                        "Restored JDBC table schema for {} has no primary-key metadata; using the configured primary key {}",
                        catalogTable.getTablePath(),
                        primaryKey.getColumnNames());
            }
        }
        if (primaryKey == null) {
            return null;
        }
        int index =
                writerTableSchema
                        .toPhysicalRowDataType()
                        .indexOf(primaryKey.getColumnNames().get(0));
        return index > -1 ? index : null;
    }

    @Override
    public Optional<Serializer<JdbcSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public boolean requiresWriterState() {
        return jdbcSinkConfig.isExactlyOnce();
    }

    private Optional<TableSchema> getDatabaseTableSchema() {
        Optional<Catalog> catalogOptional = getCatalog();
        FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcSinkOptions.FIELD_IDE);
        String fieldIde =
                fieldIdeEnumEnum == null
                        ? FieldIdeEnum.ORIGINAL.getValue()
                        : fieldIdeEnumEnum.getValue();
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        CatalogUtils.quoteTableIdentifier(
                                catalogTable.getTableId().getTableName(), fieldIde));
        if (catalogOptional.isPresent()) {
            try (Catalog catalog = catalogOptional.get()) {
                catalog.open();
                return Optional.of(catalog.getTable(tablePath).getTableSchema());
            } catch (TableNotExistException e) {
                log.warn("table {} not exist when get the database catalog table", tablePath);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
            createAggregatedCommitter() {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (Exception e) {
            log.warn(
                    "Failed to load JDBC driver {}",
                    jdbcSinkConfig.getJdbcConnectionConfig().getDriverName(),
                    e);
        }
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (Exception e) {
            log.warn(
                    "Failed to load JDBC driver {}",
                    jdbcSinkConfig.getJdbcConnectionConfig().getDriverName(),
                    e);
        }
        if (catalogTable != null) {
            Optional<Catalog> catalogOptional = getCatalog();
            if (catalogOptional.isPresent()) {
                try {
                    Catalog catalog = catalogOptional.get();
                    FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcSinkOptions.FIELD_IDE);
                    String fieldIde =
                            fieldIdeEnumEnum == null
                                    ? FieldIdeEnum.ORIGINAL.getValue()
                                    : fieldIdeEnumEnum.getValue();
                    TablePath tablePath =
                            TablePath.of(
                                    catalogTable.getTableId().getDatabaseName(),
                                    catalogTable.getTableId().getSchemaName(),
                                    CatalogUtils.quoteTableIdentifier(
                                            catalogTable.getTableId().getTableName(), fieldIde));
                    catalogTable.getOptions().put("fieldIde", fieldIde);
                    if (catalog instanceof IrisCatalog) {
                        return Optional.of(
                                new IrisSaveModeHandler(
                                        schemaSaveMode,
                                        dataSaveMode,
                                        catalog,
                                        tablePath,
                                        catalogTable,
                                        config.get(JdbcSinkOptions.CUSTOM_SQL),
                                        jdbcSinkConfig.isCreateIndex()));
                    }
                    return Optional.of(
                            new JdbcSaveModeHandler(
                                    schemaSaveMode,
                                    dataSaveMode,
                                    catalog,
                                    tablePath,
                                    catalogTable,
                                    config.get(JdbcSinkOptions.CUSTOM_SQL),
                                    jdbcSinkConfig.isCreateIndex()));
                } catch (Exception e) {
                    throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Catalog> getCatalog() {
        if (StringUtils.isBlank(jdbcSinkConfig.getDatabase())) {
            return Optional.empty();
        }
        if (StringUtils.isBlank(jdbcSinkConfig.getTable())) {
            return Optional.empty();
        }
        // use query to write data can not support get catalog
        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            return Optional.empty();
        }
        return JdbcCatalogUtils.findCatalog(jdbcSinkConfig.getJdbcConnectionConfig(), dialect);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
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
