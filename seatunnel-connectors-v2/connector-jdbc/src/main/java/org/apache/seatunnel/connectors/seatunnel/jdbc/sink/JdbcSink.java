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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.IrisCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.savemode.IrisSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.savemode.JdbcSaveModeHandler;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        AbstractJdbcSinkWriter sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            sinkTablePath,
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            tableSchema,
                            getDatabaseTableSchema().orElse(null),
                            new ArrayList<>());
        } else {
            if (catalogTable.getTableSchema().getPrimaryKey() != null) {
                String keyName = tableSchema.getPrimaryKey().getColumnNames().get(0);
                int index = tableSchema.toPhysicalRowDataType().indexOf(keyName);
                if (index > -1) {
                    return new JdbcSinkWriter(
                            sinkTablePath,
                            dialect,
                            jdbcSinkConfig,
                            tableSchema,
                            getDatabaseTableSchema().orElse(null),
                            index);
                }
            }
            sinkWriter =
                    new JdbcSinkWriter(
                            sinkTablePath,
                            dialect,
                            jdbcSinkConfig,
                            tableSchema,
                            getDatabaseTableSchema().orElse(null),
                            null);
        }
        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        if (jdbcSinkConfig.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                    sinkTablePath,
                    context,
                    jobContext,
                    dialect,
                    jdbcSinkConfig,
                    tableSchema,
                    getDatabaseTableSchema().orElse(null),
                    states);
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    private Optional<TableSchema> getDatabaseTableSchema() {
        Optional<Catalog> catalogOptional = getCatalog();
        FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
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
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (catalogTable != null) {
            Optional<Catalog> catalogOptional = getCatalog();
            if (catalogOptional.isPresent()) {
                try {
                    Catalog catalog = catalogOptional.get();
                    FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
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
                                        config.get(JdbcOptions.CUSTOM_SQL),
                                        jdbcSinkConfig.isCreateIndex()));
                    }
                    return Optional.of(
                            new JdbcSaveModeHandler(
                                    schemaSaveMode,
                                    dataSaveMode,
                                    catalog,
                                    tablePath,
                                    catalogTable,
                                    config.get(JdbcOptions.CUSTOM_SQL),
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
