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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SinkReplaceNameConstant;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.auto.service.AutoService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@AutoService(Factory.class)
public class JdbcSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Jdbc";
    }

    private ReadonlyConfig getCatalogOptions(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        // TODO Remove obsolete code
        Optional<Map<String, String>> catalogOptions =
                config.getOptional(ConnectorCommonOptions.CATALOG_OPTIONS);
        if (catalogOptions.isPresent()) {
            return ReadonlyConfig.fromMap(new HashMap<>(catalogOptions.get()));
        }
        return config;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig catalogOptions = getCatalogOptions(context);
        Optional<String> optionalTable = config.getOptional(JdbcSinkOptions.TABLE);
        Optional<String> optionalDatabase = config.getOptional(JdbcSinkOptions.DATABASE);
        if (!optionalTable.isPresent()) {
            optionalTable = Optional.of(SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY);
        }
        // get source table relevant information
        TableIdentifier tableId = catalogTable.getTableId();
        String sourceDatabaseName = tableId.getDatabaseName();
        String sourceSchemaName = tableId.getSchemaName();
        String pluginInputIdentifier = tableId.getTableName();
        // get sink table relevant information
        String sinkDatabaseName =
                optionalDatabase.orElse(SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY);
        String sinkTableNameBefore = optionalTable.get();
        String[] sinkTableSplitArray = sinkTableNameBefore.split("\\.");
        String sinkTableName = sinkTableSplitArray[sinkTableSplitArray.length - 1];
        String sinkSchemaName;
        if (sinkTableSplitArray.length > 1) {
            sinkSchemaName = sinkTableSplitArray[sinkTableSplitArray.length - 2];
        } else {
            sinkSchemaName = null;
        }
        if (StringUtils.isNotBlank(catalogOptions.get(JdbcSinkOptions.SCHEMA))) {
            sinkSchemaName = catalogOptions.get(JdbcSinkOptions.SCHEMA);
        }
        // to add tablePrefix and tableSuffix
        String tempTableName;
        String prefix = catalogOptions.get(JdbcSinkOptions.TABLE_PREFIX);
        String suffix = catalogOptions.get(JdbcSinkOptions.TABLE_SUFFIX);
        if (StringUtils.isNotEmpty(prefix) || StringUtils.isNotEmpty(suffix)) {
            tempTableName = StringUtils.isNotEmpty(prefix) ? prefix + sinkTableName : sinkTableName;
            tempTableName = StringUtils.isNotEmpty(suffix) ? tempTableName + suffix : tempTableName;

        } else {
            tempTableName = sinkTableName;
        }
        // to replace
        String finalDatabaseName = sinkDatabaseName;
        if (StringUtils.isNotEmpty(sourceDatabaseName)) {
            finalDatabaseName =
                    sinkDatabaseName.replace(
                            SinkReplaceNameConstant.REPLACE_DATABASE_NAME_KEY, sourceDatabaseName);
        }

        String finalSchemaName;
        if (sinkSchemaName != null) {
            if (sourceSchemaName == null) {
                finalSchemaName = sinkSchemaName;
            } else {
                finalSchemaName =
                        sinkSchemaName.replace(
                                SinkReplaceNameConstant.REPLACE_SCHEMA_NAME_KEY, sourceSchemaName);
            }
        } else {
            finalSchemaName = null;
        }
        String finalTableName = sinkTableName;
        if (StringUtils.isNotEmpty(pluginInputIdentifier)) {
            finalTableName =
                    tempTableName.replace(
                            SinkReplaceNameConstant.REPLACE_TABLE_NAME_KEY, pluginInputIdentifier);
        }

        // rebuild TableIdentifier and catalogTable
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        finalDatabaseName,
                        finalSchemaName,
                        finalTableName);
        catalogTable =
                CatalogTable.of(
                        newTableId,
                        catalogTable.getTableSchema(),
                        catalogTable.getOptions(),
                        catalogTable.getPartitionKeys(),
                        catalogTable.getComment(),
                        catalogTable.getCatalogName());
        Map<String, String> map = config.toMap();
        if (catalogTable.getTableId().getSchemaName() != null) {
            map.put(
                    JdbcSinkOptions.TABLE.key(),
                    catalogTable.getTableId().getSchemaName()
                            + "."
                            + catalogTable.getTableId().getTableName());
        } else {
            map.put(JdbcSinkOptions.TABLE.key(), catalogTable.getTableId().getTableName());
        }
        map.put(JdbcSinkOptions.DATABASE.key(), catalogTable.getTableId().getDatabaseName());
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (!config.getOptional(JdbcSinkOptions.PRIMARY_KEYS).isPresent()) {
            if (primaryKey != null && !CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
                map.put(
                        JdbcSinkOptions.PRIMARY_KEYS.key(),
                        String.join(",", primaryKey.getColumnNames()));
            } else {
                Optional<ConstraintKey> keyOptional =
                        catalogTable.getTableSchema().getConstraintKeys().stream()
                                .filter(
                                        key ->
                                                ConstraintKey.ConstraintType.UNIQUE_KEY.equals(
                                                        key.getConstraintType()))
                                .findFirst();
                if (keyOptional.isPresent()) {
                    map.put(
                            JdbcSinkOptions.PRIMARY_KEYS.key(),
                            keyOptional.get().getColumnNames().stream()
                                    .map(key -> key.getColumnName())
                                    .collect(Collectors.joining(",")));
                }
            }
        } else {
            // replace primary key to config
            PrimaryKey configPk =
                    PrimaryKey.of(
                            catalogTable.getTablePath().getTableName() + "_config_pk",
                            config.get(JdbcSinkOptions.PRIMARY_KEYS));
            TableSchema tableSchema = catalogTable.getTableSchema();
            catalogTable =
                    CatalogTable.of(
                            catalogTable.getTableId(),
                            TableSchema.builder()
                                    .primaryKey(configPk)
                                    .constraintKey(tableSchema.getConstraintKeys())
                                    .columns(tableSchema.getColumns())
                                    .build(),
                            catalogTable.getOptions(),
                            catalogTable.getPartitionKeys(),
                            catalogTable.getComment(),
                            catalogTable.getCatalogName());
        }
        config = ReadonlyConfig.fromMap(new HashMap<>(map));
        // always execute
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcSinkOptions.FIELD_IDE);
        catalogTable
                .getOptions()
                .put("fieldIde", fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        sinkConfig.getJdbcConnectionConfig().getUrl(),
                        sinkConfig.getJdbcConnectionConfig().getCompatibleMode(),
                        sinkConfig.getJdbcConnectionConfig().getDialect(),
                        fieldIdeEnum == null ? null : fieldIdeEnum.getValue());
        dialect.connectionUrlParse(
                sinkConfig.getJdbcConnectionConfig().getUrl(),
                sinkConfig.getJdbcConnectionConfig().getProperties(),
                dialect.defaultParameter());
        CatalogTable finalCatalogTable = catalogTable;
        // get saveMode
        DataSaveMode dataSaveMode = config.get(JdbcSinkOptions.DATA_SAVE_MODE);
        SchemaSaveMode schemaSaveMode = config.get(JdbcSinkOptions.SCHEMA_SAVE_MODE);
        return () ->
                new JdbcSink(
                        options,
                        sinkConfig,
                        dialect,
                        schemaSaveMode,
                        dataSaveMode,
                        finalCatalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        JdbcSinkOptions.URL,
                        JdbcSinkOptions.DRIVER,
                        JdbcSinkOptions.SCHEMA_SAVE_MODE,
                        JdbcSinkOptions.DATA_SAVE_MODE)
                .optional(
                        JdbcSinkOptions.CREATE_INDEX,
                        JdbcSinkOptions.USERNAME,
                        JdbcSinkOptions.PASSWORD,
                        JdbcSinkOptions.CONNECTION_CHECK_TIMEOUT_SEC,
                        JdbcSinkOptions.BATCH_SIZE,
                        JdbcSinkOptions.IS_EXACTLY_ONCE,
                        JdbcSinkOptions.GENERATE_SINK_SQL,
                        JdbcSinkOptions.AUTO_COMMIT,
                        JdbcSinkOptions.PRIMARY_KEYS,
                        JdbcSinkOptions.IS_PRIMARY_KEY_UPDATED,
                        JdbcSinkOptions.SUPPORT_UPSERT_BY_INSERT_ONLY,
                        JdbcSinkOptions.USE_COPY_STATEMENT,
                        JdbcSinkOptions.COMPATIBLE_MODE,
                        JdbcSinkOptions.ENABLE_UPSERT,
                        JdbcSinkOptions.FIELD_IDE,
                        JdbcSinkOptions.TABLE_PREFIX,
                        JdbcSinkOptions.TABLE_SUFFIX,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA,
                        JdbcSinkOptions.DIALECT)
                .conditional(
                        JdbcSinkOptions.IS_EXACTLY_ONCE,
                        true,
                        JdbcSinkOptions.XA_DATA_SOURCE_CLASS_NAME,
                        JdbcSinkOptions.MAX_COMMIT_ATTEMPTS,
                        JdbcSinkOptions.TRANSACTION_TIMEOUT_SEC)
                .conditional(JdbcSinkOptions.IS_EXACTLY_ONCE, false, JdbcSinkOptions.MAX_RETRIES)
                .conditional(JdbcSinkOptions.GENERATE_SINK_SQL, true, JdbcSinkOptions.DATABASE)
                .conditional(JdbcSinkOptions.GENERATE_SINK_SQL, false, JdbcSinkOptions.QUERY)
                .conditional(
                        JdbcSinkOptions.DATA_SAVE_MODE,
                        DataSaveMode.CUSTOM_PROCESSING,
                        JdbcSinkOptions.CUSTOM_SQL)
                .build();
    }
}
