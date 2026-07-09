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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
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
        Map<String, String> sinkTableOptions = config.get(SinkConnectorCommonOptions.TABLE_OPTIONS);
        CatalogTable catalogTable = context.getCatalogTable();
        ReadonlyConfig catalogOptions = getCatalogOptions(context);
        Optional<String> optionalTable = config.getOptional(JdbcSinkOptions.TABLE);
        Optional<String> optionalDatabase = config.getOptional(JdbcSinkOptions.DATABASE);
        // source table info
        TableIdentifier tableId = catalogTable.getTableId();
        // sink table info
        String sinkDatabaseName =
                optionalDatabase.orElse(catalogTable.getTablePath().getDatabaseName());
        String sinkTableNameBefore =
                optionalTable.orElse(catalogTable.getTablePath().getTableName());
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
        // prefix / suffix
        String tempTableName;
        String prefix = catalogOptions.get(JdbcSinkOptions.TABLE_PREFIX);
        String suffix = catalogOptions.get(JdbcSinkOptions.TABLE_SUFFIX);
        if (StringUtils.isNotEmpty(prefix) || StringUtils.isNotEmpty(suffix)) {
            tempTableName = StringUtils.isNotEmpty(prefix) ? prefix + sinkTableName : sinkTableName;
            tempTableName = StringUtils.isNotEmpty(suffix) ? tempTableName + suffix : tempTableName;
        } else {
            tempTableName = sinkTableName;
        }
        // without replace, keep original directly
        String finalSchemaName = sinkSchemaName;
        String finalTableName = tempTableName;
        // rebuild identifier
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        sinkDatabaseName,
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
        if (CollectionUtils.isEmpty(config.get(JdbcSinkOptions.PRIMARY_KEYS))) {
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
                keyOptional.ifPresent(
                        constraintKey ->
                                map.put(
                                        JdbcSinkOptions.PRIMARY_KEYS.key(),
                                        constraintKey.getColumnNames().stream()
                                                .map(
                                                        ConstraintKey.ConstraintKeyColumn
                                                                ::getColumnName)
                                                .collect(Collectors.joining(","))));
            }
        } else {
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
        final ReadonlyConfig options = config;
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcSinkOptions.FIELD_IDE);
        catalogTable.getOptions().putAll(sinkTableOptions);
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
                        JdbcSinkOptions.ORACLE_INSERT_MODE,
                        Conditions.extension(
                                JdbcSinkOptions.ORACLE_INSERT_MODE,
                                new OracleAppendValuesValidator()))
                .optional(
                        JdbcSinkOptions.IS_EXACTLY_ONCE,
                        Conditions.extension(
                                JdbcSinkOptions.IS_EXACTLY_ONCE,
                                new ExactlyOnceMaxRetriesValidator()))
                .optional(
                        JdbcSinkOptions.CREATE_INDEX,
                        JdbcSinkOptions.USERNAME,
                        JdbcSinkOptions.PASSWORD,
                        JdbcSinkOptions.CONNECTION_CHECK_TIMEOUT_SEC,
                        JdbcSinkOptions.BATCH_SIZE,
                        JdbcSinkOptions.BATCH_INTERVAL_MS,
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
                .optional(
                        SinkConnectorCommonOptions.TABLE_OPTIONS,
                        Conditions.extension(
                                SinkConnectorCommonOptions.TABLE_OPTIONS,
                                JdbcTableOptionsConditionExtension.INSTANCE))
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

    /**
     * Submission-time validator for {@code oracle_insert_mode=APPEND_VALUES}.
     *
     * <p>Enforces config-level incompatibilities that can be detected from the user-supplied
     * options alone: copy statement, exactly-once, auto_commit=false, custom query, and insert-only
     * upsert.
     *
     * <p><b>Note:</b> The {@code primary_keys} conflict is <em>not</em> checked here because
     * primary keys may be derived from the upstream {@code CatalogTable} at factory time (inside
     * {@link #createSink}), which happens after OptionRule validation. That case is guarded at
     * runtime by {@code JdbcOutputFormatBuilder.validateOracleInsertMode}.
     */
    static class OracleAppendValuesValidator
            implements ConditionExtension<JdbcSinkConfig.OracleInsertMode> {
        @Override
        public String description() {
            return "oracle_insert_mode=APPEND_VALUES conflicts with certain options";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, JdbcSinkConfig.OracleInsertMode value)
                throws OptionValidationException {
            if (value != JdbcSinkConfig.OracleInsertMode.APPEND_VALUES) {
                return true;
            }
            if (config.get(JdbcSinkOptions.USE_COPY_STATEMENT)) {
                throw new OptionValidationException(
                        "oracle_insert_mode=APPEND_VALUES does not support copy statement.");
            }
            if (config.get(JdbcSinkOptions.IS_EXACTLY_ONCE)) {
                throw new OptionValidationException(
                        "oracle_insert_mode=APPEND_VALUES does not support exactly-once.");
            }
            if (!config.get(JdbcSinkOptions.AUTO_COMMIT)) {
                throw new OptionValidationException(
                        "oracle_insert_mode=APPEND_VALUES requires auto_commit=true.");
            }
            if (!config.get(JdbcSinkOptions.GENERATE_SINK_SQL)) {
                throw new OptionValidationException(
                        "oracle_insert_mode=APPEND_VALUES does not support custom query.");
            }
            if (config.get(JdbcSinkOptions.SUPPORT_UPSERT_BY_INSERT_ONLY)) {
                throw new OptionValidationException(
                        "oracle_insert_mode=APPEND_VALUES does not support insert-only upsert.");
            }
            return true;
        }
    }

    /**
     * Submission-time validator for {@code is_exactly_once=true}.
     *
     * <p>JDBC XA sink does not support retries; {@code max_retries} must be 0 when exactly-once is
     * enabled, otherwise duplicates may occur.
     */
    static class ExactlyOnceMaxRetriesValidator implements ConditionExtension<Boolean> {
        @Override
        public String description() {
            return "is_exactly_once=true requires max_retries=0";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Boolean value)
                throws OptionValidationException {
            if (Boolean.TRUE.equals(value)) {
                int maxRetries = config.get(JdbcSinkOptions.MAX_RETRIES);
                if (maxRetries != 0) {
                    throw new OptionValidationException(
                            "JDBC XA sink requires max_retries equal to 0 when is_exactly_once=true, "
                                    + "otherwise it could cause duplicates.");
                }
            }
            return true;
        }
    }
}
