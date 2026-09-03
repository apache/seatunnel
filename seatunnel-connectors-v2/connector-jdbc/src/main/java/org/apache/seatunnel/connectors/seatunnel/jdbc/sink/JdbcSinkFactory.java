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
import org.apache.seatunnel.api.sink.TablePlaceholder;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@AutoService(Factory.class)
public class JdbcSinkFactory implements TableSinkFactory, SupportSinkDryRunValidation {
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
        // source table info
        TableIdentifier tableId = catalogTable.getTableId();
        // sink table info
        TablePath sinkTablePath = resolveSinkTablePath(config, catalogOptions, catalogTable);
        // rebuild identifier
        TableIdentifier newTableId =
                TableIdentifier.of(
                        tableId.getCatalogName(),
                        sinkTablePath.getDatabaseName(),
                        sinkTablePath.getSchemaName(),
                        sinkTablePath.getTableName());
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
        Optional<List<String>> multiTablePrimaryKeys =
                resolveMultiTablePrimaryKeys(config, catalogTable);
        if (multiTablePrimaryKeys.isPresent()) {
            catalogTable = applyPrimaryKeys(map, catalogTable, multiTablePrimaryKeys.get());
        } else {
            catalogTable = applyFallbackPrimaryKeys(config, map, catalogTable);
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

    /**
     * Writes the resolved primary key columns into the sink config map and rebuilds the catalog
     * table so that auto-created tables and generated upsert/update/delete statements use the given
     * key columns.
     *
     * @param map the sink config map that is later turned back into a {@link ReadonlyConfig}
     * @param catalogTable the table being processed
     * @param primaryKeys the resolved key columns
     * @return a new catalog table whose primary key is replaced with the resolved columns
     */
    private CatalogTable applyPrimaryKeys(
            Map<String, String> map, CatalogTable catalogTable, List<String> primaryKeys) {
        map.put(JdbcSinkOptions.PRIMARY_KEYS.key(), String.join(",", primaryKeys));
        PrimaryKey configPk =
                PrimaryKey.of(
                        catalogTable.getTablePath().getTableName() + "_config_pk", primaryKeys);
        TableSchema tableSchema = catalogTable.getTableSchema();
        return CatalogTable.of(
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

    /**
     * Resolves the primary key columns using the pre-existing fallback logic when no multi-table
     * mapping matches: explicit top-level {@code primary_keys}, otherwise the catalog primary key,
     * otherwise the first unique key. When no key can be determined, the config map is left
     * unchanged and the sink falls back to plain INSERT.
     *
     * @param config the sink config
     * @param map the sink config map that is later turned back into a {@link ReadonlyConfig}
     * @param catalogTable the table being processed
     * @return the (possibly rebuilt) catalog table
     */
    private CatalogTable applyFallbackPrimaryKeys(
            ReadonlyConfig config, Map<String, String> map, CatalogTable catalogTable) {
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
            return catalogTable;
        }
        return applyPrimaryKeys(map, catalogTable, config.get(JdbcSinkOptions.PRIMARY_KEYS));
    }

    /**
     * Resolves the per-table primary key mapping from {@code multi-table_config.primary_keys}.
     *
     * <p>Each key is a Java regular expression matched against the upstream table name using full
     * match semantics; the first matching pattern in declaration order wins. An unmatched table
     * returns {@link Optional#empty()}, leaving the fallback logic to run.
     *
     * @param config the sink config
     * @param catalogTable the table being processed
     * @return the resolved key columns, or {@link Optional#empty()} when no pattern matches
     */
    Optional<List<String>> resolveMultiTablePrimaryKeys(
            ReadonlyConfig config, CatalogTable catalogTable) {
        Map<String, Object> multiTableConfig = config.get(JdbcSinkOptions.MULTI_TABLE_CONFIG);
        if (multiTableConfig == null || multiTableConfig.isEmpty()) {
            return Optional.empty();
        }
        Object primaryKeysObj = multiTableConfig.get("primary_keys");
        if (!(primaryKeysObj instanceof Map)) {
            return Optional.empty();
        }
        String tableName = catalogTable.getTableId().getTableName();
        Map<?, ?> primaryKeyMap = (Map<?, ?>) primaryKeysObj;
        for (Map.Entry<?, ?> entry : primaryKeyMap.entrySet()) {
            String pattern = String.valueOf(entry.getKey());
            if (matchesPattern(tableName, pattern)) {
                return Optional.of(
                        expandPrimaryKeyPlaceholder(
                                toPrimaryKeyList(entry.getValue()), catalogTable, pattern));
            }
        }
        return Optional.empty();
    }

    /**
     * Matches a table name against a regular expression using full-match semantics.
     *
     * @throws JdbcConnectorException when the pattern is not a valid regular expression
     */
    private boolean matchesPattern(String tableName, String pattern) {
        try {
            return tableName.matches(pattern);
        } catch (java.util.regex.PatternSyntaxException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.INVALID_MULTI_TABLE_CONFIG,
                    String.format(
                            "Invalid regular expression '%s' in multi-table_config.primary_keys.",
                            pattern),
                    e);
        }
    }

    /**
     * Converts a configured primary key value into a list of column names. A list value is used
     * as-is; a string value is split by comma.
     *
     * @throws JdbcConnectorException when the value is neither a list nor a string
     */
    private List<String> toPrimaryKeyList(Object value) {
        if (value instanceof List) {
            List<String> keys = new ArrayList<>();
            for (Object element : (List<?>) value) {
                keys.add(String.valueOf(element));
            }
            return keys;
        }
        if (value instanceof String) {
            String stringValue = (String) value;
            if (StringUtils.isBlank(stringValue)) {
                return Collections.emptyList();
            }
            return Arrays.stream(stringValue.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }
        throw new JdbcConnectorException(
                JdbcConnectorErrorCode.INVALID_MULTI_TABLE_CONFIG,
                "multi-table_config.primary_keys values must be a string or a list of strings.");
    }

    /**
     * Expands {@code ${primary_key}} and {@code ${unique_key}} placeholders in a key-column list.
     * Each placeholder must be a whole element and is replaced by the corresponding upstream key
     * columns.
     *
     * @throws JdbcConnectorException when a placeholder is used but the upstream table has no
     *     matching key
     */
    private List<String> expandPrimaryKeyPlaceholder(
            List<String> keys, CatalogTable catalogTable, String pattern) {
        String primaryKeyPlaceholder =
                "${" + TablePlaceholder.REPLACE_PRIMARY_KEY.getPlaceholder() + "}";
        String uniqueKeyPlaceholder =
                "${" + TablePlaceholder.REPLACE_UNIQUE_KEY.getPlaceholder() + "}";
        List<String> primaryKeyColumns = getPrimaryKeyColumns(catalogTable);
        List<String> uniqueKeyColumns = getUniqueKeyColumns(catalogTable);
        List<String> resolved = new ArrayList<>();
        for (String key : keys) {
            if (primaryKeyPlaceholder.equals(key)) {
                if (primaryKeyColumns.isEmpty()) {
                    throw new JdbcConnectorException(
                            JdbcConnectorErrorCode.INVALID_MULTI_TABLE_CONFIG,
                            String.format(
                                    "Table '%s' matched pattern '%s' in multi-table_config.primary_keys "
                                            + "which uses '${primary_key}', but the upstream table has no primary key.",
                                    catalogTable.getTableId().getTableName(), pattern));
                }
                resolved.addAll(primaryKeyColumns);
            } else if (uniqueKeyPlaceholder.equals(key)) {
                if (uniqueKeyColumns.isEmpty()) {
                    throw new JdbcConnectorException(
                            JdbcConnectorErrorCode.INVALID_MULTI_TABLE_CONFIG,
                            String.format(
                                    "Table '%s' matched pattern '%s' in multi-table_config.primary_keys "
                                            + "which uses '${unique_key}', but the upstream table has no unique key.",
                                    catalogTable.getTableId().getTableName(), pattern));
                }
                resolved.addAll(uniqueKeyColumns);
            } else {
                resolved.add(key);
            }
        }
        return resolved;
    }

    /**
     * Returns the upstream primary key column names, or an empty list when there is no primary key.
     */
    private List<String> getPrimaryKeyColumns(CatalogTable catalogTable) {
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        if (primaryKey == null || CollectionUtils.isEmpty(primaryKey.getColumnNames())) {
            return Collections.emptyList();
        }
        return new ArrayList<>(primaryKey.getColumnNames());
    }

    /**
     * Returns the first upstream unique key column names, or an empty list when there is no unique
     * key.
     */
    private List<String> getUniqueKeyColumns(CatalogTable catalogTable) {
        Optional<ConstraintKey> keyOptional =
                catalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key ->
                                        ConstraintKey.ConstraintType.UNIQUE_KEY.equals(
                                                key.getConstraintType()))
                        .findFirst();
        return keyOptional
                .map(
                        constraintKey ->
                                constraintKey.getColumnNames().stream()
                                        .map(ConstraintKey.ConstraintKeyColumn::getColumnName)
                                        .collect(Collectors.toList()))
                .orElseGet(Collections::emptyList);
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
                        JdbcSinkOptions.MULTI_TABLE_CONFIG,
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
     * Validates sink connectivity and schema compatibility for {@code --dry-run connect} without
     * creating writers, committers, or save-mode handlers, and without executing any DDL/DML.
     *
     * <p>Checks performed:
     *
     * <ul>
     *   <li>Connectivity and credentials, by opening the same catalog (or raw connection) used at
     *       runtime
     *   <li>Target table existence; a missing table only fails when {@code schema_save_mode} is
     *       {@link SchemaSaveMode#ERROR_WHEN_SCHEMA_NOT_EXIST}, because other save modes create it
     *       at runtime
     *   <li>Field compatibility: every upstream field must exist in the target table when it
     *       already exists
     * </ul>
     */
    @Override
    public void validateConnectionForDryRun(TableSinkFactoryContext context) throws Exception {
        ReadonlyConfig config = context.getOptions();
        JdbcSinkConfig sinkConfig = JdbcSinkConfig.of(config);
        FieldIdeEnum fieldIdeEnum = config.get(JdbcSinkOptions.FIELD_IDE);
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

        Optional<Catalog> optionalCatalog =
                JdbcCatalogUtils.findCatalog(sinkConfig.getJdbcConnectionConfig(), dialect);
        if (!optionalCatalog.isPresent()) {
            // No catalog implementation for this dialect: validate connectivity and credentials
            // with a plain connection, table-level checks are not possible.
            try (Connection connection =
                    dialect.getJdbcConnectionProvider(sinkConfig.getJdbcConnectionConfig())
                            .getOrEstablishConnection()) {
                return;
            }
        }

        try (Catalog catalog = optionalCatalog.get()) {
            catalog.open();

            TablePath targetTablePath = resolveDryRunTargetTablePath(context);
            if (targetTablePath == null) {
                // Custom-query sink or unresolvable table name: connectivity is all we can check.
                return;
            }

            if (!catalog.tableExists(targetTablePath)) {
                if (config.get(JdbcSinkOptions.SCHEMA_SAVE_MODE)
                        == SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST) {
                    throw new JdbcConnectorException(
                            JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                            String.format(
                                    "Sink table %s does not exist and schema_save_mode is %s.",
                                    targetTablePath.getFullName(),
                                    SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST));
                }
                // Table will be created by save mode at runtime; nothing more to validate.
                return;
            }

            CatalogTable targetTable = catalog.getTable(targetTablePath);
            Set<String> targetColumns =
                    targetTable.getTableSchema().getColumns().stream()
                            .map(column -> column.getName().toLowerCase(Locale.ROOT))
                            .collect(Collectors.toSet());
            List<String> missingColumns =
                    context.getCatalogTable().getTableSchema().getColumns().stream()
                            .map(Column::getName)
                            .filter(name -> !targetColumns.contains(name.toLowerCase(Locale.ROOT)))
                            .collect(Collectors.toList());
            if (!missingColumns.isEmpty()) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.CONNECT_DATABASE_FAILED,
                        String.format(
                                "Sink table %s is missing upstream fields %s.",
                                targetTablePath.getFullName(), missingColumns));
            }
        }
    }

    /**
     * Resolves the sink table naming shared by {@link #createSink} and dry-run validation:
     * database/table overrides, dotted table name split, schema option, and prefix/suffix. Keeping
     * a single implementation guarantees dry-run validates exactly the table the runtime writes to.
     */
    private TablePath resolveSinkTablePath(
            ReadonlyConfig config, ReadonlyConfig catalogOptions, CatalogTable upstreamTable) {
        String databaseName =
                config.getOptional(JdbcSinkOptions.DATABASE)
                        .orElse(upstreamTable.getTablePath().getDatabaseName());
        String tableNameBefore =
                config.getOptional(JdbcSinkOptions.TABLE)
                        .orElse(upstreamTable.getTablePath().getTableName());
        String[] tableSplitArray = tableNameBefore.split("\\.");
        String tableName = tableSplitArray[tableSplitArray.length - 1];
        String schemaName =
                tableSplitArray.length > 1 ? tableSplitArray[tableSplitArray.length - 2] : null;
        if (StringUtils.isNotBlank(catalogOptions.get(JdbcSinkOptions.SCHEMA))) {
            schemaName = catalogOptions.get(JdbcSinkOptions.SCHEMA);
        }
        String prefix = catalogOptions.get(JdbcSinkOptions.TABLE_PREFIX);
        String suffix = catalogOptions.get(JdbcSinkOptions.TABLE_SUFFIX);
        if (StringUtils.isNotEmpty(prefix)) {
            tableName = prefix + tableName;
        }
        if (StringUtils.isNotEmpty(suffix)) {
            tableName = tableName + suffix;
        }
        return TablePath.of(databaseName, schemaName, tableName);
    }

    /**
     * Resolves the target table path for dry-run validation. Returns {@code null} when the sink
     * writes through a custom query or the table name cannot be determined.
     */
    private TablePath resolveDryRunTargetTablePath(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        if (config.getOptional(JdbcSinkOptions.QUERY).isPresent()) {
            return null;
        }
        TablePath tablePath =
                resolveSinkTablePath(config, getCatalogOptions(context), context.getCatalogTable());
        if (StringUtils.isBlank(tablePath.getDatabaseName())
                || StringUtils.isBlank(tablePath.getTableName())) {
            return null;
        }
        return tablePath;
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
