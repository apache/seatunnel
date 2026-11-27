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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.condition.UpdateCondition;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferReducedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.BufferedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.CopyManagerBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.FieldNamedPreparedStatement;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.InsertOrUpdateBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.OrderedBufferedBatchStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.SimpleBatchStatementExecutor;

import org.postgresql.Driver;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class JdbcOutputFormatBuilder {
    private static final String POSTGRESQL_URL_PREFIX = "jdbc:postgresql:";

    @NonNull private final JdbcDialect dialect;
    @NonNull private final JdbcConnectionProvider connectionProvider;
    @NonNull private final JdbcSinkConfig jdbcSinkConfig;
    @NonNull private final TableSchema tableSchema;
    @Nullable private final TableSchema databaseTableSchema;

    private boolean commitOnFlush;

    /**
     * Commits the connection after every successful batch flush when it uses manual commit. Only
     * the non-XA writer enables this, and only when checkpointing is disabled, so flushed batches
     * are not held in one unbounded transaction until close.
     */
    public JdbcOutputFormatBuilder commitOnFlush(boolean commitOnFlush) {
        this.commitOnFlush = commitOnFlush;
        return this;
    }

    public JdbcOutputFormat build() {
        JdbcOutputFormat.StatementExecutorFactory statementExecutorFactory;

        final String database = jdbcSinkConfig.getDatabase();
        final String table = jdbcSinkConfig.getTable();
        final List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        validateOracleInsertMode(dialect, jdbcSinkConfig, primaryKeys);
        Optional<UpdateCondition> updateCondition =
                resolveUpdateCondition(dialect, jdbcSinkConfig, tableSchema);
        if (updateCondition.isPresent()) {
            String[] pkNames = primaryKeys.toArray(new String[0]);
            String upsertSQL =
                    ((PostgresDialect) dialect)
                            .getConditionalUpsertStatement(
                                    database,
                                    table,
                                    tableSchema.getFieldNames(),
                                    pkNames,
                                    updateCondition.get());
            log.info("update_condition_sql is enabled, generated upsert SQL: {}", upsertSQL);
            boolean oneRowPerStatement =
                    isRewriteBatchedInsertsEnabled(jdbcSinkConfig.getJdbcConnectionConfig());
            if (oneRowPerStatement) {
                log.info(
                        "reWriteBatchedInserts is enabled, update_condition_sql sends one row per"
                                + " statement");
            }
            statementExecutorFactory =
                    () ->
                            createConditionalUpsertBufferedExecutor(
                                    (PostgresDialect) dialect,
                                    database,
                                    table,
                                    tableSchema,
                                    databaseTableSchema,
                                    pkNames,
                                    upsertSQL,
                                    oneRowPerStatement);
        } else if (jdbcSinkConfig.isUseCopyStatement()) {
            statementExecutorFactory =
                    () ->
                            createCopyInBufferStatementExecutor(
                                    createCopyInBatchStatementExecutor(
                                            dialect, table, tableSchema));
        } else if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    jdbcSinkConfig.getSimpleSql(),
                                    tableSchema,
                                    databaseTableSchema,
                                    dialect.getRowConverter());
        } else if (primaryKeys == null || primaryKeys.isEmpty()) {
            statementExecutorFactory =
                    () ->
                            createSimpleBufferedExecutor(
                                    dialect,
                                    jdbcSinkConfig,
                                    database,
                                    table,
                                    tableSchema,
                                    databaseTableSchema);
        } else {
            statementExecutorFactory =
                    () ->
                            createUpsertBufferedExecutor(
                                    dialect,
                                    database,
                                    table,
                                    tableSchema,
                                    databaseTableSchema,
                                    primaryKeys.toArray(new String[0]),
                                    jdbcSinkConfig.isEnableUpsert(),
                                    jdbcSinkConfig.isPrimaryKeyUpdated(),
                                    jdbcSinkConfig.isSupportUpsertByInsertOnly());
        }

        return new JdbcOutputFormat(
                connectionProvider,
                jdbcSinkConfig.getJdbcConnectionConfig(),
                statementExecutorFactory,
                commitOnFlush);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema) {
        String insertSQL =
                dialect.getInsertIntoStatement(database, table, tableSchema.getFieldNames());
        insertSQL = applyOracleAppendValuesHintIfNeeded(jdbcSinkConfig, insertSQL);
        return createSimpleBufferedExecutor(
                insertSQL, tableSchema, databaseTableSchema, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleBufferedExecutor(
            String sql,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            JdbcRowConverter rowConverter) {
        JdbcBatchStatementExecutor<SeaTunnelRow> simpleRowExecutor =
                createSimpleExecutor(sql, tableSchema, databaseTableSchema, rowConverter);
        return new BufferedBatchStatementExecutor(simpleRowExecutor, Function.identity());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertBufferedExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            String[] pkNames,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(tableSchema.toPhysicalRowDataType()::indexOf)
                        .toArray();

        TableSchema pkSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.stream(pkFields)
                                        .mapToObj(
                                                (IntFunction<Column>) tableSchema.getColumns()::get)
                                        .collect(Collectors.toList()))
                        .build();

        Function<SeaTunnelRow, SeaTunnelRow> keyExtractor = createKeyExtractor(pkFields);
        JdbcBatchStatementExecutor<SeaTunnelRow> deleteExecutor =
                createDeleteExecutor(
                        dialect, database, table, pkNames, pkSchema, databaseTableSchema);
        JdbcBatchStatementExecutor<SeaTunnelRow> upsertExecutor =
                createUpsertExecutor(
                        dialect,
                        database,
                        table,
                        tableSchema,
                        databaseTableSchema,
                        pkNames,
                        pkSchema,
                        keyExtractor,
                        enableUpsert,
                        isPrimaryKeyUpdated,
                        supportUpsertByInsertOnly);
        return new BufferReducedBatchStatementExecutor(
                upsertExecutor, deleteExecutor, keyExtractor, Function.identity());
    }

    /**
     * Builds the executor for {@code update_condition_sql}: upserts and deletes are kept in arrival
     * order and replayed together after a failed flush. The output format owns the connection and
     * prepares the statements again after a reconnect.
     */
    private static JdbcBatchStatementExecutor<SeaTunnelRow> createConditionalUpsertBufferedExecutor(
            PostgresDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            String[] pkNames,
            String upsertSQL,
            boolean oneRowPerStatement) {
        int[] pkFields =
                Arrays.stream(pkNames)
                        .mapToInt(tableSchema.toPhysicalRowDataType()::indexOf)
                        .toArray();
        TableSchema pkSchema =
                TableSchema.builder()
                        .columns(
                                Arrays.stream(pkFields)
                                        .mapToObj(
                                                (IntFunction<Column>) tableSchema.getColumns()::get)
                                        .collect(Collectors.toList()))
                        .build();
        return new OrderedBufferedBatchStatementExecutor(
                createSimpleExecutor(
                        upsertSQL, tableSchema, databaseTableSchema, dialect.getRowConverter()),
                createSimpleExecutor(
                        dialect.getConditionalDeleteStatement(database, table, pkNames),
                        pkSchema,
                        databaseTableSchema,
                        dialect.getRowConverter()),
                createKeyExtractor(pkFields),
                Function.identity(),
                oneRowPerStatement);
    }

    /**
     * Whether the PostgreSQL driver may merge the rows of a batch into one statement. The driver
     * reads the url itself, which decodes its values and lets them override the properties.
     */
    static boolean isRewriteBatchedInsertsEnabled(JdbcConnectionConfig connectionConfig) {
        Properties properties = new Properties();
        properties.putAll(connectionConfig.getProperties());
        Properties effective = Driver.parseURL(connectionConfig.getUrl(), properties);
        // A url the driver cannot read does not connect either, so assume the safe answer.
        return effective == null
                || Boolean.parseBoolean(effective.getProperty("reWriteBatchedInserts"));
    }

    /**
     * Validates {@code update_condition_sql} against the sink configuration and parses it. Called
     * when the sink is created, so that a wrong configuration fails the job before it runs, and
     * again when the writer builds its statements.
     */
    public static Optional<UpdateCondition> resolveUpdateCondition(
            JdbcDialect dialect, JdbcSinkConfig jdbcSinkConfig, TableSchema tableSchema) {
        String condition = jdbcSinkConfig.getUpdateConditionSql();
        if (condition == null) {
            return Optional.empty();
        }
        if (dialect.getClass() != PostgresDialect.class) {
            // Name the dialect class: dialects based on the PostgreSQL dialect, such as the one
            // for compatible_mode = "postgresLow", report the same dialect name.
            throw invalidUpdateCondition(
                    String.format(
                            "it is only supported by the PostgreSQL dialect (PostgresDialect,"
                                    + " without compatible_mode = \"postgresLow\"), but the sink"
                                    + " uses %s",
                            dialect.getClass().getSimpleName()));
        }
        JdbcConnectionConfig connectionConfig = jdbcSinkConfig.getJdbcConnectionConfig();
        String url = connectionConfig == null ? null : connectionConfig.getUrl();
        if (url == null || !url.startsWith(POSTGRESQL_URL_PREFIX)) {
            throw invalidUpdateCondition(
                    "it is only supported for a PostgreSQL database, the url must start with "
                            + POSTGRESQL_URL_PREFIX);
        }
        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            throw invalidUpdateCondition(
                    "it requires generate_sink_sql = true and cannot be used with query");
        }
        if (jdbcSinkConfig.isUseCopyStatement()) {
            throw invalidUpdateCondition("it cannot be used with use_copy_statement = true");
        }
        List<String> primaryKeys = jdbcSinkConfig.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw invalidUpdateCondition("it requires primary_keys to be set");
        }
        if (!jdbcSinkConfig.isEnableUpsert()) {
            throw invalidUpdateCondition("it requires enable_upsert = true");
        }
        if (jdbcSinkConfig.isSupportUpsertByInsertOnly()) {
            throw invalidUpdateCondition(
                    "it cannot be used with support_upsert_by_insert_only = true");
        }
        String[] fieldNames = tableSchema.getFieldNames();
        for (String fieldName : fieldNames) {
            if (!isBindableColumnName(fieldName, fieldNames)) {
                throw invalidUpdateCondition(
                        String.format(
                                "the column name '%s' cannot be used as a statement parameter, it"
                                        + " contains ':' or '?' or starts with a character such as a"
                                        + " space or a quote",
                                fieldName));
            }
        }
        return Optional.of(UpdateCondition.parse(condition, Arrays.asList(fieldNames)));
    }

    /**
     * Whether {@code :name} is read back as one parameter of that name. A {@code ?} is excluded as
     * well, because the statement would count it as another parameter.
     */
    private static boolean isBindableColumnName(String name, String[] fieldNames) {
        Map<String, List<Integer>> parameters = new HashMap<>();
        return name.indexOf('?') < 0
                && "?"
                        .equals(
                                FieldNamedPreparedStatement.parseNamedStatement(
                                        ":" + name, parameters, fieldNames))
                && parameters.containsKey(name);
    }

    private static JdbcConnectorException invalidUpdateCondition(String reason) {
        return new JdbcConnectorException(
                SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                "Invalid update_condition_sql: " + reason);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createUpsertExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            String[] pkNames,
            TableSchema pkTableSchema,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean enableUpsert,
            boolean isPrimaryKeyUpdated,
            boolean supportUpsertByInsertOnly) {
        if (supportUpsertByInsertOnly) {
            return createInsertOnlyExecutor(
                    dialect, database, table, tableSchema, databaseTableSchema);
        }
        if (enableUpsert) {
            Optional<String> upsertSQL =
                    dialect.getUpsertStatementByTableSchema(database, table, tableSchema, pkNames);
            if (upsertSQL.isPresent()) {
                return createSimpleExecutor(
                        upsertSQL.get(),
                        tableSchema,
                        databaseTableSchema,
                        dialect.getRowConverter());
            }
            return createInsertOrUpdateByQueryExecutor(
                    dialect,
                    database,
                    table,
                    tableSchema,
                    databaseTableSchema,
                    pkNames,
                    pkTableSchema,
                    keyExtractor,
                    isPrimaryKeyUpdated);
        }
        return createInsertOrUpdateExecutor(
                dialect,
                database,
                table,
                tableSchema,
                databaseTableSchema,
                pkNames,
                isPrimaryKeyUpdated);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createCopyInBufferStatementExecutor(
            CopyManagerBatchStatementExecutor copyManagerBatchStatementExecutor) {
        return new BufferedBatchStatementExecutor(
                copyManagerBatchStatementExecutor, Function.identity());
    }

    private static CopyManagerBatchStatementExecutor createCopyInBatchStatementExecutor(
            JdbcDialect dialect, String table, TableSchema tableSchema) {
        String columns =
                Arrays.stream(tableSchema.getFieldNames())
                        .map(dialect::quoteIdentifier)
                        .collect(Collectors.joining(",", "(", ")"));
        String copyInSql = String.format("COPY %s %s FROM STDIN WITH CSV", table, columns);
        return new CopyManagerBatchStatementExecutor(copyInSql, tableSchema);
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOnlyExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema) {
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                tableSchema,
                databaseTableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            String[] pkNames,
            boolean isPrimaryKeyUpdated) {

        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        tableSchema.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                tableSchema.getFieldNames()),
                tableSchema,
                databaseTableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createInsertOrUpdateByQueryExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            String[] pkNames,
            TableSchema pkTableSchema,
            Function<SeaTunnelRow, SeaTunnelRow> keyExtractor,
            boolean isPrimaryKeyUpdated) {
        return new InsertOrUpdateBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getRowExistsStatement(database, table, pkNames),
                                pkNames),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getInsertIntoStatement(
                                        database, table, tableSchema.getFieldNames()),
                                tableSchema.getFieldNames()),
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection,
                                dialect.getUpdateStatement(
                                        database,
                                        table,
                                        tableSchema.getFieldNames(),
                                        pkNames,
                                        isPrimaryKeyUpdated),
                                tableSchema.getFieldNames()),
                pkTableSchema,
                keyExtractor,
                tableSchema,
                databaseTableSchema,
                dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createDeleteExecutor(
            JdbcDialect dialect,
            String database,
            String table,
            String[] pkNames,
            TableSchema pkTableSchema,
            TableSchema databaseTableSchema) {
        String deleteSQL = dialect.getDeleteStatement(database, table, pkNames);
        return createSimpleExecutor(
                deleteSQL, pkTableSchema, databaseTableSchema, dialect.getRowConverter());
    }

    private static JdbcBatchStatementExecutor<SeaTunnelRow> createSimpleExecutor(
            String sql,
            TableSchema tableSchema,
            TableSchema databaseTableSchema,
            JdbcRowConverter rowConverter) {
        return new SimpleBatchStatementExecutor(
                connection ->
                        FieldNamedPreparedStatement.prepareStatement(
                                connection, sql, tableSchema.getFieldNames()),
                tableSchema,
                databaseTableSchema,
                rowConverter);
    }

    static String applyOracleAppendValuesHintIfNeeded(
            JdbcSinkConfig jdbcSinkConfig, String insertSQL) {
        if (!isOracleAppendValuesConfigured(jdbcSinkConfig)) {
            return insertSQL;
        }
        final String insertPrefix = "INSERT";
        if (!insertSQL.regionMatches(true, 0, insertPrefix, 0, insertPrefix.length())) {
            throw new IllegalArgumentException(
                    "oracle_insert_mode=APPEND_VALUES only supports generated INSERT statements.");
        }
        String appendValuesSQL =
                insertPrefix + " /*+ APPEND_VALUES */" + insertSQL.substring(insertPrefix.length());
        log.info("Oracle APPEND_VALUES insert mode is enabled, generated SQL: {}", appendValuesSQL);
        return appendValuesSQL;
    }

    private static void validateOracleInsertMode(
            JdbcDialect dialect, JdbcSinkConfig jdbcSinkConfig, List<String> primaryKeys) {
        if (!isOracleAppendValuesConfigured(jdbcSinkConfig)) {
            return;
        }
        if (!DatabaseIdentifier.ORACLE.equals(dialect.dialectName())) {
            throw new IllegalArgumentException(
                    "oracle_insert_mode=APPEND_VALUES only supports Oracle JDBC sink.");
        }
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "oracle_insert_mode=APPEND_VALUES only supports insert-only writes without primary keys.");
        }
    }

    private static boolean isOracleAppendValuesConfigured(JdbcSinkConfig jdbcSinkConfig) {
        return JdbcSinkConfig.OracleInsertMode.APPEND_VALUES.equals(
                jdbcSinkConfig.getOracleInsertMode());
    }

    static Function<SeaTunnelRow, SeaTunnelRow> createKeyExtractor(int[] pkFields) {
        return row -> {
            Object[] fields = new Object[pkFields.length];
            for (int i = 0; i < pkFields.length; i++) {
                fields[i] = row.getField(pkFields[i]);
            }
            SeaTunnelRow newRow = new SeaTunnelRow(fields);
            newRow.setTableId(row.getTableId());
            return newRow;
        };
    }
}
