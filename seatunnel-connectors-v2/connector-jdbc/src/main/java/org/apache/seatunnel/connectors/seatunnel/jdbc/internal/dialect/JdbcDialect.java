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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.ConverterLoader;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Represents a dialect of SQL implemented by a particular JDBC system. Dialects should be immutable
 * and stateless.
 */
public interface JdbcDialect extends Serializable {

    Logger log = LoggerFactory.getLogger(JdbcDialect.class.getName());

    /**
     * Get the name of jdbc dialect.
     *
     * @return the dialect name.
     */
    String dialectName();

    /**
     * Get converter that convert jdbc object to seatunnel internal object.
     *
     * @return a row converter for the database
     */
    JdbcRowConverter getRowConverter();

    /**
     * get jdbc meta-information type to seatunnel data type mapper.
     *
     * @return a type mapper for the database
     */
    JdbcDialectTypeMapper getJdbcDialectTypeMapper();

    default String hashModForField(String nativeType, String fieldName, int mod) {
        return hashModForField(fieldName, mod);
    }

    default String hashModForField(String fieldName, int mod) {
        return "ABS(MD5(" + quoteIdentifier(fieldName) + ") % " + mod + ")";
    }

    /** Quotes the identifier for table name or field name */
    default String quoteIdentifier(String identifier) {
        return identifier;
    }
    /** Quotes the identifier for database name or field name */
    default String quoteDatabaseIdentifier(String identifier) {
        return identifier;
    }

    default String tableIdentifier(String database, String tableName) {
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    /**
     * Constructs the dialects insert statement for a single row. The returned string will be used
     * as a {@link java.sql.PreparedStatement}. Fields in the statement must be in the same order as
     * the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * INSERT INTO table_name (column_name [, ...]) VALUES (value [, ...])
     * }</pre>
     *
     * @return the dialects {@code INSERT INTO} statement.
     */
    default String getInsertIntoStatement(String database, String tableName, String[] fieldNames) {
        String columns =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String placeholders =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName)
                        .collect(Collectors.joining(", "));
        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableIdentifier(database, tableName), columns, placeholders);
    }

    /**
     * Constructs the dialects update statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * UPDATE table_name SET col = val [, ...] WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code UPDATE} statement.
     */
    default String getUpdateStatement(
            String database,
            String tableName,
            String[] fieldNames,
            String[] conditionFields,
            boolean isPrimaryKeyUpdated) {

        fieldNames =
                Arrays.stream(fieldNames)
                        .filter(
                                fieldName ->
                                        isPrimaryKeyUpdated
                                                || !Arrays.asList(conditionFields)
                                                        .contains(fieldName))
                        .toArray(String[]::new);

        String setClause =
                Arrays.stream(fieldNames)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(", "));
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "UPDATE %s SET %s WHERE %s",
                tableIdentifier(database, tableName), setClause, conditionClause);
    }

    /**
     * Constructs the dialects delete statement for a single row with the given condition. The
     * returned string will be used as a {@link java.sql.PreparedStatement}. Fields in the statement
     * must be in the same order as the {@code fieldNames} parameter.
     *
     * <pre>{@code
     * DELETE FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code DELETE} statement.
     */
    default String getDeleteStatement(String database, String tableName, String[] conditionFields) {
        String conditionClause =
                Arrays.stream(conditionFields)
                        .map(fieldName -> format("%s = :%s", quoteIdentifier(fieldName), fieldName))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "DELETE FROM %s WHERE %s", tableIdentifier(database, tableName), conditionClause);
    }

    /**
     * Generates a query to determine if a row exists in the table. The returned string will be used
     * as a {@link java.sql.PreparedStatement}.
     *
     * <pre>{@code
     * SELECT 1 FROM table_name WHERE cond [AND ...]
     * }</pre>
     *
     * @return the dialects {@code QUERY} statement.
     */
    default String getRowExistsStatement(
            String database, String tableName, String[] conditionFields) {
        String fieldExpressions =
                Arrays.stream(conditionFields)
                        .map(field -> format("%s = :%s", quoteIdentifier(field), field))
                        .collect(Collectors.joining(" AND "));
        return String.format(
                "SELECT 1 FROM %s WHERE %s",
                tableIdentifier(database, tableName), fieldExpressions);
    }

    /**
     * Constructs the dialects upsert statement if supported; such as MySQL's {@code DUPLICATE KEY
     * UPDATE}, or PostgreSQL's {@code ON CONFLICT... DO UPDATE SET..}.
     *
     * <p>If supported, the returned string will be used as a {@link java.sql.PreparedStatement}.
     * Fields in the statement must be in the same order as the {@code fieldNames} parameter.
     *
     * <p>If the dialect does not support native upsert statements, the writer will fallback to
     * {@code SELECT ROW Exists} + {@code UPDATE}/{@code INSERT} which may have poor performance.
     *
     * @return the dialects {@code UPSERT} statement or {@link Optional#empty()}.
     */
    Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields);

    /**
     * Different dialects optimize their PreparedStatement
     *
     * @return The logic about optimize PreparedStatement
     */
    default PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        }
        return statement;
    }

    default ResultSetMetaData getResultSetMetaData(Connection conn, String query)
            throws SQLException {
        PreparedStatement ps = conn.prepareStatement(query);
        return ps.getMetaData();
    }

    default String extractTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    default String getFieldIde(String identifier, String fieldIde) {
        if (StringUtils.isEmpty(fieldIde)) {
            return identifier;
        }
        switch (FieldIdeEnum.valueOf(fieldIde.toUpperCase())) {
            case LOWERCASE:
                return identifier.toLowerCase();
            case UPPERCASE:
                return identifier.toUpperCase();
            default:
                return identifier;
        }
    }

    default Map<String, String> defaultParameter() {
        return new HashMap<>();
    }

    default void connectionUrlParse(
            String url, Map<String, String> info, Map<String, String> defaultParameter) {
        defaultParameter.forEach(
                (key, value) -> {
                    if (!url.contains(key) && !info.containsKey(key)) {
                        info.put(key, value);
                    }
                });
    }

    default TablePath parse(String tablePath) {
        return TablePath.of(tablePath);
    }

    default String tableIdentifier(TablePath tablePath) {
        return tablePath.getFullName();
    }

    /**
     * Approximate total number of entries in the lookup table.
     *
     * @param connection The JDBC connection object used to connect to the database.
     * @param table table info.
     * @return approximate row count statement.
     */
    default Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {
        if (StringUtils.isNotBlank(table.getQuery())) {
            return SQLUtils.countForSubquery(connection, table.getQuery());
        }
        return SQLUtils.countForTable(connection, tableIdentifier(table.getTablePath()));
    }

    /**
     * Performs a sampling operation on the specified column of a table in a JDBC-connected
     * database.
     *
     * @param connection The JDBC connection object used to connect to the database.
     * @param table The table in which the column resides.
     * @param columnName The name of the column to be sampled.
     * @param samplingRate samplingRate The inverse of the fraction of the data to be sampled from
     *     the column. For example, a value of 1000 would mean 1/1000 of the data will be sampled.
     * @return Returns a List of sampled data from the specified column.
     * @throws SQLException If an SQL error occurs during the sampling operation.
     */
    default Object[] sampleDataFromColumn(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int samplingRate,
            int fetchSize)
            throws Exception {
        String sampleQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM (%s) AS T",
                            quoteIdentifier(columnName), table.getQuery());
        } else {
            sampleQuery =
                    String.format(
                            "SELECT %s FROM %s",
                            quoteIdentifier(columnName), tableIdentifier(table.getTablePath()));
        }

        try (PreparedStatement stmt = creatPreparedStatement(connection, sampleQuery, fetchSize)) {
            log.info(String.format("Split Chunk, approximateRowCntStatement: %s", sampleQuery));
            try (ResultSet rs = stmt.executeQuery()) {
                int count = 0;
                List<Object> results = new ArrayList<>();

                while (rs.next()) {
                    count++;
                    if (count % samplingRate == 0) {
                        results.add(rs.getObject(1));
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Thread interrupted");
                    }
                }
                Object[] resultsArray = results.toArray();
                Arrays.sort(resultsArray);
                return resultsArray;
            }
        }
    }

    /**
     * Query the maximum value of the next chunk, and the next chunk must be greater than or equal
     * to <code>includedLowerBound</code> value [min_1, max_1), [min_2, max_2),... [min_n, null).
     * Each time this method is called it will return max1, max2...
     *
     * @param connection JDBC connection.
     * @param table table info.
     * @param columnName column name.
     * @param chunkSize chunk size.
     * @param includedLowerBound the previous chunk end value.
     * @return next chunk end value.
     */
    default Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quoteIdentifier(columnName);
        String sqlQuery;
        if (StringUtils.isNotBlank(table.getQuery())) {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM (%s) AS T1 WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                    + ") AS T2",
                            quotedColumn,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                    + ") AS T",
                            quotedColumn,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            quotedColumn,
                            chunkSize);
        }
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            ps.setObject(1, includedLowerBound);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                } else {
                    // this should never happen
                    throw new SQLException(
                            String.format("No result returned after running query [%s]", sqlQuery));
                }
            }
        }
    }

    default JdbcConnectionProvider getJdbcConnectionProvider(
            JdbcConnectionConfig jdbcConnectionConfig) {
        return new SimpleJdbcConnectionProvider(jdbcConnectionConfig);
    }

    /**
     * Cast column type e.g. CAST(column AS type)
     *
     * @param columnName
     * @param columnType
     * @return the text of converted column type.
     */
    default String convertType(String columnName, String columnType) {
        return columnName;
    }

    /**
     * Refresh physical table schema by schema change event
     *
     * @param sourceDialectName source dialect name
     * @param event schema change event
     * @param jdbcConnectionProvider jdbc connection provider
     * @param sinkTablePath sink table path
     */
    default void refreshTableSchemaBySchemaChangeEvent(
            String sourceDialectName,
            AlterTableColumnEvent event,
            JdbcConnectionProvider jdbcConnectionProvider,
            TablePath sinkTablePath) {}

    /**
     * generate alter table sql
     *
     * @param sourceDialectName source dialect name
     * @param event schema change event
     * @param sinkTablePath sink table path
     * @return alter table sql for sink table
     */
    default String generateAlterTableSql(
            String sourceDialectName, AlterTableColumnEvent event, TablePath sinkTablePath) {
        String tableIdentifierWithQuoted =
                tableIdentifier(sinkTablePath.getDatabaseName(), sinkTablePath.getTableName());
        switch (event.getEventType()) {
            case SCHEMA_CHANGE_ADD_COLUMN:
                Column addColumn = ((AlterTableAddColumnEvent) event).getColumn();
                return buildAlterTableSql(
                        sourceDialectName,
                        addColumn.getSourceType(),
                        AlterType.ADD.name(),
                        addColumn,
                        tableIdentifierWithQuoted,
                        StringUtils.EMPTY);
            case SCHEMA_CHANGE_DROP_COLUMN:
                String dropColumn = ((AlterTableDropColumnEvent) event).getColumn();
                return buildAlterTableSql(
                        sourceDialectName,
                        null,
                        AlterType.DROP.name(),
                        null,
                        tableIdentifierWithQuoted,
                        dropColumn);
            case SCHEMA_CHANGE_MODIFY_COLUMN:
                Column modifyColumn = ((AlterTableModifyColumnEvent) event).getColumn();
                return buildAlterTableSql(
                        sourceDialectName,
                        modifyColumn.getSourceType(),
                        AlterType.MODIFY.name(),
                        modifyColumn,
                        tableIdentifierWithQuoted,
                        StringUtils.EMPTY);
            case SCHEMA_CHANGE_CHANGE_COLUMN:
                AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                        (AlterTableChangeColumnEvent) event;
                Column changeColumn = alterTableChangeColumnEvent.getColumn();
                String oldColumnName = alterTableChangeColumnEvent.getOldColumn();
                return buildAlterTableSql(
                        sourceDialectName,
                        changeColumn.getSourceType(),
                        AlterType.CHANGE.name(),
                        changeColumn,
                        tableIdentifierWithQuoted,
                        oldColumnName);
            default:
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent for event type: " + event.getEventType());
        }
    }

    /**
     * build alter table sql
     *
     * @param sourceDialectName source dialect name
     * @param sourceColumnType source column type
     * @param alterOperation alter operation of ddl
     * @param newColumn new column after ddl
     * @param tableName table name of sink table
     * @param oldColumnName old column name before ddl
     * @return alter table sql for sink table after schema change
     */
    default String buildAlterTableSql(
            String sourceDialectName,
            String sourceColumnType,
            String alterOperation,
            Column newColumn,
            String tableName,
            String oldColumnName) {
        if (StringUtils.equals(alterOperation, AlterType.DROP.name())) {
            return String.format(
                    "ALTER TABLE %s drop column %s", tableName, quoteIdentifier(oldColumnName));
        }
        TypeConverter<?> typeConverter = ConverterLoader.loadTypeConverter(dialectName());
        BasicTypeDefine typeBasicTypeDefine = (BasicTypeDefine) typeConverter.reconvert(newColumn);

        String basicSql = buildAlterTableBasicSql(alterOperation, tableName);
        basicSql =
                decorateWithColumnNameAndType(
                        sourceDialectName,
                        sourceColumnType,
                        basicSql,
                        alterOperation,
                        newColumn,
                        oldColumnName,
                        typeBasicTypeDefine.getColumnType());
        basicSql = decorateWithNullable(basicSql, typeBasicTypeDefine);
        basicSql = decorateWithDefaultValue(basicSql, typeBasicTypeDefine);
        basicSql = decorateWithComment(basicSql, typeBasicTypeDefine);
        return basicSql + ";";
    }

    /**
     * build the body of alter table sql
     *
     * @param alterOperation alter operation of ddl
     * @param tableName table name of sink table
     * @return basic sql of alter table for sink table
     */
    default String buildAlterTableBasicSql(String alterOperation, String tableName) {
        StringBuilder sql =
                new StringBuilder(
                        "ALTER TABLE "
                                + tableName
                                + StringUtils.SPACE
                                + alterOperation
                                + StringUtils.SPACE);
        return sql.toString();
    }

    /**
     * decorate the sql with column name and type
     *
     * @param sourceDialectName source dialect name
     * @param sourceColumnType source column type
     * @param basicSql basic sql of alter table for sink table
     * @param alterOperation alter operation of ddl
     * @param newColumn new column after ddl
     * @param oldColumnName old column name before ddl
     * @param columnType column type of new column
     * @return basic sql with column name and type of alter table for sink table
     */
    default String decorateWithColumnNameAndType(
            String sourceDialectName,
            String sourceColumnType,
            String basicSql,
            String alterOperation,
            Column newColumn,
            String oldColumnName,
            String columnType) {
        StringBuilder sql = new StringBuilder(basicSql);
        String oldColumnNameWithQuoted = quoteIdentifier(oldColumnName);
        String newColumnNameWithQuoted = quoteIdentifier(newColumn.getName());
        if (alterOperation.equals(AlterType.CHANGE.name())) {
            sql.append(oldColumnNameWithQuoted)
                    .append(StringUtils.SPACE)
                    .append(newColumnNameWithQuoted)
                    .append(StringUtils.SPACE);
        } else {
            sql.append(newColumnNameWithQuoted).append(StringUtils.SPACE);
        }
        if (sourceDialectName.equals(dialectName())) {
            sql.append(sourceColumnType);
        } else {
            sql.append(columnType);
        }
        sql.append(StringUtils.SPACE);
        return sql.toString();
    }

    /**
     * decorate with nullable
     *
     * @param basicSql alter table sql for sink table
     * @param typeBasicTypeDefine type basic type define of new column
     * @return alter table sql with nullable for sink table
     */
    default String decorateWithNullable(String basicSql, BasicTypeDefine typeBasicTypeDefine) {
        StringBuilder sql = new StringBuilder(basicSql);
        if (typeBasicTypeDefine.isNullable()) {
            sql.append("NULL ");
        } else {
            sql.append("NOT NULL ");
        }
        return sql.toString();
    }

    /**
     * decorate with default value
     *
     * @param basicSql alter table sql for sink table
     * @param typeBasicTypeDefine type basic type define of new column
     * @return alter table sql with default value for sink table
     */
    default String decorateWithDefaultValue(String basicSql, BasicTypeDefine typeBasicTypeDefine) {
        Object defaultValue = typeBasicTypeDefine.getDefaultValue();
        if (Objects.nonNull(defaultValue)
                && needsQuotesWithDefaultValue(typeBasicTypeDefine.getColumnType())
                && !isSpecialDefaultValue(defaultValue)) {
            defaultValue = quotesDefaultValue(defaultValue);
        }
        StringBuilder sql = new StringBuilder(basicSql);
        if (Objects.nonNull(defaultValue)) {
            sql.append("DEFAULT ").append(defaultValue).append(StringUtils.SPACE);
        }
        return sql.toString();
    }

    /**
     * decorate with comment
     *
     * @param basicSql alter table sql for sink table
     * @param typeBasicTypeDefine type basic type define of new column
     * @return alter table sql with comment for sink table
     */
    default String decorateWithComment(String basicSql, BasicTypeDefine typeBasicTypeDefine) {
        String comment = typeBasicTypeDefine.getComment();
        StringBuilder sql = new StringBuilder(basicSql);
        if (StringUtils.isNotBlank(comment)) {
            sql.append("COMMENT '").append(comment).append("'");
        }
        return sql.toString();
    }

    /**
     * whether quotes with default value
     *
     * @param sqlType sql type of column
     * @return whether needs quotes with the type
     */
    default boolean needsQuotesWithDefaultValue(String sqlType) {
        return false;
    }

    /**
     * whether is special default value e.g. current_timestamp
     *
     * @param defaultValue default value of column
     * @return whether is special default value e.g current_timestamp
     */
    default boolean isSpecialDefaultValue(Object defaultValue) {
        return false;
    }

    /**
     * quotes default value
     *
     * @param defaultValue default value of column
     * @return quoted default value
     */
    default String quotesDefaultValue(Object defaultValue) {
        return "'" + defaultValue + "'";
    }

    enum AlterType {
        ADD,
        DROP,
        MODIFY,
        CHANGE
    }
}
