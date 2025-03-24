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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_CHARACTER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_TEXT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_VARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter.PG_XML;

@Slf4j
public class PostgresDialect implements JdbcDialect {

    private static final long serialVersionUID = -5834746193472465218L;
    public static final int DEFAULT_POSTGRES_FETCH_SIZE = 128;

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public PostgresDialect() {}

    public PostgresDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.POSTGRESQL;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new PostgresJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new PostgresTypeMapper();
    }

    @Override
    public String hashModForField(String nativeType, String fieldName, int mod) {
        String quoteFieldName = quoteIdentifier(fieldName);
        if (StringUtils.isNotBlank(nativeType)) {
            quoteFieldName = convertType(quoteFieldName, nativeType);
        }
        return "(ABS(HASHTEXT(" + quoteFieldName + ")) % " + mod + ")";
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return hashModForField(null, fieldName, mod);
    }

    @Override
    public Object queryNextChunkMax(
            Connection connection,
            JdbcSourceTable table,
            String columnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        Map<String, Column> columns =
                table.getCatalogTable().getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(c -> c.getName(), c -> c));
        Column column = columns.get(columnName);

        String quotedColumn = quoteIdentifier(columnName);
        quotedColumn = convertType(quotedColumn, column.getSourceType());
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

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        String uniqueColumns =
                Arrays.stream(uniqueKeyFields)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String updateClause =
                Arrays.stream(fieldNames)
                        .map(
                                fieldName ->
                                        quoteIdentifier(fieldName)
                                                + "=EXCLUDED."
                                                + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "%s ON CONFLICT (%s) DO UPDATE SET %s",
                        getInsertIntoStatement(database, tableName, fieldNames),
                        uniqueColumns,
                        updateClause);
        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        // use cursor mode, reference:
        // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
        connection.setAutoCommit(false);
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_POSTGRES_FETCH_SIZE);
        }
        return statement;
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        // resolve pg database name upper or lower not recognised
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }

        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tablePath.getFullNameWithQuoted("\"");
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public Long approximateRowCntStatement(Connection connection, JdbcSourceTable table)
            throws SQLException {

        // 1. If no query is configured, use TABLE STATUS.
        // 2. If a query is configured but does not contain a WHERE clause and tablePath is
        // configured, use TABLE STATUS.
        // 3. If a query is configured with a WHERE clause, or a query statement is configured but
        // tablePath is TablePath.DEFAULT, use COUNT(*).

        boolean useTableStats =
                StringUtils.isBlank(table.getQuery())
                        || (!table.getQuery().toLowerCase().contains("where")
                                && table.getTablePath() != null
                                && !TablePath.DEFAULT
                                        .getFullName()
                                        .equals(table.getTablePath().getFullName()));
        if (useTableStats) {
            String rowCountQuery =
                    String.format(
                            "SELECT reltuples FROM pg_class r WHERE relkind = 'r' AND relname = '%s';",
                            table.getTablePath().getTableName());
            try (Statement stmt = connection.createStatement()) {
                log.info("Split Chunk, approximateRowCntStatement: {}", rowCountQuery);
                try (ResultSet rs = stmt.executeQuery(rowCountQuery)) {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(1);
                }
            }
        }
        return SQLUtils.countForSubquery(connection, table.getQuery());
    }

    @Override
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return PostgresTypeConverter.INSTANCE;
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        ddlSQL.add(buildAddColumnSQL(tablePath, event));

        if (event.getColumn().getComment() != null) {
            ddlSQL.add(buildColumnCommentSQL(tablePath, event.getColumn()));
        }
        executeDDL(connection, ddlSQL);
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        if (event.getOldColumn() != null
                && !(event.getColumn().getName().equals(event.getOldColumn()))) {
            StringBuilder sqlBuilder =
                    new StringBuilder()
                            .append("ALTER TABLE ")
                            .append(tableIdentifier(tablePath))
                            .append(" RENAME COLUMN ")
                            .append(quoteIdentifier(event.getOldColumn()))
                            .append(" TO ")
                            .append(quoteIdentifier(event.getColumn().getName()));
            ddlSQL.add(sqlBuilder.toString());
        }

        executeDDL(connection, ddlSQL);

        if (event.getColumn().getDataType() != null) {
            applySchemaChange(
                    connection,
                    tablePath,
                    AlterTableModifyColumnEvent.modify(event.tableIdentifier(), event.getColumn()));
        }
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = buildUpdateColumnSQL(connection, tablePath, event);
        if (event.getColumn().getComment() != null) {
            ddlSQL.add(buildColumnCommentSQL(tablePath, event.getColumn()));
        }
        executeDDL(connection, ddlSQL);
    }

    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        String pgDataType = columnDefine.getDataType().toLowerCase();
        switch (pgDataType) {
            case PG_CHAR:
            case PG_VARCHAR:
            case PG_TEXT:
            case PG_CHARACTER:
            case PG_XML:
                return true;
            default:
                return false;
        }
    }

    private void executeDDL(Connection connection, List<String> ddlSQL) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : ddlSQL) {
                log.info("Executing DDL SQL: {}", sql);
                statement.execute(sql);
            }
        }
    }

    private String buildAddColumnSQL(TablePath tablePath, AlterTableAddColumnEvent event) {
        Column column = event.getColumn();
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = sameCatalog ? column.getSourceType() : typeDefine.getColumnType();
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(tableIdentifier(tablePath))
                        .append(" ADD ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType);
        if (column.getDefaultValue() == null) {
            sqlBuilder.append(" NULL");
        } else {
            if (column.isNullable()) {
                sqlBuilder.append(" NULL");
            } else if (sameCatalog
                    || !isSpecialDefaultValue(typeDefine.getDefaultValue(), sourceDialectName)) {
                sqlBuilder
                        .append(" NOT NULL")
                        .append(" ")
                        .append(sqlClauseWithDefaultValue(typeDefine, sourceDialectName));
            } else {
                log.warn(
                        "Skipping unsupported default value for column {} in table {}.",
                        column.getName(),
                        tablePath.getFullName());
                sqlBuilder.append(" NULL");
            }
        }
        return sqlBuilder.toString();
    }

    private List<String> buildUpdateColumnSQL(
            Connection connection, TablePath tablePath, AlterTableModifyColumnEvent event)
            throws SQLException {
        List<String> ddlSQl = new ArrayList<>();
        Column column = event.getColumn();
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = sameCatalog ? column.getSourceType() : typeDefine.getColumnType();
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(tableIdentifier(tablePath))
                        .append(" ALTER COLUMN ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append("TYPE ")
                        .append(columnType);
        ddlSQl.add(sqlBuilder.toString());
        boolean targetColumnNullable = columnIsNullable(connection, tablePath, column.getName());
        if (column.isNullable() != targetColumnNullable) {
            ddlSQl.add(
                    String.format(
                            "ALTER TABLE %s ALTER COLUMN %s %s NOT NULL",
                            tablePath,
                            quoteIdentifier(column.getName()),
                            column.isNullable() ? "DROP" : "SET"));
        }
        return ddlSQl;
    }

    private String buildColumnCommentSQL(TablePath tablePath, Column column) {
        return String.format(
                "COMMENT ON COLUMN %s.%s IS '%s'",
                tableIdentifier(tablePath), quoteIdentifier(column.getName()), column.getComment());
    }

    private boolean columnIsNullable(Connection connection, TablePath tablePath, String column)
            throws SQLException {
        String selectColumnSQL =
                "SELECT"
                        + "        is_nullable FROM"
                        + "        information_schema.columns c"
                        + "        WHERE c.table_catalog = '"
                        + tablePath.getDatabaseName()
                        + "'"
                        + "        AND c.table_schema = '"
                        + tablePath.getSchemaName()
                        + "'"
                        + "        AND c.table_name = '"
                        + tablePath.getTableName()
                        + "'"
                        + "        AND c.column_name = '"
                        + column
                        + "'";
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(selectColumnSQL);
            if (rs.next()) {
                return rs.getString("is_nullable").equals("YES");
            }
            return false;
        }
    }

    public String convertType(String columnName, String columnType) {
        if (PostgresTypeConverter.PG_UUID.equals(columnType)) {
            return columnName + "::text";
        }
        return columnName;
    }
}
