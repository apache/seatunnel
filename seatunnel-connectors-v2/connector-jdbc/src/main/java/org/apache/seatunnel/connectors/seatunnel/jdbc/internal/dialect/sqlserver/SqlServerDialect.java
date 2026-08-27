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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.SQLUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_NCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_NTEXT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_NVARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_SQLVARIANT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_TEXT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_UNIQUEIDENTIFIER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_VARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter.SQLSERVER_XML;

@Slf4j
public class SqlServerDialect implements JdbcDialect {

    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();

    public SqlServerDialect() {}

    public SqlServerDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.SQLSERVER;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new SqlserverJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new SqlserverTypeMapper();
    }

    @Override
    public String hashModForField(String fieldName, int mod) {
        return "ABS(HASHBYTES('MD5', " + quoteIdentifier(fieldName) + ") % " + mod + ")";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] pkNames) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(pkNames).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));

        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(pkNames)
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));
        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "[TARGET].%s=[SOURCE].%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));
        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "[SOURCE]." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        "MERGE INTO %s.%s AS [TARGET]"
                                + " USING (%s) AS [SOURCE]"
                                + " ON (%s)"
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s);",
                        quoteDatabaseIdentifier(database),
                        quoteIdentifier(tableName),
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("[").append(parts[i]).append("]").append(".");
            }
            return sb.append("[")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("]")
                    .toString();
        }

        return "[" + getFieldIde(identifier, fieldIde) + "]";
    }

    @Override
    public String quoteDatabaseIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return quoteIdentifier(tablePath.getFullName());
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
            TablePath tablePath = table.getTablePath();
            try (Statement stmt = connection.createStatement()) {
                if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                    String useDatabaseStatement =
                            String.format(
                                    "USE %s;",
                                    quoteDatabaseIdentifier(tablePath.getDatabaseName()));
                    log.info("Split Chunk, approximateRowCntStatement: {}", useDatabaseStatement);
                    stmt.execute(useDatabaseStatement);
                }
                String rowCountQuery =
                        String.format(
                                "SELECT Total_Rows = SUM(st.row_count) FROM sys"
                                        + ".dm_db_partition_stats st WHERE object_name(object_id) = '%s' AND index_id < 2;",
                                tablePath.getTableName());
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
    public Object queryNextChunkMax(
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
                                    + "SELECT TOP (%s) %s FROM (%s) AS T1 WHERE %s >= ? ORDER BY %s ASC"
                                    + ") AS T2",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            table.getQuery(),
                            quotedColumn,
                            quotedColumn);
        } else {
            sqlQuery =
                    String.format(
                            "SELECT MAX(%s) FROM ("
                                    + "SELECT TOP (%s) %s FROM %s WHERE %s >= ? ORDER BY %s ASC "
                                    + ") AS T",
                            quotedColumn,
                            chunkSize,
                            quotedColumn,
                            tableIdentifier(table.getTablePath()),
                            quotedColumn,
                            quotedColumn);
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
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return SqlServerTypeConverter.INSTANCE;
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableAddColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        Column column = event.getColumn();
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = resolveColumnType(sameCatalog, column, typeDefine);

        // Build the SQL statement that add the column
        StringBuilder sqlBuilder =
                buildAlterTablePrefix(tablePath)
                        .append(" ADD ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType)
                        .append(" ");

        if (column.getDefaultValue() != null) {
            // Handle default values
            String defaultValueClause = sqlClauseWithDefaultValue(typeDefine, sourceDialectName);
            sqlBuilder.append(defaultValueClause);
        }

        if (!column.isNullable()) {
            if (column.getDefaultValue() != null) {
                // A DEFAULT is present — SQL Server can populate existing rows, so NOT NULL is
                // safe.
                sqlBuilder.append(" NOT NULL");
            } else {
                // SQL Server forbids adding a NOT NULL column without a DEFAULT to a non-empty
                // table.
                // Add as NULL so that existing rows are not affected; subsequent CDC UPDATE events
                // will fill in the actual values for those rows.
                log.warn(
                        "Column '{}' in table {} is NOT NULL but has no DEFAULT; adding as NULL "
                                + "to allow addition to a non-empty table.",
                        column.getName(),
                        tablePath.getFullName());
                sqlBuilder.append(" NULL");
            }
        }

        ddlSQL.add(sqlBuilder.toString());
        // Process column comment
        if (column.getComment() != null) {
            ddlSQL.add(buildColumnCommentSQL(tablePath, column));
        }

        // Execute the DDL statement
        executeDDL(connection, ddlSQL);
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableChangeColumnEvent event)
            throws SQLException {
        List<String> ddlSQL = new ArrayList<>();
        if (event.getOldColumn() != null
                && !(event.getColumn().getName().equals(event.getOldColumn()))) {
            String renameObject = buildRenameColumnObject(tablePath, event.getOldColumn());
            StringBuilder sqlBuilder =
                    new StringBuilder()
                            .append("EXEC ")
                            .append(buildRenameProcedure(tablePath))
                            .append(" ")
                            .append(String.format("'%s', ", renameObject))
                            .append(String.format("'%s', 'COLUMN';", event.getColumn().getName()));
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
        Column column = event.getColumn();
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = resolveColumnType(sameCatalog, column, typeDefine);
        List<String> ddlSQL = new ArrayList<>();
        // Handle field default constraints.
        if (column.getDefaultValue() != null) {
            if (sameCatalog
                    || !isSpecialDefaultValue(typeDefine.getDefaultValue(), sourceDialectName)) {
                String constraintQuery =
                        String.format(
                                "SELECT dc.name AS constraint_name\n"
                                        + "FROM sys.default_constraints dc \n"
                                        + "JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \n"
                                        + "JOIN sys.tables t ON c.object_id = t.object_id \n"
                                        + "JOIN sys.schemas s ON t.schema_id = s.schema_id \n"
                                        + "WHERE t.name = '%s' AND s.name = '%s' AND c.name = '%s';",
                                tablePath.getTableName(),
                                tablePath.getSchemaName(),
                                event.getColumn().getName());

                try (Statement stmt = connection.createStatement();
                        ResultSet rs = stmt.executeQuery(constraintQuery)) {
                    while (rs.next()) {
                        String constraintName = rs.getString(1);
                        if (StringUtils.isBlank(constraintName)) {
                            continue;
                        }
                        StringBuilder dropConstraintSQL =
                                buildAlterTablePrefix(tablePath)
                                        .append(" DROP CONSTRAINT ")
                                        .append(quoteIdentifier(constraintName));
                        ddlSQL.add(dropConstraintSQL.toString());
                    }
                }

                // Process column default
                String defaultValueClause =
                        sqlClauseWithDefaultValue(typeDefine, sourceDialectName);
                if (StringUtils.isNotBlank(defaultValueClause)) {
                    StringBuilder defaultSqlBuilder =
                            buildAlterTablePrefix(tablePath)
                                    .append(" ADD ")
                                    .append(defaultValueClause)
                                    .append(" FOR ")
                                    .append(quoteIdentifier(column.getName()));
                    ddlSQL.add(defaultSqlBuilder.toString());
                }
            } else {
                log.warn(
                        "Skipping unsupported default value for column {} in table {}.",
                        column.getName(),
                        tablePath.getFullName());
            }
        }

        // Process column comment
        if (column.getComment() != null) {
            ddlSQL.add(buildColumnCommentSQL(tablePath, column));
        }

        // Build the SQL statement that modifies the column
        StringBuilder sqlBuilder =
                buildAlterTablePrefix(tablePath)
                        .append(" ALTER COLUMN ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType);
        boolean targetColumnNullable = columnIsNullable(connection, tablePath, column.getName());
        if (column.isNullable() != targetColumnNullable && !targetColumnNullable) {
            sqlBuilder.append(" NULL ");
        }
        ddlSQL.add(sqlBuilder.toString());

        // Execute the DDL statement
        executeDDL(connection, ddlSQL);
    }

    @Override
    public void applySchemaChange(
            Connection connection, TablePath tablePath, AlterTableDropColumnEvent event)
            throws SQLException {
        // Handle field`s constraints.
        String constraintQuery =
                String.format(
                        "SELECT dc.name AS constraint_name\n"
                                + "FROM sys.default_constraints dc \n"
                                + "JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \n"
                                + "JOIN sys.tables t ON c.object_id = t.object_id \n"
                                + "JOIN sys.schemas s ON t.schema_id = s.schema_id \n"
                                + "WHERE t.name = '%s' AND c.name = '%s' and s.name = '%s';",
                        tablePath.getTableName(), event.getColumn(), tablePath.getSchemaName());

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(constraintQuery)) {
            while (rs.next()) {
                String constraintName = rs.getString(1);
                String dropConstraintSQL =
                        String.format(
                                "ALTER TABLE %s DROP CONSTRAINT %s",
                                tableIdentifier(tablePath), quoteIdentifier(constraintName));
                try (Statement dropStmt = connection.createStatement()) {
                    log.info("Executing drop constraint SQL: {}", dropConstraintSQL);
                    dropStmt.execute(dropConstraintSQL);
                }
            }
        }

        String dropColumnSQL =
                String.format(
                        "ALTER TABLE %s DROP COLUMN %s",
                        tableIdentifier(tablePath), quoteIdentifier(event.getColumn()));
        try (Statement statement = connection.createStatement()) {
            log.info("Executing drop column SQL: {}", dropColumnSQL);
            statement.execute(dropColumnSQL);
        }
    }

    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        String sqlServerType = columnDefine.getDataType();
        switch (sqlServerType) {
            case SQLSERVER_CHAR:
            case SQLSERVER_VARCHAR:
            case SQLSERVER_NCHAR:
            case SQLSERVER_NVARCHAR:
            case SQLSERVER_TEXT:
            case SQLSERVER_NTEXT:
            case SQLSERVER_XML:
            case SQLSERVER_UNIQUEIDENTIFIER:
            case SQLSERVER_SQLVARIANT:
                return true;
            default:
                return false;
        }
    }

    private void executeDDL(Connection connection, List<String> ddlSQL) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : ddlSQL) {
                log.info("Executing SqlServer SQL: {}", sql);
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new SQLException("Error executing SqlServer SQL: " + ddlSQL, e.getSQLState(), e);
        }
    }

    private String buildColumnCommentSQL(TablePath tablePath, Column column) {
        return String.format(
                "EXEC %s.sys.sp_updateextendedproperty 'MS_Description', N'%s', 'schema', N'%s', "
                        + "'table', N'%s', 'column', N'%s';",
                tablePath.getDatabaseName(),
                column.getComment(),
                tablePath.getSchemaName(),
                tablePath.getTableName(),
                column.getName());
    }

    private boolean columnIsNullable(Connection connection, TablePath tablePath, String column)
            throws SQLException {
        // Prefix with the target database name so the query works even when the JDBC connection
        // is connected to a different database (e.g. master with no databaseName in the URL).
        String databaseName = tablePath.getDatabaseName();
        String infoSchemaPrefix =
                StringUtils.isNotBlank(databaseName)
                        ? quoteDatabaseIdentifier(databaseName) + ".INFORMATION_SCHEMA"
                        : "INFORMATION_SCHEMA";
        String selectColumnSQL =
                String.format(
                        "SELECT IS_NULLABLE FROM %s.COLUMNS WHERE %s AND COLUMN_NAME = '%s';",
                        infoSchemaPrefix, buildCommonWhereClause(tablePath), column);
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(selectColumnSQL);
            if (!rs.next()) {
                // Column not found — default to non-nullable to avoid incorrectly appending
                // NULL to an ALTER COLUMN statement for a column that doesn't allow nulls.
                log.warn(
                        "Column '{}' not found in {}.COLUMNS for table {}; assuming NOT NULL",
                        column,
                        infoSchemaPrefix,
                        tablePath.getFullName());
                return false;
            }
            return rs.getString("IS_NULLABLE").equals("YES");
        }
    }

    /**
     * Returns the SQL column type string to use in DDL statements.
     *
     * <p>When source and sink are the same catalog (SQL Server to SQL Server) we prefer the
     * original {@code sourceType} because it already carries the full type expression (e.g. {@code
     * varchar(255)}). However, when the CDC schema-change path produces a column whose {@code
     * sourceType} is a bare type name without length/precision (e.g. {@code varchar} instead of
     * {@code varchar(255)}), SQL Server rejects the resulting DDL statement. In that case we fall
     * back to {@code typeDefine.getColumnType()}, which is always fully qualified.
     */
    private String resolveColumnType(
            boolean sameCatalog, Column column, BasicTypeDefine typeDefine) {
        if (!sameCatalog) {
            return typeDefine.getColumnType();
        }
        String sourceType = column.getSourceType();
        if (StringUtils.isBlank(sourceType) || !sourceType.contains("(")) {
            // Bare type name (no length/precision); use the fully-qualified type from
            // the type converter to avoid SQL Server DDL syntax errors.
            return typeDefine.getColumnType();
        }
        return sourceType;
    }

    private StringBuilder buildAlterTablePrefix(TablePath tablePath) {
        return new StringBuilder("ALTER TABLE ").append(tableIdentifier(tablePath));
    }

    private String buildCommonWhereClause(TablePath tablePath) {
        return String.format(
                "TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                tablePath.getDatabaseName(), tablePath.getSchemaName(), tablePath.getTableName());
    }

    private String buildRenameColumnObject(TablePath tablePath, String oldColumn) {
        List<String> objectParts = new ArrayList<>();
        if (StringUtils.isNotBlank(tablePath.getSchemaName())) {
            objectParts.add(tablePath.getSchemaName());
        }
        objectParts.add(tablePath.getTableName());
        objectParts.add(oldColumn);
        return String.join(".", objectParts);
    }

    private String buildRenameProcedure(TablePath tablePath) {
        if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
            return quoteDatabaseIdentifier(tablePath.getDatabaseName()) + ".sys.sp_rename";
        }
        return "sp_rename";
    }
}
