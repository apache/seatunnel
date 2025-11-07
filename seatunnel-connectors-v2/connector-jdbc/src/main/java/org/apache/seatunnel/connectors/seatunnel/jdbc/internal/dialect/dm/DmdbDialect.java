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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

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

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_CHARACTER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_LONG;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_LONGVARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_NVARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_TEXT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_VARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_VARCHAR2;

@Slf4j
public class DmdbDialect implements JdbcDialect {

    public String fieldIde;

    public DmdbDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    @Override
    public String dialectName() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new DmdbJdbcRowConverter();
    }

    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new DmdbTypeMapper();
    }

    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String usingClause = String.format("SELECT %s", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));

        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));

        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        // If there is a schema in the sql of dm, an error will be reported.
        // This is compatible with the case that the schema is written or not written in the conf
        // configuration file
        String databaseName = tableIdentifier(database, tableName);
        String upsertSQL =
                String.format(
                        " MERGE INTO %s TARGET"
                                + " USING (%s) SOURCE"
                                + " ON (%s) "
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s)",
                        databaseName,
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tablePath.getSchemaAndTableName("\"");
    }

    // Compatibility Both database = mode and table-names = schema.tableName are configured
    @Override
    public String tableIdentifier(String database, String tableName) {
        if (database == null) {
            return quoteIdentifier(tableName);
        }
        if (tableName.contains(".")) {
            return quoteIdentifier(tableName);
        }
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
    public TypeConverter<BasicTypeDefine> getTypeConverter() {
        return DmdbTypeConverter.INSTANCE;
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
        String columnType = sameCatalog ? column.getSourceType() : typeDefine.getColumnType();

        // Build the SQL statement that add the column
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE ")
                        .append(tableIdentifier(tablePath))
                        .append(" ADD ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType);

        if (column.getDefaultValue() != null
                && !column.isNullable()
                && (sameCatalog
                        || !isSpecialDefaultValue(
                                typeDefine.getDefaultValue(), sourceDialectName))) {
            // Handle default values and null constraints
            String defaultValueClause = sqlClauseWithDefaultValue(typeDefine, sourceDialectName);
            sqlBuilder.append(" NOT NULL ").append(defaultValueClause);
        } else {
            // If the column is nullable or the default value is not supported,
            // the NULL constraint is added.
            if (column.getDefaultValue() != null
                    && isSpecialDefaultValue(typeDefine.getDefaultValue(), sourceDialectName)) {
                log.warn(
                        "Skipping unsupported default value for column {} in table {}. Using NULL constraint instead.",
                        column.getName(),
                        tablePath.getFullName());
            }
            sqlBuilder.append(" NULL");
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
        Column column = event.getColumn();
        String sourceDialectName = event.getSourceDialectName();
        boolean sameCatalog = StringUtils.equals(dialectName(), sourceDialectName);
        // string conversion length will be extended by 4 in cross-database.
        // eg: mysql varchar(10) -> Dameng varchar(40)
        BasicTypeDefine typeDefine = getTypeConverter().reconvert(column);
        String columnType = sameCatalog ? column.getSourceType() : typeDefine.getColumnType();
        if (event.getTypeChanged() != null
                && event.getTypeChanged()
                && DM_TEXT.equals(typeDefine.getColumnType())) {
            log.warn(
                    "DamengDB does not support modifying the TEXT type directly. "
                            + "Please use ALTER TABLE MODIFY COLUMN to change the column type.");
        }
        // Build the SQL statement that modifies the column
        StringBuilder sqlBuilder =
                new StringBuilder("ALTER TABLE ")
                        .append(tableIdentifier(tablePath))
                        .append(" MODIFY ")
                        .append(quoteIdentifier(column.getName()))
                        .append(" ")
                        .append(columnType);

        // Handle null constraints
        // DamengDB does not direct support modifying the NULL to NOT-NUll constraint directly.
        // if supported, need update null value to defaultvalue, then modify the column to NOT NULL.
        // this is a high-risk operation, so we do not support it.
        boolean targetColumnNullable = columnIsNullable(connection, tablePath, column.getName());
        if (column.isNullable() != targetColumnNullable && !targetColumnNullable) {
            sqlBuilder.append(" NULL ");
        }

        // Handle default value
        if (column.getDefaultValue() != null) {
            if (sameCatalog
                    || !isSpecialDefaultValue(typeDefine.getDefaultValue(), sourceDialectName)) {
                String defaultValueClause =
                        sqlClauseWithDefaultValue(typeDefine, sourceDialectName);
                sqlBuilder.append(" ").append(defaultValueClause);
            } else {
                log.warn(
                        "Skipping unsupported default value for column {} in table {}.",
                        column.getName(),
                        tablePath.getFullName());
            }
        }
        List<String> ddlSQL = new ArrayList<>();
        ddlSQL.add(sqlBuilder.toString());
        // Process column comment
        if (column.getComment() != null) {
            ddlSQL.add(buildColumnCommentSQL(tablePath, column));
        }
        // Execute the DDL statement
        executeDDL(connection, ddlSQL);
    }

    @Override
    public boolean needsQuotesWithDefaultValue(BasicTypeDefine columnDefine) {
        String dmDataType = columnDefine.getDataType();
        switch (dmDataType) {
            case DM_CHAR:
            case DM_CHARACTER:
            case DM_VARCHAR:
            case DM_VARCHAR2:
            case DM_NVARCHAR:
            case DM_LONGVARCHAR:
            case DM_CLOB:
            case DM_TEXT:
            case DM_LONG:
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
        } catch (SQLException e) {
            throw new SQLException("Error executing DDL SQL: " + ddlSQL, e.getSQLState(), e);
        }
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
                        + "        NULLABLE FROM"
                        + "        ALL_TAB_COLUMNS c"
                        + "        WHERE c.owner = '"
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
            rs.next();
            return rs.getString("NULLABLE").equals("Y");
        }
    }
}
