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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DB2Catalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT COLNAME AS COLUMN_NAME,\n"
                    + "       TYPENAME AS DATA_TYPE,\n"
                    + "       LENGTH,\n"
                    + "       SCALE,\n"
                    + "       NULLS,\n"
                    + "       \"DEFAULT\" AS DEFAULT_VALUE,\n"
                    + "       REMARKS AS COMMENT,\n"
                    + "       COLNO\n"
                    + "FROM SYSCAT.COLUMNS\n"
                    + "WHERE TABSCHEMA = '%s' AND TABNAME = '%s'\n"
                    + "ORDER BY COLNO";

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(
                "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1 WHERE UPPER(CURRENT SERVER) = UPPER('%s')",
                databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES "
                        + "WHERE TYPE = 'T' AND TABSCHEMA = '%s' AND TABNAME = '%s'",
                resolveSchema(tablePath), tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT TABSCHEMA, TABNAME FROM SYSCAT.TABLES "
                + "WHERE TYPE = 'T' AND TABSCHEMA NOT LIKE 'SYS%' "
                + "ORDER BY TABSCHEMA, TABNAME";
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, resolveSchema(tablePath), tablePath.getTableName());
    }

    @Override
    protected Optional<String> getTableComment(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        String sql =
                String.format(
                        "SELECT REMARKS FROM SYSCAT.TABLES "
                                + "WHERE TYPE = 'T' AND TABSCHEMA = '%s' AND TABNAME = '%s'",
                        resolveSchema(tablePath), tablePath.getTableName());
        List<String> comments =
                queryString(
                        getUrlFromDatabaseName(tablePath.getDatabaseName()),
                        sql,
                        rs -> rs.getString("REMARKS"));
        if (comments.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(comments.get(0));
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String dataType = resultSet.getString("DATA_TYPE");
        long length = resultSet.getLong("LENGTH");
        int scale = resultSet.getInt("SCALE");
        boolean nullable = "Y".equalsIgnoreCase(resultSet.getString("NULLS"));

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(dataType)
                        .dataType(dataType)
                        .length(length)
                        .precision(length)
                        .scale(scale)
                        .nullable(nullable)
                        .defaultValue(resultSet.getObject("DEFAULT_VALUE"))
                        .comment(resultSet.getString("COMMENT"))
                        .build();
        return DB2TypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        PrimaryKey primaryKey = table.getTableSchema().getPrimaryKey();
        Set<String> primaryKeyColumns =
                primaryKey == null || primaryKey.getColumnNames() == null
                        ? Collections.emptySet()
                        : new HashSet<>(primaryKey.getColumnNames());
        String columnSql =
                table.getTableSchema().getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(column -> buildCreateColumnSql(column, primaryKeyColumns))
                        .collect(Collectors.joining(", "));
        if (primaryKey != null && primaryKey.getColumnNames() != null) {
            String primaryKeySql =
                    primaryKey.getColumnNames().stream()
                            .map(this::quoteIdentifier)
                            .collect(Collectors.joining(", "));
            columnSql = columnSql + ", PRIMARY KEY (" + primaryKeySql + ")";
        }
        return String.format("CREATE TABLE %s (%s)", quoteSchemaTable(tablePath), columnSql);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", quoteSchemaTable(tablePath));
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE %s", quoteIdentifier(databaseName));
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE %s", quoteIdentifier(databaseName));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DB2TypeMapper());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format("TRUNCATE TABLE %s IMMEDIATE", quoteSchemaTable(tablePath));
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT 1 FROM %s FETCH FIRST 1 ROW ONLY", quoteSchemaTable(tablePath));
    }

    private String buildCreateColumnSql(Column column, Set<String> primaryKeyColumns) {
        BasicTypeDefine typeDefine = DB2TypeConverter.INSTANCE.reconvert(column);
        // DB2 rejects a primary key if any key column is nullable, while JDBC query metadata can
        // lose the original NOT NULL flag when generate_sink_sql builds the sink schema.
        boolean nullable = column.isNullable() && !primaryKeyColumns.contains(column.getName());
        return String.format(
                "%s %s%s",
                quoteIdentifier(column.getName()),
                typeDefine.getColumnType(),
                nullable ? "" : " NOT NULL");
    }

    private String quoteSchemaTable(TablePath tablePath) {
        return quoteIdentifier(resolveSchema(tablePath))
                + "."
                + quoteIdentifier(tablePath.getTableName());
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String resolveSchema(TablePath tablePath) {
        if (StringUtils.isNotBlank(tablePath.getSchemaName())) {
            return tablePath.getSchemaName();
        }
        Optional<String> schema = defaultSchema.filter(StringUtils::isNotBlank);
        if (schema.isPresent()) {
            return schema.get();
        }
        return username.toUpperCase(Locale.ROOT);
    }

    @Override
    protected Optional<PrimaryKey> getPrimaryKey(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        String sql =
                String.format(
                        "SELECT k.COLNAME, c.CONSTNAME "
                                + "FROM SYSCAT.KEYCOLUSE k "
                                + "JOIN SYSCAT.TABCONST c "
                                + "ON k.CONSTNAME = c.CONSTNAME "
                                + "AND k.TABSCHEMA = c.TABSCHEMA "
                                + "AND k.TABNAME = c.TABNAME "
                                + "WHERE c.TYPE = 'P' "
                                + "AND k.TABSCHEMA = '%s' "
                                + "AND k.TABNAME = '%s' "
                                + "ORDER BY k.COLSEQ",
                        resolveSchema(tablePath), tablePath.getTableName());
        String primaryKeyName = null;
        List<String> primaryKeyColumns = new ArrayList<>();
        try (PreparedStatement statement =
                        getConnection(getUrlFromDatabaseName(tablePath.getDatabaseName()))
                                .prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                primaryKeyName = resultSet.getString("CONSTNAME");
                primaryKeyColumns.add(resultSet.getString("COLNAME"));
            }
        }
        if (primaryKeyColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(primaryKeyName, primaryKeyColumns));
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, TablePath tablePath) {
        return Collections.emptyList();
    }
}
