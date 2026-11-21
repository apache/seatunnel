/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class DamengCatalog extends AbstractJdbcCatalog {

    /** Sink {@code table_options} key for Dameng table tablespace ({@code STORAGE (ON ...)}). */
    public static final String TABLE_OPTION_TABLESPACE = "tablespace";

    /**
     * Sink {@code table_options} key for Dameng page fill factor ({@code STORAGE (FILLFACTOR
     * ...)}).
     */
    public static final String TABLE_OPTION_FILLFACTOR = "fillfactor";

    private static final String SELECT_COLUMNS_SQL =
            "SELECT COLUMNS.COLUMN_NAME, COLUMNS.DATA_TYPE, COLUMNS.DATA_LENGTH, COLUMNS.DATA_PRECISION, COLUMNS.DATA_SCALE "
                    + ", COLUMNS.NULLABLE, COLUMNS.DATA_DEFAULT, COMMENTS.COMMENTS ,"
                    + "COLUMNS.CHAR_LENGTH, SYSCOL.LENGTH$ AS DECLARED_LENGTH, "
                    + "CASE \n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('CHAR', 'CHARACTER', 'VARCHAR', 'VARCHAR2', 'VARBINARY', 'BINARY') THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_LENGTH || ')'\n"
                    + "        WHEN COLUMNS.DATA_TYPE IN ('NUMERIC', 'DECIMAL', 'NUMBER') AND COLUMNS.DATA_PRECISION IS NOT NULL AND COLUMNS.DATA_SCALE IS NOT NULL AND COLUMNS.DATA_PRECISION != 0 AND COLUMNS.DATA_SCALE != 0 THEN COLUMNS.DATA_TYPE || '(' || COLUMNS.DATA_PRECISION || ', ' || COLUMNS.DATA_SCALE || ')'\n"
                    + "        ELSE COLUMNS.DATA_TYPE\n"
                    + "    END AS SOURCE_TYPE \n"
                    + "FROM ALL_TAB_COLUMNS COLUMNS "
                    + "LEFT JOIN ALL_COL_COMMENTS COMMENTS "
                    + "ON COLUMNS.OWNER = COMMENTS.SCHEMA_NAME "
                    + "AND COLUMNS.TABLE_NAME = COMMENTS.TABLE_NAME "
                    + "AND COLUMNS.COLUMN_NAME = COMMENTS.COLUMN_NAME "
                    // SYSCOLUMNS.LENGTH$ is the declared length. ALL_TAB_COLUMNS derives
                    // DATA_LENGTH from it as a byte count and leaves CHAR_LENGTH at 0 for
                    // NVARCHAR2, see nationalCharLength.
                    + "LEFT JOIN SYS.SYSOBJECTS SCH "
                    + "ON SCH.NAME = COLUMNS.OWNER AND SCH.TYPE$ = 'SCH' "
                    + "LEFT JOIN SYS.SYSOBJECTS TAB "
                    + "ON TAB.SCHID = SCH.ID AND TAB.NAME = COLUMNS.TABLE_NAME AND TAB.TYPE$ = 'SCHOBJ' "
                    + "LEFT JOIN SYS.SYSCOLUMNS SYSCOL "
                    + "ON SYSCOL.ID = TAB.ID AND SYSCOL.NAME = COLUMNS.COLUMN_NAME "
                    + "WHERE COLUMNS.OWNER = '%s' "
                    + "AND COLUMNS.TABLE_NAME = '%s' "
                    + "ORDER BY COLUMNS.COLUMN_ID ASC";

    public DamengCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, defaultSchema, driverClass);
    }

    @Override
    protected void createDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select * from \"%s\".\"%s\" LIMIT 1",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where name = '%s'", databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " where OWNER = '%s' and TABLE_NAME = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT name FROM v$database";
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return getCreateTableSqls(tablePath, table, createIndex).get(0);
    }

    @Override
    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new DamengCreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", getTableName(tablePath));
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName("\"");
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("DATA_TYPE");
        long columnLength =
                nationalCharLength(
                        typeName,
                        resultSet.getLong("DATA_LENGTH"),
                        resultSet.getLong("CHAR_LENGTH"),
                        resultSet.getLong("DECLARED_LENGTH"));
        long columnPrecision = resultSet.getLong("DATA_PRECISION");
        int columnScale = resultSet.getInt("DATA_SCALE");
        String columnComment = resultSet.getString("COMMENTS");
        Object defaultValue = resultSet.getObject("DATA_DEFAULT");
        boolean isNullable = resultSet.getString("NULLABLE").equals("Y");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return DmdbTypeConverter.INSTANCE.convert(typeDefine);
    }

    /**
     * Length of a column in the unit the type converter expects: characters for {@code NVARCHAR}
     * and {@code NVARCHAR2}, {@code DATA_LENGTH} for every other type.
     *
     * <p>{@code NVARCHAR(n)} and {@code NVARCHAR2(n)} always hold n characters. {@code
     * ALL_TAB_COLUMNS.DATA_LENGTH} reports them in bytes: n * 2 on a GB18030 database and n * 4 on
     * a UTF-8 database, whatever {@code LENGTH_IN_CHAR} is. {@code CHAR_LENGTH} is n for {@code
     * NVARCHAR} but 0 for {@code NVARCHAR2}, which the view does not list. {@code
     * SYS.SYSCOLUMNS.LENGTH$} is n for both, so it is used first.
     *
     * @param dataLength {@code ALL_TAB_COLUMNS.DATA_LENGTH}
     * @param charLength {@code ALL_TAB_COLUMNS.CHAR_LENGTH}, 0 if unknown
     * @param declaredLength {@code SYS.SYSCOLUMNS.LENGTH$}, 0 if unknown
     */
    static long nationalCharLength(
            String typeName, long dataLength, long charLength, long declaredLength) {
        if (!DmdbTypeConverter.DM_NVARCHAR.equalsIgnoreCase(typeName)
                && !DmdbTypeConverter.DM_NVARCHAR2.equalsIgnoreCase(typeName)) {
            return dataLength;
        }
        if (declaredLength > 0) {
            return declaredLength;
        }
        if (charLength > 0) {
            return charLength;
        }
        // Unknown character length: keep the byte count, which oversizes but never truncates.
        return dataLength;
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (PreparedStatement ps =
                        getConnection(defaultUrl)
                                .prepareStatement("SELECT OWNER, TABLE_NAME FROM ALL_TABLES");
                ResultSet rs = ps.executeQuery()) {

            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing table in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DmdbTypeMapper());
    }
}
