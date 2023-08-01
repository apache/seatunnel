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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import com.mysql.cj.MysqlType;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_BIT;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_BYTEA;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_CHARACTER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_CHARACTER_VARYING;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_GEOGRAPHY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_GEOMETRY;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_TEXT;

@Slf4j
public class PostgresCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL =
            "SELECT \n"
                    + "    a.attname AS column_name, \n"
                    + "\t\tt.typname as type_name,\n"
                    + "    CASE \n"
                    + "        WHEN t.typname = 'varchar' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'bpchar' THEN 'char' || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        WHEN t.typname = 'numeric' OR t.typname = 'decimal' THEN t.typname || '(' || ((a.atttypmod - 4) >> 16) || ', ' || ((a.atttypmod - 4) & 65535) || ')'\n"
                    + "        WHEN t.typname = 'bit' OR t.typname = 'bit varying' THEN t.typname || '(' || (a.atttypmod - 4) || ')'\n"
                    + "        ELSE t.typname\n"
                    + "    END AS full_type_name,\n"
                    + "    CASE\n"
                    + "        WHEN t.typname IN ('varchar', 'bpchar', 'bit', 'bit varying') THEN a.atttypmod - 4\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) >> 16\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_length,\n"
                    + "\t\tCASE\n"
                    + "        WHEN t.typname IN ('numeric', 'decimal') THEN (a.atttypmod - 4) & 65535\n"
                    + "        ELSE NULL\n"
                    + "    END AS column_scale,\n"
                    + "\t\td.description AS column_comment,\n"
                    + "\t\tpg_get_expr(ad.adbin, ad.adrelid) AS default_value,\n"
                    + "\t\tCASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable\n"
                    + "FROM \n"
                    + "    pg_class c\n"
                    + "    JOIN pg_namespace n ON c.relnamespace = n.oid\n"
                    + "    JOIN pg_attribute a ON a.attrelid = c.oid\n"
                    + "    JOIN pg_type t ON a.atttypid = t.oid\n"
                    + "    LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid\n"
                    + "    LEFT JOIN pg_attrdef ad ON a.attnum = ad.adnum AND a.attrelid = ad.adrelid\n"
                    + "WHERE \n"
                    + "    n.nspname = '%s'\n"
                    + "    AND c.relname = '%s'\n"
                    + "    AND a.attnum > 0\n"
                    + "ORDER BY \n"
                    + "    a.attnum;";

    protected static final Set<String> SYS_DATABASES = new HashSet<>(9);

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("pg_catalog");
        SYS_DATABASES.add("root");
        SYS_DATABASES.add("pg_toast");
        SYS_DATABASES.add("pg_temp_1");
        SYS_DATABASES.add("pg_toast_temp_1");
        SYS_DATABASES.add("postgres");
        SYS_DATABASES.add("template0");
        SYS_DATABASES.add("template1");
    }

    protected final Map<String, Connection> connectionMap;

    public PostgresCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
        this.connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        for (Map.Entry<String, Connection> entry : connectionMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                throw new CatalogException(
                        String.format("Failed to close %s via JDBC.", entry.getKey()), e);
            }
        }
        super.close();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement("select datname from pg_database;")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!SYS_DATABASES.contains(databaseName)) {
                    databases.add(rs.getString(1));
                }
            }

            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps =
                connection.prepareStatement(
                        "SELECT table_schema, table_name FROM information_schema.tables;")) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                String schemaName = rs.getString("table_schema");
                String tableName = rs.getString("table_name");
                if (org.apache.commons.lang3.StringUtils.isNotBlank(schemaName)
                        && !SYS_DATABASES.contains(schemaName)) {
                    tables.add(schemaName + "." + tableName);
                }
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection conn = getConnection(dbUrl);
        try {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

            String sql =
                    String.format(
                            SELECT_COLUMNS_SQL,
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            try (PreparedStatement ps = conn.prepareStatement(sql);
                    ResultSet resultSet = ps.executeQuery()) {
                TableSchema.Builder builder = TableSchema.builder();

                // add column
                while (resultSet.next()) {
                    buildColumn(resultSet, builder);
                }

                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        "postgres");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private void buildColumn(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String typeName = resultSet.getString("type_name");
        String fullTypeName = resultSet.getString("full_type_name");
        long columnLength = resultSet.getLong("column_length");
        long columnScale = resultSet.getLong("column_scale");
        String columnComment = resultSet.getString("column_comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getString("is_nullable").equals("YES");

        if (defaultValue != null && defaultValue.toString().contains("regclass"))
            defaultValue = null;

        SeaTunnelDataType<?> type = fromJdbcType(typeName, columnLength, columnScale);
        long bitLen = 0;
        switch (typeName) {
            case PG_BYTEA:
                bitLen = -1;
                break;
            case PG_TEXT:
                columnLength = -1;
                break;
            case PG_INTERVAL:
                columnLength = 50;
                break;
            case PG_GEOMETRY:
            case PG_GEOGRAPHY:
                columnLength = 255;
                break;
            case PG_BIT:
                bitLen = columnLength;
                break;
            case PG_CHAR:
            case PG_CHARACTER:
            case PG_CHARACTER_VARYING:
            default:
                break;
        }

        PhysicalColumn physicalColumn =
                PhysicalColumn.of(
                        columnName,
                        type,
                        0,
                        isNullable,
                        defaultValue,
                        columnComment,
                        fullTypeName,
                        false,
                        false,
                        bitLen,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String createTableSql = new PostgresCreateTableSqlBuilder(table).build(tablePath);
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection conn = getConnection(dbUrl);
        log.info("create table sql: {}", createTableSql);
        try (PreparedStatement ps = conn.prepareStatement(createTableSql)) {
            ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
        return true;
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());

        String schemaName = tablePath.getSchemaName();
        String tableName = tablePath.getTableName();

        String sql = "DROP TABLE IF EXISTS \"" + schemaName + "\".\"" + tableName + "\"";
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "CREATE DATABASE \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        String sql = "DROP DATABASE IF EXISTS \"" + databaseName + "\"";
        try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    /**
     * @see MysqlType
     * @see ResultSetImpl#getObjectStoredProc(int, int)
     */
    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnTypeName = metadata.getColumnTypeName(colIndex);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(
                PostgresDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(PostgresDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return new PostgresDataTypeConvertor().toSeaTunnelType(columnTypeName, dataTypeProperties);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(PostgresDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(PostgresDataTypeConvertor.SCALE, scale);
        return new PostgresDataTypeConvertor().toSeaTunnelType(typeName, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    private String getUrlFromDatabaseName(String databaseName) {
        String url = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        return url + databaseName + suffix;
    }
}
