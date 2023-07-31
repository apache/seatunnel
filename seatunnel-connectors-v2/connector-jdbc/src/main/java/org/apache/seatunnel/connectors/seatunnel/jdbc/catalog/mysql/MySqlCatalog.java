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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

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
import com.mysql.cj.util.StringUtils;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MySqlCatalog extends AbstractJdbcCatalog {

    protected static final Set<String> SYS_DATABASES = new HashSet<>(4);
    private final String SELECT_COLUMNS =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s'";

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("mysql");
        SYS_DATABASES.add("performance_schema");
        SYS_DATABASES.add("sys");
    }

    protected final Map<String, Connection> connectionMap;

    public MySqlCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo, null);
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
        try (PreparedStatement ps = defaultConnection.prepareStatement("SHOW DATABASES;")) {

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
        try (PreparedStatement ps = connection.prepareStatement("SHOW TABLES;")) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1));
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
                    getPrimaryKey(metaData, tablePath.getDatabaseName(), tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData, tablePath.getDatabaseName(), tablePath.getTableName());
            String sql =
                    String.format(
                            SELECT_COLUMNS, tablePath.getDatabaseName(), tablePath.getTableName());
            try (PreparedStatement ps = conn.prepareStatement(sql);
                    ResultSet resultSet = ps.executeQuery(); ) {

                TableSchema.Builder builder = TableSchema.builder();
                while (resultSet.next()) {
                    buildTable(resultSet, builder);
                }
                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        "mysql");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private void buildTable(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String sourceType = resultSet.getString("COLUMN_TYPE");
        String typeName = resultSet.getString("DATA_TYPE").toUpperCase();
        int precision = resultSet.getInt("NUMERIC_PRECISION");
        int scale = resultSet.getInt("NUMERIC_SCALE");
        long columnLength = resultSet.getLong("CHARACTER_MAXIMUM_LENGTH");
        long octetLength = resultSet.getLong("CHARACTER_OCTET_LENGTH");
        if (sourceType.toLowerCase(Locale.ROOT).contains("unsigned")) {
            typeName += "_UNSIGNED";
        }
        SeaTunnelDataType<?> type = fromJdbcType(typeName, precision, scale);
        String comment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String isNullableStr = resultSet.getString("IS_NULLABLE");
        boolean isNullable = isNullableStr.equals("YES");
        long bitLen = 0;
        MysqlType mysqlType = MysqlType.valueOf(typeName);
        switch (mysqlType) {
            case BIT:
                bitLen = precision;
                break;
            case CHAR:
            case VARCHAR:
                columnLength = octetLength;
                break;
            case BINARY:
            case VARBINARY:
                // Uniform conversion to bits
                bitLen = octetLength * 4 * 8L;
                break;
            case BLOB:
            case TINYBLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
                bitLen = columnLength << 3;
                break;
            case JSON:
                columnLength = 4 * 1024 * 1024 * 1024L;
                break;
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
                        comment,
                        sourceType,
                        sourceType.contains("unsigned"),
                        sourceType.contains("zerofill"),
                        bitLen,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    public static Map<String, Object> getColumnsDefaultValue(TablePath tablePath, Connection conn) {
        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getTableName(), "`", false));
        queryBuf.append(" FROM ");
        queryBuf.append(StringUtils.quoteIdentifier(tablePath.getDatabaseName(), "`", false));
        try (PreparedStatement ps2 = conn.prepareStatement(queryBuf.toString())) {
            ResultSet rs = ps2.executeQuery();
            Map<String, Object> result = new HashMap<>();
            while (rs.next()) {
                String field = rs.getString("Field");
                Object defaultValue = rs.getObject("Default");
                result.put(field, defaultValue);
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed getting table(%s) columns default value",
                            tablePath.getFullName()),
                    e);
        }
    }

    // todo: If the origin source is mysql, we can directly use create table like to create the
    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(tablePath, table).build(table.getCatalogName());
        Connection connection = getConnection(dbUrl);
        log.info("create table sql: {}", createTableSql);
        try (PreparedStatement ps = connection.prepareStatement(createTableSql)) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        Connection connection = getConnection(dbUrl);
        try (PreparedStatement ps =
                connection.prepareStatement(
                        String.format("DROP TABLE IF EXISTS %s;", tablePath.getFullName()))) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement(
                        String.format("CREATE DATABASE `%s`;", databaseName))) {
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
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement(
                        String.format("DROP DATABASE `%s`;", databaseName))) {
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
     * @see com.mysql.cj.MysqlType
     * @see ResultSetImpl#getObjectStoredProc(int, int)
     */
    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        MysqlType mysqlType = MysqlType.getByName(metadata.getColumnTypeName(colIndex));
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(MysqlDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(MysqlDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return new MysqlDataTypeConvertor().toSeaTunnelType(mysqlType, dataTypeProperties);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, int precision, int scale) {
        MysqlType mysqlType = MysqlType.getByName(typeName);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(MysqlDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(MysqlDataTypeConvertor.SCALE, scale);
        return new MysqlDataTypeConvertor().toSeaTunnelType(mysqlType, dataTypeProperties);
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
