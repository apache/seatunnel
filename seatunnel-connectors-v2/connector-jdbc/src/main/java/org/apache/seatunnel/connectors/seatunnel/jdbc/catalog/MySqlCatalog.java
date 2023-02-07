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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import com.mysql.cj.MysqlType;
import com.mysql.cj.jdbc.result.ResultSetImpl;

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

public class MySqlCatalog extends AbstractJdbcCatalog {

    private static final Set<String> SYS_DATABASES = new HashSet<>(4);

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("mysql");
        SYS_DATABASES.add("performance_schema");
        SYS_DATABASES.add("sys");
    }

    public MySqlCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(catalogName, defaultDatabase, username, pwd, baseUrl);
    }

    public MySqlCatalog(String catalogName, String username, String pwd, String defaultUrl) {
        super(catalogName, username, pwd, defaultUrl);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

            PreparedStatement ps = conn.prepareStatement("SHOW DATABASES;");

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
    public List<String> listTables(String databaseName) throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
            PreparedStatement ps =
                conn.prepareStatement("SHOW TABLES;");

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
    public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        String dbUrl = baseUrl + tablePath.getDatabaseName();
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<TableSchema.PrimaryKey> primaryKey =
                getPrimaryKey(metaData, tablePath.getDatabaseName(), tablePath.getTableName());

            PreparedStatement ps =
                conn.prepareStatement(String.format("SELECT * FROM %s WHERE 1 = 0;", tablePath.getFullName()));

            ResultSetMetaData tableMetaData = ps.getMetaData();

            TableSchema.Builder builder = TableSchema.builder();
            for (int i = 1; i <= tableMetaData.getColumnCount(); i++) {
                SeaTunnelDataType<?> type = fromJdbcType(tableMetaData, i);
                builder.physicalColumn(tableMetaData.getColumnName(i), type);
            }

            primaryKey.ifPresent(builder::primaryKey);

            TableIdentifier tableIdentifier = TableIdentifier.of(catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
            return CatalogTable.of(tableIdentifier, builder.build(), buildConnectorOptions(tablePath), Collections.emptyList(), "");
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    /**
     * @see com.mysql.cj.MysqlType
     * @see ResultSetImpl#getObjectStoredProc(int, int)
     */
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex) throws SQLException {
        MysqlType mysqlType = MysqlType.getByName(metadata.getColumnTypeName(colIndex));
        switch (mysqlType) {
            case NULL:
                return BasicType.VOID_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case BIT:
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case TINYINT_UNSIGNED:
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case SMALLINT_UNSIGNED:
            case INT:
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
                return BasicType.INT_TYPE;
            case INT_UNSIGNED:
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
            case FLOAT_UNSIGNED:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
            case DOUBLE_UNSIGNED:
                return BasicType.DOUBLE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
            case DATETIME:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            // TODO: to confirm
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
            case JSON:
            case ENUM:
                return BasicType.STRING_TYPE;
            case BINARY:
            case VARBINARY:
            case TINYBLOB:
            case BLOB:
            case MEDIUMBLOB:
            case LONGBLOB:
            case GEOMETRY:
                return PrimitiveByteArrayType.INSTANCE;
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
                int precision = metadata.getPrecision(colIndex);
                int scale = metadata.getScale(colIndex);
                return new DecimalType(precision, scale);
                // TODO: support 'SET' & 'YEAR' type
            default:
                throw new JdbcConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, String.format("Doesn't support MySQL type '%s' yet", mysqlType.getName()));
        }
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
}
