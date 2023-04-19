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

package org.apache.seatunnel.connectors.seatunnel.starrocks.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.MysqlType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class StarRocksCatalog extends AbstractJdbcCatalog {

    protected final String catalogName;
    protected String defaultDatabase = "information_schema";
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected String defaultUrl;
    private final JdbcUrlUtil.UrlInfo urlInfo;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    static {
        SYS_DATABASES.add("information_schema");
        SYS_DATABASES.add("_statistics_");
    }

    public StarRocksCatalog(String catalogName, String username, String pwd, String defaultUrl) {

        super(catalogName, username, pwd, JdbcUrlUtil.getUrlInfo(defaultUrl));

        checkArgument(StringUtils.isNotBlank(username));
        checkArgument(StringUtils.isNotBlank(pwd));
        checkArgument(StringUtils.isNotBlank(defaultUrl));
        urlInfo = JdbcUrlUtil.getUrlInfo(defaultUrl);
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        if (urlInfo.getDefaultDatabase().isPresent()) {
            this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        }
        this.defaultUrl = defaultUrl;
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        throw new UnsupportedOperationException("Unsupported create table");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            if (ignoreIfExists) {
                conn.createStatement()
                        .execute(
                                "CREATE DATABASE IF NOT EXISTS `"
                                        + tablePath.getDatabaseName()
                                        + "`");
            } else {
                conn.createStatement()
                        .execute("CREATE DATABASE `" + tablePath.getDatabaseName() + "`");
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            if (ignoreIfNotExists) {
                conn.createStatement()
                        .execute("DROP DATABASE IF EXISTS `" + tablePath.getDatabaseName() + "`");
            } else {
                conn.createStatement()
                        .execute(String.format("DROP DATABASE `%s`", tablePath.getDatabaseName()));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /** @see com.mysql.cj.MysqlType */
    @Override
    public SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        MysqlType starrocksType = MysqlType.getByName(metadata.getColumnTypeName(colIndex));
        switch (starrocksType) {
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
            default:
                throw new StarRocksConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "Doesn't support Starrocks type '%s' yet",
                                starrocksType.getName()));
        }
    }

    @SuppressWarnings("MagicNumber")
    @Override
    public Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "starrocks");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    public void createTable(String sql)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            conn.createStatement().execute(sql);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed create table in catalog %s, sql :[%s]", catalogName, sql),
                    e);
        }
    }

    /**
     * URL has to be without database, like "jdbc:mysql://localhost:5432/" or
     * "jdbc:mysql://localhost:5432" rather than "jdbc:mysql://localhost:5432/db".
     */
    public static boolean validateJdbcUrlWithoutDatabase(String url) {
        String[] parts = url.trim().split("\\/+");

        return parts.length == 2;
    }

    /**
     * URL has to be with database, like "jdbc:mysql://localhost:5432/db" rather than
     * "jdbc:mysql://localhost:5432/".
     */
    @SuppressWarnings("MagicNumber")
    public static boolean validateJdbcUrlWithDatabase(String url) {
        String[] parts = url.trim().split("\\/+");
        return parts.length == 3;
    }

    /**
     * Ensure that the url was validated {@link #validateJdbcUrlWithDatabase}.
     *
     * @return The array size is fixed at 2, index 0 is base url, and index 1 is default database.
     */
    public static String[] splitDefaultUrl(String defaultUrl) {
        String[] res = new String[2];
        int index = defaultUrl.lastIndexOf("/") + 1;
        res[0] = defaultUrl.substring(0, index);
        res[1] = defaultUrl.substring(index);
        return res;
    }

    protected Optional<PrimaryKey> getPrimaryKey(String schema, String table) throws SQLException {

        List<String> pkFields = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            ResultSet rs =
                    conn.createStatement()
                            .executeQuery(
                                    String.format(
                                            "SELECT COLUMN_NAME FROM information_schema.columns where TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION",
                                            schema, table));
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                pkFields.add(columnName);
            }
        }
        if (!pkFields.isEmpty()) {
            // PK_NAME maybe null according to the javadoc, generate a unique name in that case
            String pkName = "pk_" + String.join("_", pkFields);
            return Optional.of(PrimaryKey.of(pkName, pkFields));
        }
        return Optional.empty();
    }
}
