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

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.StarRocksSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.cj.MysqlType;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class StarRocksCatalog implements Catalog {

    protected final String catalogName;
    protected String defaultDatabase = "information_schema";
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected String defaultUrl;
    private final JdbcUrlUtil.UrlInfo urlInfo;
    private final String template;
    private Connection conn;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    public StarRocksCatalog(
            String catalogName, String username, String pwd, String defaultUrl, String template) {

        checkArgument(StringUtils.isNotBlank(username));
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
        this.template = template;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (PreparedStatement ps = conn.prepareStatement("SHOW DATABASES;");
                ResultSet rs = ps.executeQuery()) {
            List<String> databases = new ArrayList<>();

            while (rs.next()) {
                databases.add(rs.getString(1));
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

        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT TABLE_NAME FROM information_schema.tables "
                                + "WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME")) {
            ps.setString(1, databaseName);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
                return tables;
            }
        } catch (SQLException e) {
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

        try {
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(tablePath.getDatabaseName(), tablePath.getTableName());

            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format(
                                    "SELECT * FROM %s WHERE 1 = 0;",
                                    tablePath.getFullNameWithQuoted()));

            ResultSetMetaData tableMetaData = ps.getMetaData();

            TableSchema.Builder builder = TableSchema.builder();
            buildColumnsWithErrorCheck(
                    tablePath,
                    builder,
                    IntStream.range(1, tableMetaData.getColumnCount() + 1).iterator(),
                    i -> {
                        try {
                            SeaTunnelDataType<?> type = fromJdbcType(tableMetaData, i);
                            // TODO add default value and test it
                            return PhysicalColumn.of(
                                    tableMetaData.getColumnName(i),
                                    type,
                                    tableMetaData.getColumnDisplaySize(i),
                                    tableMetaData.isNullable(i) == ResultSetMetaData.columnNullable,
                                    null,
                                    tableMetaData.getColumnLabel(i));
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });

            primaryKey.ifPresent(builder::primaryKey);

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        this.createTable(
                StarRocksSaveModeUtil.INSTANCE.getCreateTableSql(
                        template,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName(),
                        table.getTableSchema(),
                        table.getComment(),
                        StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key()));
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            conn.createStatement()
                    .execute(
                            StarRocksSaveModeUtil.INSTANCE.getDropTableSql(
                                    tablePath, ignoreIfNotExists));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            if (ignoreIfNotExists) {
                conn.createStatement()
                        .execute(StarRocksSaveModeUtil.INSTANCE.getTruncateTableSql(tablePath));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed TRUNCATE TABLE in catalog %s", tablePath.getFullName()),
                    e);
        }
    }

    public void executeSql(TablePath tablePath, String sql) {
        try {
            conn.createStatement().execute(sql);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed EXECUTE SQL in catalog %s", sql), e);
        }
    }

    public boolean isExistsData(TablePath tablePath) {
        String sql = String.format("select * from %s limit 1", tablePath.getFullName());
        try (Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet == null) {
                return false;
            }
            return resultSet.next();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed Connection JDBC error %s", tablePath.getTableName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            conn.createStatement()
                    .execute(
                            StarRocksSaveModeUtil.INSTANCE.getCreateDatabaseSql(
                                    tablePath.getDatabaseName(), ignoreIfExists));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            conn.createStatement()
                    .execute(
                            StarRocksSaveModeUtil.INSTANCE.getDropDatabaseSql(
                                    tablePath.getDatabaseName(), ignoreIfNotExists));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    /** @see com.mysql.cj.MysqlType */
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
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
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "Doesn't support Starrocks type '%s' yet",
                                starrocksType.getName()));
        }
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "starrocks");
        options.put("url", baseUrl + tablePath.getDatabaseName());
        options.put("table-name", tablePath.getFullName());
        return options;
    }

    public void createTable(String sql)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {
            log.info("create table sql is :{}", sql);
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

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        try {
            conn = DriverManager.getConnection(defaultUrl, username, pwd);
            // test connection, fail early if we cannot connect to database
            conn.getCatalog();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", catalogName);
        try {
            conn.close();
        } catch (SQLException e) {
            throw new CatalogException("close doris catalog failed", e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    protected Optional<PrimaryKey> getPrimaryKey(String schema, String table) throws SQLException {

        List<String> pkFields = new ArrayList<>();
        try (ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                                String.format(
                                        "SELECT COLUMN_NAME FROM information_schema.columns where TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION",
                                        schema, table))) {
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

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));

        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try (PreparedStatement ps =
                conn.prepareStatement(
                        "SELECT TABLE_NAME FROM information_schema.tables "
                                + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                                + "ORDER BY TABLE_NAME")) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("check table [%s] exists failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            Preconditions.checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(
                    StarRocksSaveModeUtil.INSTANCE.getCreateTableSql(
                            template,
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            catalogTable.get().getTableSchema(),
                            catalogTable.get().getComment(),
                            StarRocksSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key()));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(
                    StarRocksSaveModeUtil.INSTANCE.getDropTableSql(tablePath, true));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(
                    StarRocksSaveModeUtil.INSTANCE.getTruncateTableSql(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(
                    StarRocksSaveModeUtil.INSTANCE.getCreateDatabaseSql(
                            tablePath.getDatabaseName(), true));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(
                    "DROP DATABASE IF EXISTS `" + tablePath.getDatabaseName() + "`");
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
