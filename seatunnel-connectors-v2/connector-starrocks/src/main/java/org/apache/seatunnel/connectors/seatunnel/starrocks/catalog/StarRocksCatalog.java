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

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
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
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.datatypes.StarRocksTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.starrocks.sink.StarRocksSaveModeUtil;
import org.apache.seatunnel.connectors.seatunnel.starrocks.util.StarRocksConditionProvider;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class StarRocksCatalog implements Catalog {

    protected final String catalogName;
    protected String defaultDatabase = "information_schema";
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected String defaultUrl;
    private final String template;
    private Connection conn;
    private final StarRocksTypeConverter typeConverter;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksCatalog.class);

    public StarRocksCatalog(
            String catalogName, String username, String pwd, String defaultUrl, String template) {

        checkArgument(StringUtils.isNotBlank(username));
        checkArgument(StringUtils.isNotBlank(defaultUrl));
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(defaultUrl);
        this.baseUrl = urlInfo.getUrlWithoutDatabase();
        if (urlInfo.getDefaultDatabase().isPresent()) {
            this.defaultDatabase = urlInfo.getDefaultDatabase().get();
        }
        this.defaultUrl = defaultUrl;
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
        this.template = template;
        this.typeConverter = new StarRocksTypeConverter();
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

        try (PreparedStatement ps =
                conn.prepareStatement(StarRocksConditionProvider.TABLE_SCHEMA_QUERY)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet resultSet = ps.executeQuery()) {
                Map<String, String> options = buildConnectorOptions(tablePath);
                Optional<PrimaryKey> primaryKey =
                        getPrimaryKey(tablePath.getDatabaseName(), tablePath.getTableName());

                TableSchema.Builder builder = TableSchema.builder();
                buildTableSchemaWithErrorCheck(
                        tablePath, resultSet, builder, options, Collections.emptyList());

                primaryKey.ifPresent(builder::primaryKey);

                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier, builder.build(), options, Collections.emptyList(), "");
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath, List<String> fieldNames)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        try (PreparedStatement ps =
                conn.prepareStatement(StarRocksConditionProvider.TABLE_SCHEMA_QUERY)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> options = buildConnectorOptions(tablePath);
                buildTableSchemaWithErrorCheck(tablePath, rs, builder, options, fieldNames);
                return CatalogTable.of(
                        TableIdentifier.of(
                                catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                        builder.build(),
                        options,
                        Collections.emptyList(),
                        "",
                        catalogName);
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        this.createTable(
                StarRocksSaveModeUtil.getCreateTableSql(
                        template,
                        tablePath.getDatabaseName(),
                        tablePath.getTableName(),
                        table.getTableSchema()));
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            conn.createStatement()
                    .execute(StarRocksSaveModeUtil.getDropTableSql(tablePath, ignoreIfNotExists));
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
                        .execute(StarRocksSaveModeUtil.getTruncateTableSql(tablePath));
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
                            StarRocksSaveModeUtil.getCreateDatabaseSql(
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
                            StarRocksSaveModeUtil.getDropDatabaseSql(
                                    tablePath.getDatabaseName(), ignoreIfNotExists));
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
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
        try {
            log.info("create table sql is :{}", sql);
            conn.createStatement().execute(sql);
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed create table in catalog %s, sql :[%s]", catalogName, sql),
                    e);
        }
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
                    StarRocksSaveModeUtil.getCreateTableSql(
                            template,
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            catalogTable.get().getTableSchema()));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(StarRocksSaveModeUtil.getDropTableSql(tablePath, true));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(StarRocksSaveModeUtil.getTruncateTableSql(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(
                    StarRocksSaveModeUtil.getCreateDatabaseSql(tablePath.getDatabaseName(), true));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(
                    "DROP DATABASE IF EXISTS `" + tablePath.getDatabaseName() + "`");
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }

    private void buildTableSchemaWithErrorCheck(
            TablePath tablePath,
            ResultSet resultSet,
            TableSchema.Builder builder,
            Map<String, String> options,
            List<String> fieldNames)
            throws SQLException {
        Map<String, String> unsupported = new LinkedHashMap<>();
        List<String> keyList = new ArrayList<>();
        while (resultSet.next()) {
            try {
                String columName = resultSet.getString("COLUMN_NAME");
                if (CollectionUtils.isEmpty(fieldNames) || fieldNames.contains(columName)) {
                    String columnKey = resultSet.getString("COLUMN_KEY");
                    builder.column(buildColumn(resultSet));
                    if ("UNI".equalsIgnoreCase(columnKey)) {
                        keyList.add(columName);
                    } else if ("DUP".equalsIgnoreCase(columnKey)) {
                        String dupKey =
                                options.getOrDefault(
                                        SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY
                                                .getPlaceHolderKey(),
                                        "");
                        if (StringUtils.isBlank(dupKey)) {
                            dupKey = columName;
                        } else {
                            dupKey = dupKey + "," + columName;
                        }
                        options.put(
                                SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getPlaceHolderKey(),
                                dupKey);
                    }
                }
            } catch (SeaTunnelRuntimeException e) {
                if (e.getSeaTunnelErrorCode()
                        .equals(CommonErrorCode.CONVERT_TO_SEATUNNEL_TYPE_ERROR_SIMPLE)) {
                    unsupported.put(e.getParams().get("field"), e.getParams().get("dataType"));
                } else {
                    throw e;
                }
            }
        }
        if (!keyList.isEmpty()) {
            builder.primaryKey(
                    PrimaryKey.of(
                            "uk_" + tablePath.getDatabaseName() + "_" + tablePath.getTableName(),
                            keyList));
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType(
                    catalogName, tablePath.getFullName(), unsupported);
        }
    }

    private Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        // e.g. tinyint(1) unsigned
        String columnType = resultSet.getString("COLUMN_TYPE");
        // e.g. tinyint
        String dataType = resultSet.getString("DATA_TYPE").toUpperCase();
        String comment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String isNullableStr = resultSet.getString("IS_NULLABLE");
        boolean isNullable = isNullableStr.equals("YES");
        // e.g. `decimal(10, 2)` is 10
        long numberPrecision = resultSet.getInt("NUMERIC_PRECISION");
        // e.g. `decimal(10, 2)` is 2
        int numberScale = resultSet.getInt("NUMERIC_SCALE");
        long charOctetLength = resultSet.getLong("CHARACTER_MAXIMUM_LENGTH");
        // e.g. `timestamp(3)` is 3
        int timePrecision = resultSet.getInt("DATETIME_PRECISION");

        Preconditions.checkArgument(!(numberPrecision > 0 && charOctetLength > 0));
        Preconditions.checkArgument(!(numberScale > 0 && timePrecision > 0));

        BasicTypeDefine<StarRocksType> typeDefine =
                BasicTypeDefine.<StarRocksType>builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType)
                        .length(Math.max(charOctetLength, numberPrecision))
                        .precision(numberPrecision)
                        .scale(Math.max(numberScale, timePrecision))
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return typeConverter.convert(typeDefine);
    }
}
