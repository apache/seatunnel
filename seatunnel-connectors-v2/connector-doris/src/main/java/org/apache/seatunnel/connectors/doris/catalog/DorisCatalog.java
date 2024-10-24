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

package org.apache.seatunnel.connectors.doris.catalog;

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
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterFactory;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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

import static com.google.common.base.Preconditions.checkArgument;

public class DorisCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(DorisCatalog.class);

    private final String catalogName;

    private final String[] frontEndNodes;

    private final Integer queryPort;

    private final String username;

    private final String password;

    private String defaultDatabase = "information_schema";

    private Connection conn;

    private String createTableTemplate;

    private String dorisVersion;

    private TypeConverter<BasicTypeDefine> typeConverter;

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password) {
        this.catalogName = catalogName;
        this.frontEndNodes = frontEndNodes.split(",");
        this.queryPort = queryPort;
        this.username = username;
        this.password = password;
    }

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password,
            String createTableTemplate) {
        this(catalogName, frontEndNodes, queryPort, username, password);
        this.createTableTemplate = createTableTemplate;
    }

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password,
            String createTableTemplate,
            String defaultDatabase) {
        this(catalogName, frontEndNodes, queryPort, username, password, createTableTemplate);
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        String jdbcUrl =
                DorisCatalogUtil.getJdbcUrl(
                        DorisCatalogUtil.randomFrontEndHost(frontEndNodes),
                        queryPort,
                        defaultDatabase);
        try {
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            conn.getCatalog();
            dorisVersion = getDorisVersion();
            typeConverter = DorisTypeConverterFactory.getTypeConverter(dorisVersion);
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed to connect url %s", jdbcUrl), e);
        }
        LOG.info("Catalog {} established connection to {} success", catalogName, jdbcUrl);
    }

    private String getDorisVersion() throws SQLException {
        String dorisVersion = null;
        try (PreparedStatement preparedStatement =
                        conn.prepareStatement(DorisCatalogUtil.QUERY_DORIS_VERSION_QUERY);
                ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                dorisVersion = resultSet.getString(2);
            }
        }
        return dorisVersion;
    }

    @Override
    public void close() throws CatalogException {
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

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.DATABASE_QUERY)) {
            ps.setString(1, databaseName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new CatalogException("check database exists failed", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.ALL_DATABASES_QUERY);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String database = rs.getString(1);
                databases.add(database);
            }
        } catch (SQLException e) {
            throw new CatalogException("list databases failed", e);
        }
        Collections.sort(databases);
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps =
                conn.prepareStatement(DorisCatalogUtil.TABLES_QUERY_WITH_DATABASE_QUERY)) {
            ps.setString(1, databaseName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String table = rs.getString(1);
                    tables.add(table);
                }
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("list tables of database [%s] failed", databaseName), e);
        }
        Collections.sort(tables);
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try (PreparedStatement ps =
                conn.prepareStatement(DorisCatalogUtil.TABLES_QUERY_WITH_IDENTIFIER_QUERY)) {
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
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> options = connectorOptions();
                buildTableSchemaWithErrorCheck(
                        tablePath, rs, builder, options, Collections.emptyList());
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
    public CatalogTable getTable(TablePath tablePath, List<String> fieldNames)
            throws CatalogException, TableNotExistException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            try (ResultSet rs = ps.executeQuery()) {
                Map<String, String> options = connectorOptions();
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

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
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

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        boolean tableExists = tableExists(tablePath);
        if (ignoreIfExists && tableExists) {
            LOG.info("table {} is exists, skip create", tablePath.getFullName());
            return;
        }

        if (tableExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        String stmt =
                DorisCatalogUtil.getCreateTableStatement(
                        createTableTemplate, tablePath, table, typeConverter);
        try (Statement statement = conn.createStatement()) {
            statement.execute(stmt);
        } catch (SQLException e) {
            throw new CatalogException("create table statement execute failed", e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        String query = DorisCatalogUtil.getDropTableQuery(tablePath, ignoreIfNotExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        String query =
                DorisCatalogUtil.getCreateDatabaseQuery(
                        tablePath.getDatabaseName(), ignoreIfExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("create database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String query =
                DorisCatalogUtil.getDropDatabaseQuery(
                        tablePath.getDatabaseName(), ignoreIfNotExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("drop database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    private Map<String, String> connectorOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "doris");
        options.put(DorisOptions.FENODES.key(), String.join(",", frontEndNodes));
        options.put(DorisOptions.USERNAME.key(), username);
        options.put(DorisOptions.PASSWORD.key(), password);
        return options;
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            if (ignoreIfNotExists) {
                conn.createStatement().execute(DorisCatalogUtil.getTruncateTableQuery(tablePath));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed TRUNCATE TABLE in catalog %s", tablePath.getFullName()),
                    e);
        }
    }

    public boolean isExistsData(TablePath tablePath) {
        String tableName = tablePath.getFullName();
        String sql = String.format("select * from %s limit 1;", tableName);
        try (PreparedStatement ps = conn.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery()) {
            return resultSet.next();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(
                    DorisCatalogUtil.getCreateTableStatement(
                            createTableTemplate,
                            tablePath,
                            catalogTable.get(),
                            // used for test when typeConverter is null
                            typeConverter != null ? typeConverter : DorisTypeConverterV2.INSTANCE));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(DorisCatalogUtil.getDropTableQuery(tablePath, true));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(DorisCatalogUtil.getTruncateTableQuery(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(
                    DorisCatalogUtil.getCreateDatabaseQuery(tablePath.getDatabaseName(), true));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(
                    DorisCatalogUtil.getDropDatabaseQuery(tablePath.getDatabaseName(), true));
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
