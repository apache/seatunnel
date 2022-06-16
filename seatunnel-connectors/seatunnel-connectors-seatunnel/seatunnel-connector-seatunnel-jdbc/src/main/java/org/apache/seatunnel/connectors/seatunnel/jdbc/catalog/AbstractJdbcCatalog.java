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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractJdbcCatalog implements Catalog {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJdbcCatalog.class);

    protected final String catalogName;
    protected final String defaultDatabase;
    protected final String username;
    protected final String pwd;
    protected final String baseUrl;
    protected final String defaultUrl;

    public AbstractJdbcCatalog(
        String catalogName,
        String defaultDatabase,
        String username,
        String pwd,
        String baseUrl) {

        checkArgument(StringUtils.isNotBlank(username));
        checkArgument(StringUtils.isNotBlank(pwd));
        checkArgument(StringUtils.isNotBlank(baseUrl));

        validateJdbcUrl(baseUrl);
        this.catalogName = catalogName;
        this.defaultDatabase = defaultDatabase;
        this.username = username;
        this.pwd = pwd;
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl : baseUrl + "/";
        this.defaultUrl = this.baseUrl + defaultDatabase;
    }

    /**
     * URL has to be without database, like "jdbc:postgresql://localhost:5432/" or
     * "jdbc:postgresql://localhost:5432" rather than "jdbc:postgresql://localhost:5432/db".
     */
    public static void validateJdbcUrl(String url) {
        String[] parts = url.trim().split("\\/+");

        checkArgument(parts.length == 2);
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return pwd;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    @SuppressWarnings("EmptyBlock")
    @Override
    public void open() throws CatalogException {
        // test connection, fail early if we cannot connect to database
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
        } catch (SQLException e) {
            throw new CatalogException(
                String.format("Failed connecting to %s via JDBC.", defaultUrl), e);
        }

        LOG.info("Catalog {} established connection to {}", catalogName, defaultUrl);
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", catalogName);
    }

    protected Optional<TableSchema.PrimaryKey> getPrimaryKey(
        DatabaseMetaData metaData, String schema, String table) throws SQLException {

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        ResultSet rs = metaData.getPrimaryKeys(null, schema, table);

        Map<Integer, String> keySeqColumnName = new HashMap<>();
        String pkName = null;
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            // all the PK_NAME should be the same
            pkName = rs.getString("PK_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            // KEY_SEQ is 1-based index
            keySeqColumnName.put(keySeq - 1, columnName);
        }
        // initialize size
        List<String> pkFields = Arrays.asList(new String[keySeqColumnName.size()]);
        keySeqColumnName.forEach(pkFields::set);
        if (!pkFields.isEmpty()) {
            // PK_NAME maybe null according to the javadoc, generate an unique name in that case
            pkName = pkName == null ? "pk_" + String.join("_", pkFields) : pkName;
            return Optional.of(TableSchema.PrimaryKey.of(pkName, pkFields));
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
        try {
            return databaseExists(tablePath.getDatabaseName())
                && listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }
}
