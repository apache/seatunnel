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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.lob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-selects Oracle LOB columns over JDBC.
 *
 * <p>A positive {@code commit_scn} is bound to {@code AS OF SCN ?}. ORA-01555, ORA-01466,
 * ORA-08181, and ORA-01031 fall back to the current row. Prepared statements are cached per SQL
 * text so Oracle reuses the cursor. The connection is opened on the reader and is not serialized.
 */
public class JdbcOracleLobColumnReselector implements OracleLobColumnReselector {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcOracleLobColumnReselector.class);
    private static final String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";

    private final String url;
    private final String username;
    private final String password;
    private final String pdbName;

    private transient Connection connection;
    private transient boolean pdbSelected;
    private transient Map<String, PreparedStatement> statementCache;

    public JdbcOracleLobColumnReselector(
            String url, String username, String password, String pdbName) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.pdbName = pdbName;
    }

    /**
     * Re-selects LOB columns for one primary key. Tries the commit SCN first, then the current row
     * when flashback cannot be used.
     */
    @Override
    public synchronized OracleLobColumnReselector.ReselectResult reselect(
            String schema,
            String table,
            List<String> lobColumns,
            List<String> primaryKeyColumns,
            List<Object> primaryKeyValues,
            String commitScn)
            throws SQLException {
        return query(
                openConnection(),
                statementCache(),
                schema,
                table,
                lobColumns,
                primaryKeyColumns,
                primaryKeyValues,
                commitScn);
    }

    @Override
    public synchronized void close() {
        closeCachedStatements();
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close the Oracle LOB re-select connection", e);
        } finally {
            connection = null;
            pdbSelected = false;
        }
    }

    /**
     * Executes the re-select on an existing connection. Package-visible so tests can supply a mock
     * connection without opening Oracle.
     */
    static OracleLobColumnReselector.ReselectResult query(
            Connection connection,
            String schema,
            String table,
            List<String> lobColumns,
            List<String> primaryKeyColumns,
            List<Object> primaryKeyValues,
            String commitScn)
            throws SQLException {
        return query(
                connection,
                null,
                schema,
                table,
                lobColumns,
                primaryKeyColumns,
                primaryKeyValues,
                commitScn);
    }

    /**
     * Executes the re-select, optionally reusing prepared statements from {@code cache}. A null
     * cache prepares a new statement for each call and closes it.
     */
    static OracleLobColumnReselector.ReselectResult query(
            Connection connection,
            Map<String, PreparedStatement> cache,
            String schema,
            String table,
            List<String> lobColumns,
            List<String> primaryKeyColumns,
            List<Object> primaryKeyValues,
            String commitScn)
            throws SQLException {
        String scn = OracleLobReselectSql.usableCommitScn(commitScn);
        if (scn != null) {
            try {
                return execute(
                        connection,
                        cache,
                        OracleLobReselectSql.buildQuery(
                                schema, table, lobColumns, primaryKeyColumns, scn),
                        lobColumns,
                        primaryKeyValues,
                        Long.valueOf(scn));
            } catch (SQLException e) {
                if (!OracleLobReselectSql.isFlashbackUnavailable(e)) {
                    throw e;
                }
                LOG.warn(
                        "Flashback LOB re-select failed for {}.{} at SCN {}. "
                                + "Reading the current row instead. Grant FLASHBACK ANY TABLE, "
                                + "or FLASHBACK on the table, to read the committed image. "
                                + "The current row can differ if it changed again.",
                        schema,
                        table,
                        scn,
                        e);
            }
        }
        return execute(
                connection,
                cache,
                OracleLobReselectSql.buildQuery(schema, table, lobColumns, primaryKeyColumns, null),
                lobColumns,
                primaryKeyValues,
                null);
    }

    private Connection openConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        closeCachedStatements();
        if (url == null || url.trim().isEmpty()) {
            throw new SQLException(
                    "Oracle LOB re-select requires the Oracle-CDC url option to open a JDBC connection");
        }
        try {
            Class.forName(ORACLE_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new SQLException(
                    "Oracle JDBC driver " + ORACLE_DRIVER + " was not found for LOB re-select", e);
        }
        connection = DriverManager.getConnection(url, username, password);
        connection.setAutoCommit(true);
        pdbSelected = false;
        selectPdb(connection);
        LOG.info(
                "Opened Oracle LOB re-select connection for {}{}",
                url,
                pdbName == null || pdbName.trim().isEmpty() ? "" : ", pdb " + pdbName);
        return connection;
    }

    private void selectPdb(Connection jdbc) throws SQLException {
        if (pdbSelected || pdbName == null || pdbName.trim().isEmpty()) {
            return;
        }
        try (Statement statement = jdbc.createStatement()) {
            statement.execute(
                    "ALTER SESSION SET CONTAINER = " + OracleLobReselectSql.quote(pdbName));
        }
        pdbSelected = true;
    }

    private Map<String, PreparedStatement> statementCache() {
        if (statementCache == null) {
            statementCache = new HashMap<String, PreparedStatement>();
        }
        return statementCache;
    }

    private void closeCachedStatements() {
        if (statementCache == null) {
            return;
        }
        for (PreparedStatement statement : statementCache.values()) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Failed to close a cached Oracle LOB re-select statement", e);
            }
        }
        statementCache = null;
    }

    private static OracleLobColumnReselector.ReselectResult execute(
            Connection connection,
            Map<String, PreparedStatement> cache,
            String sql,
            List<String> lobColumns,
            List<Object> primaryKeyValues,
            Long commitScn)
            throws SQLException {
        PreparedStatement statement = cachedStatement(connection, cache, sql);
        boolean keepOpen = cache != null;
        try {
            int index = 1;
            if (commitScn != null) {
                statement.setLong(index++, commitScn.longValue());
            }
            for (int i = 0; i < primaryKeyValues.size(); i++) {
                statement.setObject(index++, primaryKeyValues.get(i));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return OracleLobColumnReselector.ReselectResult.notFound();
                }
                Map<String, Object> values = new HashMap<String, Object>();
                for (int i = 0; i < lobColumns.size(); i++) {
                    values.put(lobColumns.get(i), readValue(resultSet, i + 1));
                }
                return OracleLobColumnReselector.ReselectResult.found(values);
            }
        } catch (SQLException e) {
            if (keepOpen) {
                discard(cache, sql, statement);
            }
            throw e;
        } finally {
            if (!keepOpen) {
                statement.close();
            }
        }
    }

    private static PreparedStatement cachedStatement(
            Connection connection, Map<String, PreparedStatement> cache, String sql)
            throws SQLException {
        if (cache == null) {
            return connection.prepareStatement(sql);
        }
        PreparedStatement existing = cache.get(sql);
        if (existing != null) {
            try {
                if (!existing.isClosed()) {
                    existing.clearParameters();
                    return existing;
                }
            } catch (SQLException e) {
                cache.remove(sql);
            }
        }
        PreparedStatement created = connection.prepareStatement(sql);
        cache.put(sql, created);
        return created;
    }

    private static void discard(
            Map<String, PreparedStatement> cache, String sql, PreparedStatement statement) {
        if (cache != null) {
            cache.remove(sql);
        }
        try {
            statement.close();
        } catch (SQLException e) {
            LOG.warn("Failed to close a failed Oracle LOB re-select statement", e);
        }
    }

    static Object readValue(ResultSet resultSet, int index) throws SQLException {
        Object value = resultSet.getObject(index);
        if (value instanceof Clob) {
            Clob clob = (Clob) value;
            long length = clob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException(
                        "CLOB length " + length + " exceeds the supported 2GB limit");
            }
            return clob.getSubString(1, (int) length);
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            long length = blob.length();
            if (length > Integer.MAX_VALUE) {
                throw new SQLException(
                        "BLOB length " + length + " exceeds the supported 2GB limit");
            }
            return blob.getBytes(1, (int) length);
        }
        if (value instanceof SQLXML) {
            SQLXML xml = (SQLXML) value;
            try {
                return xml.getString();
            } finally {
                xml.free();
            }
        }
        if (value != null && value.getClass().getName().endsWith("XMLType")) {
            return xmlTypeString(value);
        }
        return value;
    }

    /**
     * {@code oracle.xdb.XMLType} exposes {@code getStringVal()} and does not always implement
     * {@link SQLXML}. Read that method so a re-selected XMLTYPE column is a string.
     */
    private static String xmlTypeString(Object value) throws SQLException {
        if (value == null || !value.getClass().getName().endsWith("XMLType")) {
            return null;
        }
        try {
            Method getStringVal = value.getClass().getMethod("getStringVal");
            Object text = getStringVal.invoke(value);
            return text == null ? null : text.toString();
        } catch (ReflectiveOperationException e) {
            throw new SQLException(
                    "Failed to read Oracle XMLType value from " + value.getClass().getName(), e);
        }
    }
}
