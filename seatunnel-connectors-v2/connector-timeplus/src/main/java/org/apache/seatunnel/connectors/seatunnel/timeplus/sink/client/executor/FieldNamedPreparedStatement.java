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

package org.apache.seatunnel.connectors.seatunnel.timeplus.sink.client.executor;

import lombok.RequiredArgsConstructor;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@RequiredArgsConstructor
public class FieldNamedPreparedStatement implements PreparedStatement {
    private final PreparedStatement statement;
    private final int[][] indexMapping;

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setNull(index, sqlType);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setBoolean(index, x);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setByte(index, x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setShort(index, x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setInt(index, x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setLong(index, x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setFloat(index, x);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setDouble(index, x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setBigDecimal(index, x);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setString(index, x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setBytes(index, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setDate(index, x);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setTime(index, x);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setTimestamp(index, x);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setObject(index, x, targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setObject(index, x);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setRef(index, x);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setBlob(index, x);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setClob(index, x);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setArray(index, x);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setDate(index, x, cal);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setTime(index, x, cal);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setTimestamp(index, x, cal);
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setNull(index, sqlType, typeName);
        }
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setURL(index, x);
        }
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setRowId(index, x);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setNString(index, value);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setNClob(index, value);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setSQLXML(index, xmlObject);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        for (int index : indexMapping[parameterIndex - 1]) {
            statement.setObject(index, x, targetSqlType, scaleOrLength);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean execute() throws SQLException {
        return statement.execute();
    }

    @Override
    public void addBatch() throws SQLException {
        statement.addBatch();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return statement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {
        return statement.executeUpdate();
    }

    @Override
    public void clearParameters() throws SQLException {
        statement.clearParameters();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return statement.getMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return statement.getParameterMetaData();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        statement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return statement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return statement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return statement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return statement.isWrapperFor(iface);
    }

    public static FieldNamedPreparedStatement prepareStatement(
            Connection connection, String sql, String[] fieldNames) throws SQLException {
        checkNotNull(connection, "connection must not be null.");
        checkNotNull(sql, "sql must not be null.");
        checkNotNull(fieldNames, "fieldNames must not be null.");

        int[][] indexMapping = new int[fieldNames.length][];
        String parsedSQL;
        if (sql.contains("?")) {
            parsedSQL = sql;
            for (int i = 0; i < fieldNames.length; i++) {
                // SQL statement parameter index starts from 1
                indexMapping[i] = new int[] {i + 1};
            }
        } else {
            HashMap<String, List<Integer>> parameterMap = new HashMap<>();
            parsedSQL = parseNamedStatement(sql, parameterMap);
            // currently, the statements must contain all the field parameters
            checkArgument(parameterMap.size() >= fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                checkArgument(
                        parameterMap.containsKey(fieldName),
                        fieldName + " doesn't exist in the parameters of SQL statement: " + sql);
                indexMapping[i] = parameterMap.get(fieldName).stream().mapToInt(v -> v).toArray();
            }
        }
        return new FieldNamedPreparedStatement(
                connection.prepareStatement(parsedSQL), indexMapping);
    }

    public static String parseNamedStatement(String sql, Map<String, List<Integer>> paramMap) {
        StringBuilder parsedSql = new StringBuilder();
        int fieldIndex = 1; // SQL statement parameter index starts from 1
        int length = sql.length();
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            if (':' == c) {
                int j = i + 1;
                while (j < length
                        && (Character.isJavaIdentifierPart(sql.charAt(j))
                                || ".".equals(String.valueOf(sql.charAt(j))))) {
                    j++;
                }
                String parameterName = sql.substring(i + 1, j);
                checkArgument(
                        !parameterName.isEmpty(),
                        "Named parameters in SQL statement must not be empty.");
                paramMap.computeIfAbsent(parameterName, n -> new ArrayList<>()).add(fieldIndex);
                fieldIndex++;
                i = j - 1;
                parsedSql.append('?');
            } else {
                parsedSql.append(c);
            }
        }
        return parsedSql.toString();
    }
}
