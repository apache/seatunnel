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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.StatementBoundLobs;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_NCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_NCLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_NVARCHAR2;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_VARCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_VARCHAR2;

public class OracleJdbcRowConverter extends AbstractJdbcRowConverter {

    /**
     * ojdbc8 19.18 sends CLOB and NCLOB character streams in blocks of 8192 UTF-16 code units. A
     * supplementary character that crosses a block boundary is stored as U+FFFD U+FFFD. Values
     * shorter than one block cannot cross that boundary.
     */
    static final int LOB_UTF16_BLOCK = 8192;

    @Override
    public String converterName() {
        return DatabaseIdentifier.ORACLE;
    }

    @Override
    protected void setNullToStatementByDataType(
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (ORACLE_CLOB.equals(sourceType)) {
            statement.setNull(statementIndex, Types.CLOB);
            return;
        }
        if (ORACLE_NCLOB.equals(sourceType)) {
            statement.setNull(statementIndex, Types.NCLOB);
            return;
        }
        if (ORACLE_BLOB.equals(sourceType)) {
            statement.setNull(statementIndex, Types.BLOB);
            return;
        }
        if (ORACLE_CHAR.equals(sourceType)
                || ORACLE_NCHAR.equals(sourceType)
                || ORACLE_VARCHAR.equals(sourceType)
                || ORACLE_VARCHAR2.equals(sourceType)
                || ORACLE_NVARCHAR2.equals(sourceType)) {
            statement.setNull(statementIndex, Types.VARCHAR);
            return;
        }

        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
                statement.setNull(statementIndex, Types.VARCHAR);
                break;
            case BOOLEAN:
                statement.setNull(statementIndex, Types.INTEGER);
                break;
            case TINYINT:
                statement.setNull(statementIndex, Types.TINYINT);
                break;
            case SMALLINT:
                statement.setNull(statementIndex, Types.SMALLINT);
                break;
            case INT:
                statement.setNull(statementIndex, Types.INTEGER);
                break;
            case BIGINT:
                statement.setNull(statementIndex, Types.BIGINT);
                break;
            case FLOAT:
                statement.setNull(statementIndex, Types.FLOAT);
                break;
            case DOUBLE:
                statement.setNull(statementIndex, Types.DOUBLE);
                break;
            case DECIMAL:
                statement.setNull(statementIndex, Types.DECIMAL);
                break;
            case DATE:
                statement.setNull(statementIndex, Types.DATE);
                break;
            case TIME:
                statement.setNull(statementIndex, Types.TIME);
                break;
            case TIMESTAMP:
                statement.setNull(statementIndex, Types.TIMESTAMP);
                break;
            case TIMESTAMP_TZ:
                statement.setNull(statementIndex, Types.TIMESTAMP_WITH_TIMEZONE);
                break;
            case BYTES:
                statement.setNull(statementIndex, Types.BINARY);
                break;
            case NULL:
                statement.setNull(statementIndex, Types.NULL);
                break;
            case ARRAY:
                statement.setNull(statementIndex, Types.ARRAY);
                break;
            case MAP:
            case ROW:
            default:
                super.setNullToStatementByDataType(
                        statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.BYTES)) {
            if (ORACLE_BLOB.equals(sourceType)) {
                byte[] bytes = (byte[]) value;
                if (bytes.length == 0) {
                    // A zero-length binary stream is stored as NULL. An empty locator stays empty.
                    Blob blob = statement.getConnection().createBlob();
                    statement.setBlob(statementIndex, blob);
                    StatementBoundLobs.track(statement, blob);
                } else {
                    statement.setBinaryStream(
                            statementIndex, new ByteArrayInputStream(bytes), bytes.length);
                }
            } else {
                statement.setBytes(statementIndex, (byte[]) value);
            }
        } else if (seaTunnelDataType.getSqlType().equals(SqlType.STRING)) {
            if (ORACLE_CLOB.equals(sourceType)) {
                setNationalLob(statement, statementIndex, (String) value, false);
            } else if (ORACLE_NCLOB.equals(sourceType)) {
                setNationalLob(statement, statementIndex, (String) value, true);
            } else {
                statement.setString(statementIndex, (String) value);
            }
        } else if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            // Delegate to AbstractJdbcRowConverter which uses setObject() first (preserving the
            // offset for Oracle JDBC 12.2+) and falls back to setTimestamp() with an explicit UTC
            // Calendar for older drivers, avoiding JVM-default-timezone corruption.
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        } else {
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }

    /**
     * Binds a CLOB or NCLOB. Long values go through {@link Connection#createClob()} or {@link
     * Connection#createNClob()} and {@link Clob#setString(long, String)}, which keeps a surrogate
     * pair that sits on an 8192-code-unit boundary. The temporary LOB is freed after the batch
     * executes.
     */
    private static void setNationalLob(
            PreparedStatement statement, int statementIndex, String value, boolean national)
            throws SQLException {
        // ojdbc stores a zero-length character stream as NULL. An empty temporary locator is an
        // empty LOB, matching a source EMPTY_CLOB(). setString(1, "") can clear that locator.
        if (value.isEmpty()) {
            Connection connection = statement.getConnection();
            if (national) {
                NClob nclob = connection.createNClob();
                statement.setNClob(statementIndex, nclob);
                StatementBoundLobs.track(statement, nclob);
            } else {
                Clob clob = connection.createClob();
                statement.setClob(statementIndex, clob);
                StatementBoundLobs.track(statement, clob);
            }
            return;
        }
        if (value.length() < LOB_UTF16_BLOCK) {
            StringReader reader = new StringReader(value);
            if (national) {
                statement.setNCharacterStream(statementIndex, reader, value.length());
            } else {
                statement.setCharacterStream(statementIndex, reader, value.length());
            }
            return;
        }
        Connection connection = statement.getConnection();
        if (national) {
            NClob nclob = connection.createNClob();
            nclob.setString(1, value);
            statement.setNClob(statementIndex, nclob);
            StatementBoundLobs.track(statement, nclob);
        } else {
            Clob clob = connection.createClob();
            clob.setString(1, value);
            statement.setClob(statementIndex, clob);
            StatementBoundLobs.track(statement, clob);
        }
    }
}
