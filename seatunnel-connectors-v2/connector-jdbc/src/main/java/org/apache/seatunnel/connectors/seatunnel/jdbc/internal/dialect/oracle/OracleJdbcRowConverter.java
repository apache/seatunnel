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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter.ORACLE_BLOB;

@Slf4j
public class OracleJdbcRowConverter extends AbstractJdbcRowConverter {

    @Override
    public String converterName() {
        return DatabaseIdentifier.ORACLE;
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
                statement.setBinaryStream(statementIndex, new ByteArrayInputStream((byte[]) value));
            } else {
                statement.setBytes(statementIndex, (byte[]) value);
            }
            return;
        }
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            OffsetDateTime odt = (OffsetDateTime) value;
            try {
                statement.setObject(statementIndex, odt);
                return;
            } catch (AbstractMethodError | SQLException e) {
                log.debug(
                        "JDBC 4.2 setObject(OffsetDateTime) failed, trying Oracle-specific approach",
                        e);
            }

            try {
                java.sql.Connection conn = statement.getConnection();
                oracle.jdbc.OracleConnection oracleConn =
                        conn.unwrap(oracle.jdbc.OracleConnection.class);
                String iso = odt.toString();
                String oracleLiteral = iso.replace('T', ' ');
                oracle.sql.TIMESTAMPTZ tsTz = new oracle.sql.TIMESTAMPTZ(oracleConn, oracleLiteral);
                statement.setObject(statementIndex, tsTz);
                return;
            } catch (Throwable t) {
                log.debug(
                        "Oracle-specific TIMESTAMPTZ handling failed, using instant conversion", t);
                try {
                    statement.setTimestamp(
                            statementIndex, java.sql.Timestamp.from(odt.toInstant()));
                    return;
                } catch (SQLException se) {
                    log.error("Failed to set TIMESTAMP_TZ value using all fallback methods", se);
                    throw se;
                }
            }
        }
        super.setValueToStatementByDataType(
                value, statement, seaTunnelDataType, statementIndex, sourceType);
    }
}
