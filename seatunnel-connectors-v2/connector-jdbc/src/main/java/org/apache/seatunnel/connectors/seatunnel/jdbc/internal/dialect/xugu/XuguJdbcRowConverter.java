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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

@Slf4j
public class XuguJdbcRowConverter extends AbstractJdbcRowConverter {

    private static final java.util.concurrent.atomic.AtomicBoolean TIMESTAMP_TZ_WARNED =
            new java.util.concurrent.atomic.AtomicBoolean(false);

    @Override
    public String converterName() {
        return DatabaseIdentifier.XUGU;
    }

    @Override
    protected void setValueToStatementByDataType(
            Object value,
            PreparedStatement statement,
            SeaTunnelDataType<?> seaTunnelDataType,
            int statementIndex,
            @Nullable String sourceType)
            throws SQLException {
        if (seaTunnelDataType.getSqlType().equals(SqlType.TIMESTAMP_TZ)) {
            // LOSSY PATH: Xugu JDBC driver crashes on batch writes of OffsetDateTime /
            // timezone-formatted strings for TIMESTAMP WITH TIME ZONE columns (bug [E19138]).
            // The timezone offset is intentionally dropped; only the wall-clock value is stored.
            // This is a documented limitation — not silent data loss.
            if (TIMESTAMP_TZ_WARNED.compareAndSet(false, true)) {
                log.warn(
                        "TIMESTAMP_TZ is written to Xugu as a plain TIMESTAMP: the timezone"
                                + " offset is dropped and only the wall-clock value is stored."
                                + " This is a known Xugu JDBC driver limitation (bug [E19138]).");
            }
            OffsetDateTime odt = (OffsetDateTime) value;
            statement.setTimestamp(statementIndex, Timestamp.valueOf(odt.toLocalDateTime()));
        } else {
            super.setValueToStatementByDataType(
                    value, statement, seaTunnelDataType, statementIndex, sourceType);
        }
    }
}
