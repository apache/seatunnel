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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresUtils;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.time.Conversions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class LsnOffsetFactory extends OffsetFactory {

    private final PostgresSourceConfig sourceConfig;

    private final PostgresDialect dialect;

    public LsnOffsetFactory(PostgresSourceConfigFactory configFactory, PostgresDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        return LsnOffset.INITIAL_OFFSET;
    }

    @Override
    public Offset neverStop() {
        return LsnOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return PostgresUtils.currentLsn((PostgresConnection) jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset committedOffset() {
        String slotName = sourceConfig.getDbzConfiguration().getString("slot.name");
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            try (PreparedStatement statement =
                    jdbcConnection
                            .connection()
                            .prepareStatement(
                                    "SELECT confirmed_flush_lsn::text, active_pid FROM pg_replication_slots WHERE slot_name = ?")) {
                statement.setString(1, slotName);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return readCommittedOffset(resultSet, slotName);
                }
            }
        } catch (SeaTunnelRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new SeaTunnelRuntimeException(
                    PostgresConnectorErrorCode.READ_COMMITTED_OFFSET_FAILED,
                    String.format(
                            "Could not read PostgreSQL replication slot '%s'. Verify that the slot still exists and is not being dropped.",
                            slotName),
                    e);
        }
    }

    static LsnOffset readCommittedOffset(ResultSet resultSet, String slotName) throws SQLException {
        if (!resultSet.next()) {
            throw invalidReplicationSlot(
                    slotName, "does not exist; create the logical replication slot first");
        }
        Object activePid = resultSet.getObject(2);
        if (activePid != null) {
            throw invalidReplicationSlot(
                    slotName,
                    String.format(
                            "is already active in PostgreSQL backend PID %s; stop the other consumer first",
                            activePid));
        }
        String committedLsn = resultSet.getString(1);
        if (resultSet.wasNull() || committedLsn == null || committedLsn.trim().isEmpty()) {
            throw invalidReplicationSlot(
                    slotName,
                    "has no usable confirmed_flush_lsn; wait until the slot has committed an LSN");
        }
        return toLsnOffset(committedLsn.trim());
    }

    private static SeaTunnelRuntimeException invalidReplicationSlot(
            String slotName, String reason) {
        return new SeaTunnelRuntimeException(
                PostgresConnectorErrorCode.READ_COMMITTED_OFFSET_FAILED,
                String.format("PostgreSQL replication slot '%s' %s.", slotName, reason));
    }

    static LsnOffset toLsnOffset(String committedLsn) {
        Map<String, String> offsetMap = new HashMap<>();
        String lsn = String.valueOf(Lsn.valueOf(committedLsn).asLong());
        offsetMap.put(SourceInfo.LSN_KEY, lsn);
        offsetMap.put(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY, lsn);
        offsetMap.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, lsn);
        offsetMap.put(
                SourceInfo.TIMESTAMP_USEC_KEY,
                String.valueOf(Conversions.toEpochMicros(Instant.MIN)));
        return LsnOffset.of(offsetMap);
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new LsnOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        throw new UnsupportedOperationException(
                "not supported create new Offset by filename and position.");
    }

    @Override
    public Offset timestamp(long timestamp) {
        throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
    }
}
