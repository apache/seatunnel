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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/** Oracle connection Utilities. */
public class OracleConnectionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectionUtils.class);

    /** Returned by column metadata in Oracle if no scale is set. */
    private static final int ORACLE_UNSET_SCALE = -127;

    /** show current scn sql in oracle. */
    private static final String SHOW_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";

    /** Creates a new {@link OracleConnection}, but not open the connection. */
    public static OracleConnection createOracleConnection(JdbcConfiguration dbzConfiguration) {
        Configuration configuration = dbzConfiguration.subset(DATABASE_CONFIG_PREFIX, true);

        return new OracleConnection(
                configuration.isEmpty() ? dbzConfiguration : JdbcConfiguration.adapt(configuration),
                OracleConnectionUtils.class::getClassLoader);
    }

    /** Fetch current redoLog offsets in Oracle Server. */
    public static RedoLogOffset currentRedoLogOffset(JdbcConnection jdbc) {
        try {
            return jdbc.queryAndMap(
                    SHOW_CURRENT_SCN,
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            return new RedoLogOffset(Scn.valueOf(scn).longValue());
                        } else {
                            throw new SeaTunnelException(
                                    "Cannot read the scn via '"
                                            + SHOW_CURRENT_SCN
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    "Cannot read the redo log position via '"
                            + SHOW_CURRENT_SCN
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    /**
     * Convert timestamp (milliseconds since epoch) to Oracle SCN.
     *
     * @param jdbc JDBC connection
     * @param timestampMs timestamp in milliseconds since epoch
     * @param serverTimeZone database server time zone
     * @return RedoLogOffset with the corresponding SCN
     */
    public static RedoLogOffset timestampToScn(
            JdbcConnection jdbc, long timestampMs, String serverTimeZone) {
        try {
            String effectiveServerTimeZone =
                    serverTimeZone == null ? TimeZone.getDefault().getID() : serverTimeZone;
            LOG.info(
                    "Converting timestamp {} to SCN with server time zone {}",
                    timestampMs,
                    effectiveServerTimeZone);
            String sql = "SELECT TIMESTAMP_TO_SCN(?) AS SCN FROM DUAL";
            return jdbc.prepareQueryAndMap(
                    sql,
                    statement -> {
                        java.sql.Timestamp timestamp = new java.sql.Timestamp(timestampMs);
                        Calendar calendar =
                                Calendar.getInstance(TimeZone.getTimeZone(effectiveServerTimeZone));
                        statement.setTimestamp(1, timestamp, calendar);
                    },
                    rs -> {
                        if (rs.next()) {
                            final String scn = rs.getString(1);
                            LOG.info("Converted timestamp {} to SCN: {}", timestampMs, scn);
                            return new RedoLogOffset(Scn.valueOf(scn).longValue());
                        } else {
                            throw new SeaTunnelException(
                                    "Cannot convert timestamp to SCN. Make sure the specified timestamp is valid.");
                        }
                    });
        } catch (SQLException e) {
            LOG.error("Failed to convert timestamp to SCN", e);
            throw new SeaTunnelException("Failed to convert timestamp to SCN", e);
        }
    }

    public static List<TableId> listTables(
            JdbcConnection jdbcConnection, String database, RelationalTableFilters tableFilters)
            throws SQLException {
        final List<TableId> capturedTableIds = new ArrayList<>();

        Set<TableId> tableIdSet = new HashSet<>();
        String queryTablesSql =
                "SELECT OWNER ,TABLE_NAME,TABLESPACE_NAME FROM ALL_TABLES \n"
                        + "WHERE PARTITIONED = 'YES' OR (TABLESPACE_NAME IS NOT NULL AND TABLESPACE_NAME NOT IN ('SYSAUX'))";

        try {
            jdbcConnection.query(
                    queryTablesSql,
                    rs -> {
                        while (rs.next()) {
                            String schemaName = rs.getString(1);
                            String tableName = rs.getString(2);
                            TableId tableId = new TableId(database, schemaName, tableName);
                            tableIdSet.add(tableId);
                        }
                    });
        } catch (SQLException e) {
            LOG.warn(" SQL execute error, sql:{}", queryTablesSql, e);
        }

        for (TableId tableId : tableIdSet) {
            if (tableFilters.dataCollectionFilter().isIncluded(tableId)) {
                capturedTableIds.add(tableId);
                LOG.info("\t including '{}' for further processing", tableId);
            } else {
                LOG.debug("\t '{}' is filtered out of capturing", tableId);
            }
        }

        return capturedTableIds;
    }
}
