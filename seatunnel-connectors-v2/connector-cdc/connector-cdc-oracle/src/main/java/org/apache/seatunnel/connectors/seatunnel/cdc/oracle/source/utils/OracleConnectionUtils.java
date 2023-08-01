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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.utils;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.source.offset.RedoLogOffset;

import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.Scn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

import java.sql.SQLException;

import static io.debezium.config.CommonConnectorConfig.DATABASE_CONFIG_PREFIX;

/** Utils for Oracle connection. */
public class OracleConnectionUtils {

    /** show current scn sql in oracle. */
    private static final String SHOW_CURRENT_SCN = "SELECT CURRENT_SCN FROM V$DATABASE";

    /** Creates a new {@link OracleConnection}, but not open the connection. */
    public static OracleConnection createOracleConnection(Configuration dbzConfiguration) {
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
}
