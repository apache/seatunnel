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

package org.apache.seatunnel.connectors.seatunnel.cdc.db2.utils;

import io.debezium.connector.db2.Db2ChangeTable;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Lsn;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Maps active ASN Capture registrations to Debezium's change-table lifecycle.
 *
 * <p>Debezium 1.9.8 maps the advancing NEW/OLD synchronization points to start/stop LSNs. An active
 * registration instead starts at OLD and has no stop LSN. The streaming reader can still assign a
 * stop LSN when it discovers a successor capture instance.
 */
class Db2CdcConnection extends Db2Connection {

    private static final String CAPTURE_TABLES_QUERY =
            "SELECT r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_TABLE, r.CD_OLD_SYNCHPOINT, "
                    + "CAST((t.TBSPACEID * 65536 + t.TABLEID) AS INTEGER) "
                    + "FROM ASNCDC.IBMSNAP_REGISTER r LEFT JOIN SYSCAT.TABLES t "
                    + "ON r.SOURCE_OWNER = t.TABSCHEMA AND r.SOURCE_TABLE = t.TABNAME "
                    + "WHERE r.SOURCE_OWNER <> ''";

    Db2CdcConnection(JdbcConfiguration configuration) {
        super(configuration);
    }

    /**
     * Reads raw capture names once so Debezium can quote mixed-case names without double quoting.
     */
    @Override
    public Set<Db2ChangeTable> listOfChangeTables() throws SQLException {
        return queryAndMap(
                CAPTURE_TABLES_QUERY,
                resultSet -> {
                    Set<Db2ChangeTable> tables = new HashSet<>();
                    while (resultSet.next()) {
                        tables.add(
                                new Db2ChangeTable(
                                        new TableId(
                                                "", resultSet.getString(1), resultSet.getString(2)),
                                        resultSet.getString(3),
                                        resultSet.getInt(5),
                                        Lsn.valueOf(resultSet.getBytes(4)),
                                        Lsn.NULL));
                    }
                    return tables;
                });
    }
}
