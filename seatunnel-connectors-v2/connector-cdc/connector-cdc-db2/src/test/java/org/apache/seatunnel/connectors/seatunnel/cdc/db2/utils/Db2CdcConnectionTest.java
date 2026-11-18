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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.connector.db2.Db2ChangeTable;
import io.debezium.connector.db2.Lsn;
import io.debezium.relational.TableId;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * Protects active Capture watermarks and raw capture-name quoting at the JDBC discovery boundary.
 */
class Db2CdcConnectionTest {

    @Test
    void shouldKeepActiveCaptureOpenAfterOldSynchpoint() throws SQLException {
        Db2ChangeTable table = discoverCapture("DB2INST1_CUSTOMERS", lsn(7));

        Assertions.assertEquals(new TableId("", "DB2INST1", "CUSTOMERS"), table.getSourceTableId());
        Assertions.assertEquals(42, table.getChangeTableObjectId());
        Assertions.assertEquals(Lsn.valueOf(lsn(7)), table.getStartLsn());
        Assertions.assertEquals(Lsn.NULL, table.getStopLsn());
        Assertions.assertFalse(table.getStopLsn().isAvailable());
        Assertions.assertEquals(
                new TableId("", "ASNCDC", "DB2INST1_CUSTOMERS"), table.getChangeTableId());

        // The reader remains free to close an older instance at a successor's start LSN.
        table.setStopLsn(Lsn.valueOf(lsn(19)));
        Assertions.assertEquals(Lsn.valueOf(lsn(19)), table.getStopLsn());
    }

    @Test
    void shouldQuoteMixedCaseCaptureNameExactlyOnce() throws SQLException {
        Db2ChangeTable table = discoverCapture("CustomersCapture", lsn(3));

        Assertions.assertEquals("\"CustomersCapture\"", table.getCaptureInstance());
        Assertions.assertEquals(
                new TableId("", "ASNCDC", "\"CustomersCapture\""), table.getChangeTableId());
        Assertions.assertEquals(Lsn.valueOf(lsn(3)), table.getStartLsn());
        Assertions.assertEquals(Lsn.NULL, table.getStopLsn());
    }

    @Test
    void shouldPreserveUnavailableOldSynchpoint() throws SQLException {
        Db2ChangeTable table = discoverCapture("DB2INST1_CUSTOMERS", null);

        Assertions.assertEquals(Lsn.NULL, table.getStartLsn());
        Assertions.assertEquals(Lsn.NULL, table.getStopLsn());
    }

    private static Db2ChangeTable discoverCapture(String captureName, byte[] oldSynchpoint)
            throws SQLException {
        Db2CdcConnection connection =
                Mockito.mock(Db2CdcConnection.class, Mockito.CALLS_REAL_METHODS);
        Connection jdbc = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet rows = Mockito.mock(ResultSet.class);
        Mockito.doReturn(jdbc).when(connection).connection();
        Mockito.when(jdbc.createStatement()).thenReturn(statement);
        // The returned column is OLD, never the advancing NEW synchronization point.
        Mockito.when(statement.executeQuery(Mockito.contains("r.CD_OLD_SYNCHPOINT")))
                .thenReturn(rows);
        Mockito.when(rows.next()).thenReturn(true, false);
        Mockito.when(rows.getString(1)).thenReturn("DB2INST1");
        Mockito.when(rows.getString(2)).thenReturn("CUSTOMERS");
        Mockito.when(rows.getString(3)).thenReturn(captureName);
        Mockito.when(rows.getBytes(4)).thenReturn(oldSynchpoint);
        Mockito.when(rows.getInt(5)).thenReturn(42);

        Set<Db2ChangeTable> tables = connection.listOfChangeTables();
        Assertions.assertEquals(1, tables.size());
        return tables.iterator().next();
    }

    private static byte[] lsn(int value) {
        byte[] bytes = new byte[16];
        bytes[15] = (byte) value;
        return bytes;
    }
}
