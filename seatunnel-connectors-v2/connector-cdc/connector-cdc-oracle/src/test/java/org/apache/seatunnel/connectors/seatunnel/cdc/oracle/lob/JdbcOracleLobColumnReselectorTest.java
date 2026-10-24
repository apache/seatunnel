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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcOracleLobColumnReselectorTest {

    @Test
    public void readsSqlXmlAsString() throws Exception {
        SQLXML xml = Mockito.mock(SQLXML.class);
        when(xml.getString()).thenReturn("<a>1</a>");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(xml);

        Assertions.assertEquals("<a>1</a>", JdbcOracleLobColumnReselector.readValue(resultSet, 1));
        verify(xml).free();
    }

    @Test
    public void readsOracleXmlTypeThroughGetStringVal() throws Exception {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(new SampleXMLType("<b/>"));

        Assertions.assertEquals("<b/>", JdbcOracleLobColumnReselector.readValue(resultSet, 1));
    }

    @Test
    public void readsANullOracleXmlTypeAsNull() throws Exception {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getObject(1)).thenReturn(new SampleXMLType(null));

        Assertions.assertNull(JdbcOracleLobColumnReselector.readValue(resultSet, 1));
    }

    /**
     * After Java deserialization the transient flashbackUnavailableTables field is null and its
     * field initializer does not run. Lazy re-init must restore the skip set so a second reselect
     * for the same table goes straight to the current-row SQL.
     */
    @Test
    public void skipsFlashbackAfterDeserializationOnceTableIsRecorded() throws Exception {
        JdbcOracleLobColumnReselector original =
                new JdbcOracleLobColumnReselector(
                        "jdbc:oracle:thin:@//localhost:1521/ORCL", "u", "p", null);
        JdbcOracleLobColumnReselector restored = roundTrip(original);

        Field flashbackField =
                JdbcOracleLobColumnReselector.class.getDeclaredField("flashbackUnavailableTables");
        flashbackField.setAccessible(true);
        Assertions.assertNull(
                flashbackField.get(restored),
                "transient flashbackUnavailableTables must be null after deserialization");

        Connection connection = mock(Connection.class);
        when(connection.isClosed()).thenReturn(false);
        Field connectionField = JdbcOracleLobColumnReselector.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(restored, connection);

        PreparedStatement flashback = mock(PreparedStatement.class);
        PreparedStatement current = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        AtomicInteger flashbackPrepares = new AtomicInteger();
        when(connection.prepareStatement(ArgumentMatchers.anyString()))
                .thenAnswer(
                        invocation -> {
                            String sql = invocation.getArgument(0);
                            if (sql.contains("AS OF SCN")) {
                                flashbackPrepares.incrementAndGet();
                                return flashback;
                            }
                            return current;
                        });
        when(flashback.executeQuery())
                .thenThrow(new SQLException("ORA-01555: snapshot too old", "72000", 1555));
        when(current.isClosed()).thenReturn(false);
        when(current.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject(1)).thenReturn("lob-after-fallback");

        for (int i = 0; i < 2; i++) {
            OracleLobColumnReselector.ReselectResult result =
                    restored.reselect(
                            "DEBEZIUM",
                            "LOB_TYPES",
                            Collections.singletonList("VAL_CLOB"),
                            Collections.singletonList("ID"),
                            Collections.singletonList(11),
                            "12345");
            Assertions.assertTrue(result.isFound());
            Assertions.assertEquals("lob-after-fallback", result.getValues().get("VAL_CLOB"));
        }

        Assertions.assertEquals(
                1,
                flashbackPrepares.get(),
                "flashback AS OF SCN path must run only once after the table is recorded");
        verify(flashback, times(1)).executeQuery();
        verify(current, times(2)).executeQuery();

        @SuppressWarnings("unchecked")
        Set<String> recorded = (Set<String>) flashbackField.get(restored);
        Assertions.assertNotNull(recorded);
        Assertions.assertTrue(recorded.contains("DEBEZIUM.LOB_TYPES"));
    }

    private static JdbcOracleLobColumnReselector roundTrip(JdbcOracleLobColumnReselector source)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(source);
        }
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (JdbcOracleLobColumnReselector) in.readObject();
        }
    }

    /** Name ends with XMLType so the re-selector takes the xdb getStringVal path. */
    public static class SampleXMLType {
        private final String text;

        SampleXMLType(String text) {
            this.text = text;
        }

        public String getStringVal() {
            return text;
        }
    }
}
