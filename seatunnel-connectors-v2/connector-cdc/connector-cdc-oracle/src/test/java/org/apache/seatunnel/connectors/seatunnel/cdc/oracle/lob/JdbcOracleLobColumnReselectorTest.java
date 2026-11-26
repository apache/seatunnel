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
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLXML;
import java.sql.Types;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JdbcOracleLobColumnReselectorTest {

    @Test
    public void redactsEmbeddedOracleCredentialsFromLoggedUrl() {
        String redacted =
                JdbcOracleLobColumnReselector.redactJdbcUrl(
                        "jdbc:oracle:thin:scott/tiger@//localhost:1521/ORCL");
        Assertions.assertEquals("jdbc:oracle:thin:***/***@//localhost:1521/ORCL", redacted);
        Assertions.assertFalse(redacted.contains("scott"));
        Assertions.assertFalse(redacted.contains("tiger"));
    }

    @Test
    public void redactsAuthorityAndQueryCredentialsFromLoggedUrl() {
        String authorityRedacted =
                JdbcOracleLobColumnReselector.redactJdbcUrl(
                        "jdbc:oracle:thin://scott:tiger@localhost:1521/ORCL");
        Assertions.assertEquals(
                "jdbc:oracle:thin://***:***@localhost:1521/ORCL", authorityRedacted);

        String queryRedacted =
                JdbcOracleLobColumnReselector.redactJdbcUrl(
                        "jdbc:oracle:thin:@//localhost:1521/ORCL"
                                + "?user=scott&password=tiger&ssl=true");
        Assertions.assertEquals(
                "jdbc:oracle:thin:@//localhost:1521/ORCL?user=***&password=***&ssl=true",
                queryRedacted);
        Assertions.assertFalse(queryRedacted.contains("scott"));
        Assertions.assertFalse(queryRedacted.contains("tiger"));
    }

    @Test
    public void leavesCredentialFreeOracleUrlUnchanged() {
        String url = "jdbc:oracle:thin:@//localhost:1521/ORCL";
        Assertions.assertEquals(url, JdbcOracleLobColumnReselector.redactJdbcUrl(url));
    }

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
    public void readsXmlTypeThroughGetSqlXmlWithoutGetObject() throws Exception {
        SQLXML xml = Mockito.mock(SQLXML.class);
        when(xml.getString()).thenReturn("<root>sqlxml</root>");
        ResultSetMetaData meta = Mockito.mock(ResultSetMetaData.class);
        when(meta.getColumnType(1)).thenReturn(Types.SQLXML);
        when(meta.getColumnTypeName(1)).thenReturn("XMLTYPE");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.getMetaData()).thenReturn(meta);
        when(resultSet.getSQLXML(1)).thenReturn(xml);
        when(resultSet.getObject(1)).thenThrow(new NoClassDefFoundError("oracle/xdb/XMLType"));

        Assertions.assertEquals(
                "<root>sqlxml</root>", JdbcOracleLobColumnReselector.readValue(resultSet, 1));
        verify(resultSet).getSQLXML(1);
        verify(resultSet, never()).getObject(1);
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
