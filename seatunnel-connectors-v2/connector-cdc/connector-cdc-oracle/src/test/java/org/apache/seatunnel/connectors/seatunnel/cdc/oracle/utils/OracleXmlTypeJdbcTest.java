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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import oracle.jdbc.OracleTypes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleXmlTypeJdbcTest {

    @Test
    public void detectsXmlTypeFromJdbcTypeAndTypeName() {
        Column sqlXml =
                Column.editor()
                        .name("VAL_XML")
                        .type("XMLTYPE")
                        .jdbcType(OracleTypes.SQLXML)
                        .optional(true)
                        .position(1)
                        .create();
        Column opaque =
                Column.editor()
                        .name("VAL_XML")
                        .type("SYS.XMLTYPE")
                        .jdbcType(Types.OTHER)
                        .optional(true)
                        .position(1)
                        .create();
        Column clob =
                Column.editor()
                        .name("VAL_CLOB")
                        .type("CLOB")
                        .jdbcType(Types.CLOB)
                        .optional(true)
                        .position(1)
                        .create();

        Assertions.assertTrue(OracleXmlTypeJdbc.isXmlType(sqlXml));
        Assertions.assertTrue(OracleXmlTypeJdbc.isXmlType(opaque));
        Assertions.assertFalse(OracleXmlTypeJdbc.isXmlType(clob));
        Assertions.assertTrue(OracleXmlTypeJdbc.isXmlTypeName("xmltype"));
        Assertions.assertFalse(OracleXmlTypeJdbc.isXmlTypeName("CLOB"));
    }

    @Test
    public void readsSqlXmlWithoutCallingGetObject() throws Exception {
        SQLXML xml = mock(SQLXML.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getSQLXML(2)).thenReturn(xml);
        when(resultSet.wasNull()).thenReturn(false);
        when(resultSet.getObject(2)).thenThrow(new NoClassDefFoundError("oracle/xdb/XMLType"));

        Assertions.assertSame(xml, OracleXmlTypeJdbc.read(resultSet, 2));
        verify(resultSet).getSQLXML(2);
        verify(resultSet, never()).getObject(2);
    }

    @Test
    public void fallsBackToGetStringWhenSqlXmlIsUnsupported() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getSQLXML(1)).thenThrow(new SQLException("not supported"));
        when(resultSet.getString(1)).thenReturn("<root/>");
        when(resultSet.wasNull()).thenReturn(false);

        Assertions.assertEquals("<root/>", OracleXmlTypeJdbc.read(resultSet, 1));
    }

    @Test
    public void asStringFreesSqlXml() throws Exception {
        SQLXML xml = mock(SQLXML.class);
        when(xml.getString()).thenReturn("<a/>");
        Assertions.assertEquals("<a/>", OracleXmlTypeJdbc.asString(xml));
        verify(xml).free();
    }

    @Test
    public void readNullSqlXmlIsNull() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getSQLXML(1)).thenReturn(null);
        when(resultSet.wasNull()).thenReturn(true);
        Assertions.assertNull(OracleXmlTypeJdbc.read(resultSet, 1));
    }
}
