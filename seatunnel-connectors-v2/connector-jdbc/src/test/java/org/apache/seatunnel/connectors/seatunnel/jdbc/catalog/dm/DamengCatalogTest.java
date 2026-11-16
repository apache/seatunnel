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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.SQLException;

class DamengCatalogTest {

    /**
     * Metadata measured on DM V8 (0x7000c) for columns declared with length 10 or 100, on databases
     * initialized with each LENGTH_IN_CHAR / CHARSET combination. Every one of these columns
     * accepts exactly the declared number of characters, single-byte or Chinese.
     */
    @ParameterizedTest(name = "{0} {1} on LENGTH_IN_CHAR={2}, {3}")
    @CsvSource({
        // type, declared, LENGTH_IN_CHAR, charset, DATA_LENGTH, CHAR_LENGTH, SYSCOLUMNS.LENGTH$
        "NVARCHAR,  10,  0, GB18030, 20,  10,  10",
        "NVARCHAR2, 10,  0, GB18030, 20,  0,   10",
        "NVARCHAR,  100, 0, GB18030, 200, 100, 100",
        "NVARCHAR2, 100, 0, GB18030, 200, 0,   100",
        "NVARCHAR,  10,  1, GB18030, 20,  10,  10",
        "NVARCHAR2, 10,  1, GB18030, 20,  0,   10",
        "NVARCHAR,  10,  0, UTF-8,   40,  10,  10",
        "NVARCHAR2, 10,  0, UTF-8,   40,  0,   10",
        "NVARCHAR,  100, 0, UTF-8,   400, 100, 100",
        "NVARCHAR2, 100, 0, UTF-8,   400, 0,   100",
        "NVARCHAR,  10,  1, UTF-8,   40,  10,  10",
        "NVARCHAR2, 10,  1, UTF-8,   40,  0,   10",
    })
    void nationalCharLengthIsTheDeclaredLength(
            String type,
            long declared,
            int lengthInChar,
            String charset,
            long dataLength,
            long charLength,
            long sysColumnsLength) {
        Assertions.assertEquals(
                declared,
                DamengCatalog.nationalCharLength(type, dataLength, charLength, sysColumnsLength));
    }

    @Test
    void nationalCharLengthFallsBackWhenTheDeclaredLengthIsUnknown() {
        // NVARCHAR still has CHAR_LENGTH.
        Assertions.assertEquals(10, DamengCatalog.nationalCharLength("NVARCHAR", 20, 10, 0));
        // NVARCHAR2 has CHAR_LENGTH = 0: keep the byte count, which oversizes but never
        // truncates, instead of guessing a bytes-per-character ratio.
        Assertions.assertEquals(40, DamengCatalog.nationalCharLength("NVARCHAR2", 40, 0, 0));
        Assertions.assertEquals(10, DamengCatalog.nationalCharLength("nvarchar2", 20, 0, 10));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({
        // type, DATA_LENGTH, CHAR_LENGTH, SYSCOLUMNS.LENGTH$
        "CHAR,      10, 10, 10",
        "CHARACTER, 10, 10, 10",
        "VARCHAR,   10, 10, 10",
        "VARCHAR2,  10, 10, 10",
        "NCHAR,     10, 10, 10",
        // VARCHAR(10 CHAR) on GB18030: kept as DATA_LENGTH, as before this change.
        "VARCHAR,   20, 10, 10",
        "VARBINARY, 8,  0,  8",
    })
    void otherTypesKeepDataLength(
            String type, long dataLength, long charLength, long sysColumnsLength) {
        Assertions.assertEquals(
                dataLength,
                DamengCatalog.nationalCharLength(type, dataLength, charLength, sysColumnsLength));
    }

    @ParameterizedTest(name = "{0}({1}) with DATA_LENGTH {2}")
    @CsvSource({
        "NVARCHAR,  10, 20, 10",
        "NVARCHAR2, 10, 20, 0",
        "NVARCHAR,  10, 40, 10",
        "NVARCHAR2, 10, 40, 0",
    })
    void buildColumnReportsTheDeclaredNationalCharLength(
            String type, long declared, long dataLength, long charLength) throws SQLException {
        Column column = catalog().buildColumn(resultSet(type, dataLength, charLength, declared));
        Assertions.assertEquals(type + "(" + declared + ")", column.getSourceType());
        Assertions.assertEquals(declared * 4, column.getColumnLength());
    }

    @Test
    void buildColumnKeepsCharAndVarcharUnchanged() throws SQLException {
        Column varchar = catalog().buildColumn(resultSet("VARCHAR", 10, 10, 10));
        Assertions.assertEquals("VARCHAR2(10)", varchar.getSourceType());
        Assertions.assertEquals(40, varchar.getColumnLength());

        Column nchar = catalog().buildColumn(resultSet("NCHAR", 10, 10, 10));
        Assertions.assertEquals("NCHAR(10)", nchar.getSourceType());
        Assertions.assertEquals(40, nchar.getColumnLength());

        Column charColumn = catalog().buildColumn(resultSet("CHAR", 10, 10, 10));
        Assertions.assertEquals("CHAR(10)", charColumn.getSourceType());
        Assertions.assertEquals(40, charColumn.getColumnLength());
    }

    private static DamengCatalog catalog() {
        return new DamengCatalog(
                "dameng",
                "SYSDBA",
                "SYSDBA",
                JdbcUrlUtil.getUrlInfo("jdbc:dm://localhost:5236"),
                "SYSDBA",
                "dm.jdbc.driver.DmDriver");
    }

    private static ResultSet resultSet(
            String type, long dataLength, long charLength, long declaredLength)
            throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getString("COLUMN_NAME")).thenReturn("C");
        Mockito.when(resultSet.getString("DATA_TYPE")).thenReturn(type);
        Mockito.when(resultSet.getLong("DATA_LENGTH")).thenReturn(dataLength);
        Mockito.when(resultSet.getLong("CHAR_LENGTH")).thenReturn(charLength);
        Mockito.when(resultSet.getLong("DECLARED_LENGTH")).thenReturn(declaredLength);
        Mockito.when(resultSet.getString("NULLABLE")).thenReturn("Y");
        return resultSet;
    }
}
