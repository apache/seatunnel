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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for how {@link MySqlCatalog#buildColumn(ResultSet)} derives the UNSIGNED
 * attribute from {@code COLUMN_TYPE}.
 *
 * <p>They need no MySQL server. The catalog constructor probes the server version, so the test
 * catalog overrides connection acquisition and lets the probe fail, which is the production
 * behaviour when the server cannot be reached.
 *
 * <p>{@code MySqlCatalogTest} covers this class as well, but it is {@code @Disabled} because it
 * needs a live server, so the offline cases are kept here.
 */
class MySqlCatalogUnsignedDetectionTest {

    /**
     * Regression test for <a href="https://github.com/apache/seatunnel/issues/10451">#10451</a>.
     * {@code mysql.event.sql_mode} is a SET column whose value list contains {@code
     * NO_UNSIGNED_SUBTRACTION}. The word inside that list used to be read as the UNSIGNED
     * attribute, which turned the column into the non-existent type {@code SET UNSIGNED} and failed
     * the job during catalog discovery.
     */
    @Test
    void shouldNotTreatUnsignedWordInsideSetValueListAsUnsignedAttribute() throws SQLException {
        String columnType = "set('REAL_AS_FLOAT','PIPES_AS_CONCAT','NO_UNSIGNED_SUBTRACTION')";

        Column column = buildColumn(columnType, "SET");

        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
        Assertions.assertEquals(columnType, column.getSourceType());
    }

    /** The sibling ENUM case fails in the same way, so it is covered as well. */
    @Test
    void shouldNotTreatUnsignedWordInsideEnumValueListAsUnsignedAttribute() throws SQLException {
        Column column = buildColumn("enum('unsigned','signed')", "ENUM");

        Assertions.assertEquals(BasicType.STRING_TYPE, column.getDataType());
    }

    /**
     * Numeric columns must keep reporting the attribute, otherwise this fix would be a regression.
     */
    @Test
    void shouldStillDetectUnsignedAttributeOnNumericColumn() throws SQLException {
        Assertions.assertEquals(
                BasicType.LONG_TYPE, buildColumn("int(10) unsigned", "INT").getDataType());
    }

    @Test
    void shouldNotDetectUnsignedAttributeOnSignedNumericColumn() throws SQLException {
        Assertions.assertEquals(BasicType.INT_TYPE, buildColumn("int(10)", "INT").getDataType());
    }

    private static Column buildColumn(String columnType, String dataType) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("COLUMN_NAME")).thenReturn("test");
        when(resultSet.getString("COLUMN_TYPE")).thenReturn(columnType);
        when(resultSet.getString("DATA_TYPE")).thenReturn(dataType);
        when(resultSet.getString("COLUMN_COMMENT")).thenReturn("");
        when(resultSet.getString("IS_NULLABLE")).thenReturn("YES");
        when(resultSet.getInt("NUMERIC_PRECISION")).thenReturn(0);
        when(resultSet.getInt("NUMERIC_SCALE")).thenReturn(0);
        when(resultSet.getInt("DATETIME_PRECISION")).thenReturn(0);
        when(resultSet.getLong("CHARACTER_OCTET_LENGTH")).thenReturn(0L);
        return catalogWithoutConnection().buildColumn(resultSet);
    }

    private static MySqlCatalog catalogWithoutConnection() {
        return new MySqlCatalog(
                "mysql",
                "user",
                "pwd",
                JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:3306/test"),
                null) {
            @Override
            protected Connection getConnection(String url) {
                throw new CatalogException("the unit test does not open connections");
            }
        };
    }
}
