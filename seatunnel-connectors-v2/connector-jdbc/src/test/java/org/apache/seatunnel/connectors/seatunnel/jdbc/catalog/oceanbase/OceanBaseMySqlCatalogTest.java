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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Offline tests for {@link OceanBaseMySqlCatalog#buildColumn(ResultSet)}. */
class OceanBaseMySqlCatalogTest {

    /**
     * {@code DATA_TYPE} comes in lower case, as in MySQL. Under {@code tr-TR} and {@code az-AZ},
     * upper casing it with the default locale gave {@code İNT}, which matched no type.
     */
    @Test
    void shouldReadLowerCaseDataTypeUnderAnyDefaultLocale() throws SQLException {
        Locale[] saved = {
            Locale.getDefault(),
            Locale.getDefault(Locale.Category.FORMAT),
            Locale.getDefault(Locale.Category.DISPLAY)
        };
        try {
            for (Locale locale : new Locale[] {new Locale("tr", "TR"), new Locale("az", "AZ")}) {
                // Upper casing "i" gives a dotted capital I under these locales.
                Locale.setDefault(locale);
                Assertions.assertEquals(
                        BasicType.INT_TYPE,
                        buildColumn("int(11)", "int").getDataType(),
                        locale.toString());
                Assertions.assertEquals(
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        buildColumn("datetime", "datetime").getDataType(),
                        locale.toString());
            }
        } finally {
            // The base locale first, because setting it overwrites both category defaults.
            Locale.setDefault(saved[0]);
            Locale.setDefault(Locale.Category.FORMAT, saved[1]);
            Locale.setDefault(Locale.Category.DISPLAY, saved[2]);
        }
    }

    private static Column buildColumn(String columnType, String dataType) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("COLUMN_NAME")).thenReturn("test");
        when(resultSet.getString("COLUMN_TYPE")).thenReturn(columnType);
        when(resultSet.getString("DATA_TYPE")).thenReturn(dataType);
        when(resultSet.getString("IS_NULLABLE")).thenReturn("YES");
        // The constructor does not connect, so no server is needed.
        return new OceanBaseMySqlCatalog(
                        "oceanbase",
                        "user",
                        "pwd",
                        JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:2881/test"),
                        "com.mysql.cj.jdbc.Driver")
                .buildColumn(resultSet);
    }
}
