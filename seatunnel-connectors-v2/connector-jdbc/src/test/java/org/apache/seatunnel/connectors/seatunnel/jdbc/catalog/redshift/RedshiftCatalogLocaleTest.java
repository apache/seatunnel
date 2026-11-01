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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that catalog schema discovery does not depend on the JVM default locale.
 *
 * <p>{@link RedshiftCatalog#buildColumn(ResultSet)} upper-cases the driver column type before it is
 * matched against the dialect constants. Under a Turkish locale a bare {@code toUpperCase()} turns
 * {@code integer} into {@code İNTEGER} ({@code U+0130}) and the column is no longer recognized, so
 * this test drives the real discovery method with a mocked result set instead of calling the type
 * converter directly. All three locale categories are restored afterwards.
 */
@ResourceLock("java.util.Locale")
public class RedshiftCatalogLocaleTest {

    @Test
    public void testBuildColumnUnderTurkishLocale() throws SQLException {
        Locale base = Locale.getDefault();
        Locale display = Locale.getDefault(Locale.Category.DISPLAY);
        Locale format = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            Assertions.assertEquals(
                    BasicType.INT_TYPE, catalog().buildColumn(column("integer")).getDataType());
            Assertions.assertEquals(
                    BasicType.LONG_TYPE, catalog().buildColumn(column("bigint")).getDataType());
        } finally {
            Locale.setDefault(base);
            Locale.setDefault(Locale.Category.DISPLAY, display);
            Locale.setDefault(Locale.Category.FORMAT, format);
        }
    }

    private static RedshiftCatalog catalog() {
        return new RedshiftCatalog(
                "redshift",
                "user",
                "pwd",
                JdbcUrlUtil.getUrlInfo("jdbc:redshift://127.0.0.1:5439/test"),
                "public",
                null);
    }

    private static ResultSet column(String dataType) throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("COLUMN_NAME")).thenReturn("test");
        when(resultSet.getString("DATA_TYPE")).thenReturn(dataType);
        when(resultSet.getLong("NUMERIC_PRECISION")).thenReturn(10L);
        when(resultSet.getInt("NUMERIC_SCALE")).thenReturn(0);
        when(resultSet.getLong("CHARACTER_MAXIMUM_LENGTH")).thenReturn(0L);
        when(resultSet.getObject("COLUMN_DEFAULT")).thenReturn(null);
        when(resultSet.getString("IS_NULLABLE")).thenReturn("YES");
        return resultSet;
    }
}
