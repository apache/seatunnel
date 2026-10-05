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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AbstractJdbcCatalogTest {

    @Test
    void shouldCloseRemainingConnectionsAndKeepSuppressedExceptionsWhenCloseFails()
            throws SQLException {
        TestJdbcCatalog catalog = new TestJdbcCatalog();
        Connection failedConnection = mock(Connection.class);
        Connection remainingConnection = mock(Connection.class);
        doThrow(new SQLException("first close failed")).when(failedConnection).close();
        doThrow(new SQLException("second close failed")).when(remainingConnection).close();
        catalog.addConnection("jdbc:test:first", failedConnection);
        catalog.addConnection("jdbc:test:second", remainingConnection);

        CatalogException exception =
                Assertions.assertThrows(CatalogException.class, catalog::close);

        verify(failedConnection).close();
        verify(remainingConnection).close();
        Assertions.assertEquals(1, exception.getSuppressed().length);
    }

    private static class TestJdbcCatalog extends AbstractJdbcCatalog {

        private TestJdbcCatalog() {
            super(
                    "test",
                    "user",
                    "password",
                    new JdbcUrlUtil.UrlInfo(
                            "jdbc:test:default", "jdbc:test:", "localhost", 0, "default", null),
                    null,
                    null);
        }

        private void addConnection(String url, Connection connection) {
            connectionMap.put(url, connection);
        }
    }
}
