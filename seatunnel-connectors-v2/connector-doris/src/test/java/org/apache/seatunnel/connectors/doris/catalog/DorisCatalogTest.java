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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DorisCatalogTest {

    private static final TablePath TABLE_PATH = TablePath.of("db", "t");

    private DorisCatalog catalog;
    private Connection connection;
    private Statement statement;

    @BeforeEach
    void setUp() throws Exception {
        catalog = new DorisCatalog("test", "127.0.0.1", 9030, "user", "password");
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        // Inject the mocked JDBC connection into the catalog using reflection
        Field connField = DorisCatalog.class.getDeclaredField("conn");
        connField.setAccessible(true);
        connField.set(catalog, connection);
    }

    private void stubTableExists(boolean exists) throws SQLException {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(exists);
    }

    @Test
    void truncatesExistingTableWhetherIgnoreFlagIsEnabledOrNot() throws SQLException {
        stubTableExists(true);

        catalog.truncateTable(TABLE_PATH, false);
        catalog.truncateTable(TABLE_PATH, true);

        verify(statement, times(2)).execute("TRUNCATE TABLE db.t");
    }

    @Test
    void throwsWhenTableIsMissingAndIgnoreFlagIsDisabled() throws SQLException {
        stubTableExists(false);

        Assertions.assertThrows(
                TableNotExistException.class, () -> catalog.truncateTable(TABLE_PATH, false));

        verify(statement, never()).execute(anyString());
    }

    @Test
    void skipsWhenTableIsMissingAndIgnoreFlagIsEnabled() throws SQLException {
        stubTableExists(false);

        catalog.truncateTable(TABLE_PATH, true);

        verify(statement, never()).execute(anyString());
    }

    @Test
    void truncatesRequestedPartitions() throws SQLException {
        stubTableExists(true);

        catalog.truncateTable(TABLE_PATH, true, Arrays.asList("p1", "p2"));

        verify(statement).execute("TRUNCATE TABLE db.t PARTITION (`p1`, `p2`)");
    }
}
