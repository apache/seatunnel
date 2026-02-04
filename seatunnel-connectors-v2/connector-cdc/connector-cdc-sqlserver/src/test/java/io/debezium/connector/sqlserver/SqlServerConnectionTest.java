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

package io.debezium.connector.sqlserver;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerConnectionTest {

    @Test
    void testGetTableSchemaFromTableFiltersOutWildcardTables() throws Exception {
        String databaseName = "test_db";
        TableId tableId = TableId.parse(databaseName + ".dbo.user_info");

        SqlServerChangeTable changeTable = mock(SqlServerChangeTable.class);
        when(changeTable.getSourceTableId()).thenReturn(tableId);
        when(changeTable.getCapturedColumns()).thenReturn(Collections.singletonList("id"));

        ResultSet columnsRs = mock(ResultSet.class);
        when(columnsRs.next()).thenReturn(true, true, false);
        when(columnsRs.getString("TABLE_NAME")).thenReturn("user_info", "userAinfo");
        when(columnsRs.getString("TABLE_SCHEM")).thenReturn("dbo", "dbo");
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id", "bad");

        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(metadata.getColumns(eq(databaseName), eq("dbo"), eq("user_info"), isNull()))
                .thenReturn(columnsRs);

        Connection jdbcConnection = mock(Connection.class);
        when(jdbcConnection.getMetaData()).thenReturn(metadata);

        TestSqlServerConnection connection = new TestSqlServerConnection(jdbcConnection);
        Table table = connection.getTableSchemaFromTable(databaseName, changeTable);

        Assertions.assertEquals(1, table.columns().size());
        Assertions.assertEquals("id", table.columns().get(0).name());
    }

    @Test
    void testGetTableSchemaFromTableCaseSensitiveRequiresExactMatch() throws Exception {
        String databaseName = "test_db";
        TableId tableId = TableId.parse(databaseName + ".dbo.UserInfo");

        SqlServerChangeTable changeTable = mock(SqlServerChangeTable.class);
        when(changeTable.getSourceTableId()).thenReturn(tableId);
        when(changeTable.getCapturedColumns()).thenReturn(Collections.singletonList("id"));

        ResultSet columnsRs = mock(ResultSet.class);
        when(columnsRs.next()).thenReturn(true, false);
        when(columnsRs.getString("TABLE_NAME")).thenReturn("userinfo");
        when(columnsRs.getString("TABLE_SCHEM")).thenReturn("dbo");
        when(columnsRs.getString("COLUMN_NAME")).thenReturn("id");

        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(metadata.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(metadata.getColumns(eq(databaseName), eq("dbo"), eq("UserInfo"), isNull()))
                .thenReturn(columnsRs);

        Connection jdbcConnection = mock(Connection.class);
        when(jdbcConnection.getMetaData()).thenReturn(metadata);

        TestSqlServerConnection connection = new TestSqlServerConnection(jdbcConnection);
        Table table = connection.getTableSchemaFromTable(databaseName, changeTable);

        Assertions.assertTrue(table.columns().isEmpty());
    }

    private static final class TestSqlServerConnection extends SqlServerConnection {
        private final Connection jdbcConnection;

        private TestSqlServerConnection(Connection jdbcConnection) {
            super(
                    JdbcConfiguration.adapt(Configuration.create().build()),
                    SourceTimestampMode.COMMIT,
                    mock(SqlServerValueConverters.class),
                    SqlServerConnectionTest.class::getClassLoader,
                    Collections.emptySet(),
                    false);
            this.jdbcConnection = jdbcConnection;
        }

        @Override
        public synchronized Connection connection(boolean executeOnConnect) throws SQLException {
            return jdbcConnection;
        }

        @Override
        protected Optional<ColumnEditor> readTableColumn(
                ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
                throws SQLException {
            String columnName = columnMetadata.getString("COLUMN_NAME");
            ColumnEditor editor =
                    Column.editor().name(columnName).type("INT").jdbcType(Types.INTEGER);
            return Optional.of(editor);
        }

        @Override
        protected List<String> readPrimaryKeyOrUniqueIndexNames(
                DatabaseMetaData metadata, TableId tableId) throws SQLException {
            return Collections.emptyList();
        }
    }
}
