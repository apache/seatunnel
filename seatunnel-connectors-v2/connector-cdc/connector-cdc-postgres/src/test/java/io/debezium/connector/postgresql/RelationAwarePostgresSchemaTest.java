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

package io.debezium.connector.postgresql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.PostgresDefaultValueConverter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RelationAwarePostgresSchemaTest {

    private static final String DATABASE = "traffic";

    @Test
    public void testRelationNotifiesListenerWhenSchemaWasLoadedWithoutCatalog() throws Exception {
        // pgjdbc before 42.7.5 returns TABLE_CAT = NULL, so Debezium keys the table without it.
        List<Table> notified =
                applyFirstRelation(new TableId(null, "public", "users"), relationWithNewColumn());

        Assertions.assertEquals(1, notified.size());
        Assertions.assertEquals(2, notified.get(0).columns().size());
    }

    @Test
    public void testRelationNotifiesListenerWhenSchemaWasLoadedWithCatalog() throws Exception {
        // pgjdbc 42.7.5+ returns TABLE_CAT = <database>, while pgoutput RELATION ids have none.
        List<Table> notified =
                applyFirstRelation(
                        new TableId(DATABASE, "public", "users"), relationWithNewColumn());

        Assertions.assertEquals(1, notified.size());
        Assertions.assertEquals(2, notified.get(0).columns().size());
    }

    @Test
    public void testRelationForUnknownTableDoesNotNotifyListener() throws Exception {
        List<Table> notified =
                applyFirstRelation(
                        new TableId(DATABASE, "public", "other"), relationWithNewColumn());

        Assertions.assertTrue(notified.isEmpty());
    }

    @Test
    public void testRelationDoesNotMatchTableFromAnotherCatalog() throws Exception {
        List<Table> notified =
                applyFirstRelation(
                        new TableId("other_db", "public", "users"), relationWithNewColumn());

        Assertions.assertTrue(notified.isEmpty());
    }

    private static List<Table> applyFirstRelation(TableId loadedTableId, Table relation)
            throws Exception {
        RelationAwarePostgresSchema schema = newSchema();
        PostgresConnection connection = mock(PostgresConnection.class);
        doAnswer(
                        invocation -> {
                            Tables tables = invocation.getArgument(0);
                            tables.overwriteTable(table(loadedTableId, "id"));
                            return null;
                        })
                .when(connection)
                .readSchema(any(), any(), any(), any(), any(), anyBoolean());
        schema.refresh(connection, false);

        List<Table> notified = new ArrayList<>();
        schema.setRelationChangeListener(notified::add);
        schema.applySchemaChangesForTable(1, relation);
        return notified;
    }

    private static RelationAwarePostgresSchema newSchema() {
        PostgresConnectorConfig config =
                new PostgresConnectorConfig(
                        Configuration.create()
                                .with("database.server.name", "seatunnel")
                                .with("database.dbname", DATABASE)
                                .build());
        return new RelationAwarePostgresSchema(
                config,
                mock(TypeRegistry.class),
                mock(PostgresDefaultValueConverter.class),
                TopicSelector.defaultSelector(config, (id, prefix, delimiter) -> id.toString()),
                mock(PostgresValueConverter.class));
    }

    private static Table relationWithNewColumn() {
        return table(new TableId(null, "public", "users"), "id", "extra");
    }

    private static Table table(TableId tableId, String... columnNames) {
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.length; i++) {
            columns.add(
                    Column.editor()
                            .name(columnNames[i])
                            .type("int4")
                            .jdbcType(Types.INTEGER)
                            .position(i + 1)
                            .optional(i != 0)
                            .create());
        }
        return Table.editor()
                .tableId(tableId)
                .addColumns(columns)
                .setPrimaryKeyNames(columnNames[0])
                .create();
    }
}
