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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.RestoreTableSchemaEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

/**
 * The primary key index drives multi-table hash routing. It must follow the key column when a
 * schema change moves that column's position.
 */
class JdbcSinkWriterPrimaryKeyIndexTest {

    private static final TablePath TABLE_PATH = TablePath.of("defaultdb.public.target");
    private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("jdbc", TABLE_PATH);

    @Test
    void addColumnFirstShiftsPrimaryKeyIndex() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), 0);
        Assertions.assertEquals(Optional.of(0), writer.primaryKey());

        writer.applySchemaChange(
                AlterTableAddColumnEvent.addFirst(TABLE_IDENTIFIER, column("extra")));

        Assertions.assertEquals(Optional.of(1), writer.primaryKey());
    }

    @Test
    void addColumnAfterKeyKeepsPrimaryKeyIndex() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), 0);

        writer.applySchemaChange(
                AlterTableAddColumnEvent.addAfter(TABLE_IDENTIFIER, column("extra"), "id"));

        Assertions.assertEquals(Optional.of(0), writer.primaryKey());
    }

    @Test
    void dropColumnBeforeKeyShiftsPrimaryKeyIndex() throws Exception {
        JdbcSinkWriter writer = writer(schema("extra", "id", "name"), 1);
        Assertions.assertEquals(Optional.of(1), writer.primaryKey());

        writer.applySchemaChange(new AlterTableDropColumnEvent(TABLE_IDENTIFIER, "extra"));

        Assertions.assertEquals(Optional.of(0), writer.primaryKey());
    }

    @Test
    void renamedKeyColumnKeepsBeingFollowed() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), 0);

        writer.applySchemaChange(
                AlterTableChangeColumnEvent.change(TABLE_IDENTIFIER, "id", column("uid")));
        Assertions.assertEquals(Optional.of(0), writer.primaryKey());

        writer.applySchemaChange(
                AlterTableAddColumnEvent.addFirst(TABLE_IDENTIFIER, column("extra")));
        Assertions.assertEquals(Optional.of(1), writer.primaryKey());

        writer.applySchemaChange(new AlterTableDropColumnEvent(TABLE_IDENTIFIER, "uid"));
        Assertions.assertFalse(writer.primaryKey().isPresent());
    }

    @Test
    void restoredSchemaWithRenamedKeyColumnIsFollowed() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), 0);
        TableSchema restored = schemaWithKey("uid", "uid", "extra", "name");

        writer.applySchemaChange(
                new RestoreTableSchemaEvent(
                        CatalogTable.of(
                                TABLE_IDENTIFIER,
                                restored,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null,
                                "jdbc")));

        Assertions.assertEquals(Optional.of(0), writer.primaryKey());

        writer.applySchemaChange(
                AlterTableAddColumnEvent.addFirst(TABLE_IDENTIFIER, column("first")));
        Assertions.assertEquals(Optional.of(1), writer.primaryKey());
    }

    @Test
    void dropKeyColumnClearsPrimaryKeyIndex() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), 0);

        writer.applySchemaChange(new AlterTableDropColumnEvent(TABLE_IDENTIFIER, "id"));

        Assertions.assertFalse(writer.primaryKey().isPresent());
    }

    @Test
    void writerWithoutPrimaryKeyStaysUnkeyed() throws Exception {
        JdbcSinkWriter writer = writer(schema("id", "name"), null);

        writer.applySchemaChange(
                AlterTableAddColumnEvent.addFirst(TABLE_IDENTIFIER, column("extra")));

        Assertions.assertFalse(writer.primaryKey().isPresent());
    }

    private static JdbcSinkWriter writer(TableSchema schema, Integer primaryKeyIndex)
            throws Exception {
        JdbcConnectionConfig connectionConfig =
                JdbcConnectionConfig.builder()
                        .url("jdbc:mysql://localhost:3306/defaultdb")
                        .driverName("com.mysql.cj.jdbc.Driver")
                        .properties(Collections.emptyMap())
                        .build();
        JdbcSinkConfig sinkConfig =
                JdbcSinkConfig.builder()
                        .jdbcConnectionConfig(connectionConfig)
                        .database("defaultdb")
                        .table("public.target")
                        .build();
        JdbcSinkWriter writer =
                spy(
                        new JdbcSinkWriter(
                                TABLE_PATH,
                                new MysqlDialect(),
                                sinkConfig,
                                schema,
                                null,
                                primaryKeyIndex));
        doNothing().when(writer).reOpenOutputFormat(any());
        return writer;
    }

    private static PhysicalColumn column(String name) {
        return PhysicalColumn.builder().name(name).dataType(BasicType.STRING_TYPE).build();
    }

    private static TableSchema schema(String... names) {
        return schemaWithKey("id", names);
    }

    private static TableSchema schemaWithKey(String keyColumn, String... names) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String name : names) {
            builder.column(column(name));
        }
        builder.primaryKey(PrimaryKey.of("pk", Collections.singletonList(keyColumn)));
        return builder.build();
    }
}
