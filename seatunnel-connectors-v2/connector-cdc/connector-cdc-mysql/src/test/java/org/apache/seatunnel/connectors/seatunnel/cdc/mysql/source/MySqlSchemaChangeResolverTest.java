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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.history.HistoryRecord;

import java.util.Collections;

/** Verifies that runtime table discovery is the only path that forwards CREATE TABLE records. */
class MySqlSchemaChangeResolverTest {

    /**
     * Ensures existing schema evolution does not begin forwarding CREATE TABLE records by default.
     */
    @Test
    void testCreateTableRequiresBinlogNewlyAddedTableOption() {
        MySqlSourceConfigFactory factory = sourceConfigFactory();
        SourceRecord createTableRecord = schemaChangeRecord("CREATE TABLE new_table (id INT)");

        Assertions.assertFalse(new MySqlSchemaChangeResolver(factory).support(createTableRecord));
        Assertions.assertTrue(
                new MySqlSchemaChangeResolver(factory, true).support(createTableRecord));
    }

    /** Builds the minimal Debezium configuration required to construct a MySQL resolver. */
    private static MySqlSourceConfigFactory sourceConfigFactory() {
        MySqlSourceConfigFactory factory = new MySqlSourceConfigFactory();
        factory.hostname("localhost");
        factory.username("test");
        factory.password("test");
        return factory;
    }

    /** Builds a captured schema record with the tableChanges payload expected by the resolver. */
    private static SourceRecord schemaChangeRecord(String ddl) {
        Schema tableChangeSchema = SchemaBuilder.struct().name("table-change").build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("io.debezium.connector.mysql.SchemaChangeValue")
                        .field(HistoryRecord.Fields.DDL_STATEMENTS, Schema.STRING_SCHEMA)
                        .field(
                                HistoryRecord.Fields.TABLE_CHANGES,
                                SchemaBuilder.array(tableChangeSchema).build())
                        .build();
        Struct value =
                new Struct(valueSchema)
                        .put(HistoryRecord.Fields.DDL_STATEMENTS, ddl)
                        .put(
                                HistoryRecord.Fields.TABLE_CHANGES,
                                Collections.singletonList(new Struct(tableChangeSchema)));
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "mysql-schema-changes",
                SchemaBuilder.struct().name("schema-change-key").build(),
                null,
                valueSchema,
                value);
    }
}
