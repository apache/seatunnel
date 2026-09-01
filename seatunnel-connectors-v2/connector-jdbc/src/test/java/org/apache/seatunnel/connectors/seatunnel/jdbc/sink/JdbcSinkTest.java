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

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class JdbcSinkTest {

    @Test
    void shouldUseTheOnlyRestoredSchemaOrTheInitialSchema() {
        TableSchema initialSchema = tableSchema("id");
        TableSchema restoredSchema = tableSchema("id", "email");

        Assertions.assertEquals(
                initialSchema,
                JdbcSink.resolveRestoredTableSchema(
                        initialSchema, Collections.singletonList(new JdbcSinkState(null))));
        Assertions.assertEquals(
                restoredSchema,
                JdbcSink.resolveRestoredTableSchema(
                        initialSchema,
                        Arrays.asList(
                                new JdbcSinkState(null, restoredSchema),
                                new JdbcSinkState(null, restoredSchema))));
    }

    @Test
    void shouldRejectDivergentRestoredSchemas() {
        TableSchema initialSchema = tableSchema("id");

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () ->
                                JdbcSink.resolveRestoredTableSchema(
                                        initialSchema,
                                        Arrays.asList(
                                                new JdbcSinkState(null, tableSchema("id", "email")),
                                                new JdbcSinkState(
                                                        null, tableSchema("id", "phone")))));

        Assertions.assertTrue(exception.getMessage().contains("divergent table schemas"));
    }

    private static TableSchema tableSchema(String... columnNames) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String columnName : columnNames) {
            builder.column(
                    PhysicalColumn.of(
                            columnName, BasicType.STRING_TYPE, (Long) null, true, null, null));
        }
        return builder.build();
    }
}
