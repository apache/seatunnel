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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.snowflake;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the Snowflake sink fail-fast for user-defined queries: a JSON field bound through a plain
 * placeholder would be stored as an escaped string instead of a parsed VARIANT, so the sink must
 * reject such a query at initialization unless the query already applies PARSE_JSON itself.
 */
class SnowflakeDialectTest {

    private final SnowflakeDialect dialect = new SnowflakeDialect();

    private final TableSchema schemaWithJson =
            TableSchema.builder()
                    .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 20L, 0, false, null, ""))
                    .column(
                            PhysicalColumn.of(
                                    "payload", BasicType.JSON_TYPE, null, null, true, null, ""))
                    .build();

    @Test
    void userDefinedQueryWithoutParseJsonIsRejectedForJsonFields() {
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateUserDefinedQuery(
                                        "INSERT INTO t (id, payload) VALUES (?, ?)",
                                        schemaWithJson));
        Assertions.assertTrue(exception.getMessage().contains("payload"), exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("PARSE_JSON"), exception.getMessage());
    }

    @Test
    void userDefinedQueryWithParseJsonOrWithoutJsonFieldsIsAccepted() {
        Assertions.assertDoesNotThrow(
                () ->
                        dialect.validateUserDefinedQuery(
                                "INSERT INTO t (id, payload) VALUES (?, parse_json(?))",
                                schemaWithJson));
        TableSchema schemaWithoutJson =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 64L, 0, true, null, ""))
                        .build();
        Assertions.assertDoesNotThrow(
                () ->
                        dialect.validateUserDefinedQuery(
                                "INSERT INTO t (name) VALUES (?)", schemaWithoutJson));
        // Generated SQL path: no user query, nothing to validate.
        Assertions.assertDoesNotThrow(() -> dialect.validateUserDefinedQuery(null, schemaWithJson));
    }
}
