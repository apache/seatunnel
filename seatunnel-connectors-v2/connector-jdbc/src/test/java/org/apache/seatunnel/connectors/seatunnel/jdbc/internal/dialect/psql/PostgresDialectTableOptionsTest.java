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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PostgresDialectTableOptionsTest {

    @Test
    public void testValidateTableOptions() {
        PostgresDialect dialect = new PostgresDialect();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "pg_default");
        tableOptions.put("fillfactor", "70");

        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    public void testValidateTableOptionsWithUnknownKey() {
        PostgresDialect dialect = new PostgresDialect();

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("engine", "InnoDB")));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Postgres"));
    }
}
