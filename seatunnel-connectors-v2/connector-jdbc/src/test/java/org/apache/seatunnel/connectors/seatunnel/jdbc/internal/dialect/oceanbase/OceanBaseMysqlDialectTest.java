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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OceanBaseMysqlDialectTest {

    @Test
    public void testValidateTableOptions() {
        OceanBaseMysqlDialect dialect = new OceanBaseMysqlDialect();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");

        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    public void testValidateTableOptionsWithUnknownKey() {
        OceanBaseMysqlDialect dialect = new OceanBaseMysqlDialect();

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("bucket_num", "3")));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("OceanBase"));
    }
}
