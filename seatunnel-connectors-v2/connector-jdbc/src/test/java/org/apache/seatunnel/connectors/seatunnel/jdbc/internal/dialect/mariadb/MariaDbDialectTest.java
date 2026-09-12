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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mariadb;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MariaDbDialectTest {

    @Test
    public void testDialectBasicMethods() {
        MariaDbDialect dialect = new MariaDbDialect();
        Assertions.assertEquals(DatabaseIdentifier.MARIADB, dialect.dialectName());
        Assertions.assertEquals("`col`", dialect.quoteIdentifier("col"));
        Assertions.assertEquals("`db`", dialect.quoteDatabaseIdentifier("db"));
        Assertions.assertEquals("`db`.`tbl`", dialect.tableIdentifier(TablePath.of("db", "tbl")));
        Assertions.assertEquals("tbl", dialect.extractTableName(TablePath.of("db", "tbl")));
        Assertions.assertEquals(TablePath.of("db.tbl", false), dialect.parse("db.tbl"));
        Assertions.assertTrue(dialect.supportStringRangeSplit());
        Assertions.assertEquals("ABS(CRC32(`id`) % 10)", dialect.hashModForField("id", 10));
    }

    @Test
    public void testGetUpsertStatement() {
        MariaDbDialect dialect = new MariaDbDialect();
        Optional<String> upsertSQL =
                dialect.getUpsertStatement(
                        "test_db",
                        "test_table",
                        new String[] {"id", "name", "age"},
                        new String[] {"id"});
        Assertions.assertTrue(upsertSQL.isPresent());
        Assertions.assertEquals(
                "INSERT INTO `test_db`.`test_table` (`id`, `name`, `age`) "
                        + "VALUES (:id, :name, :age) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`), `name`=VALUES(`name`), `age`=VALUES(`age`)",
                upsertSQL.get());
    }

    @Test
    public void testValidateTableOptions() {
        MariaDbDialect dialect = new MariaDbDialect();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("engine", "InnoDB");
        tableOptions.put("charset", "utf8mb4");
        tableOptions.put("collate", "utf8mb4_unicode_ci");

        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));

        Map<String, String> invalidOptions = new HashMap<>();
        invalidOptions.put("unsupported_option", "value");
        Assertions.assertThrows(
                JdbcConnectorException.class, () -> dialect.validateTableOptions(invalidOptions));
    }

    @Test
    public void testSupportDefaultValueAndNeedsQuotes() {
        MariaDbDialect dialect = new MariaDbDialect();

        BasicTypeDefine varcharColumn =
                BasicTypeDefine.builder()
                        .name("c1")
                        .columnType("VARCHAR(255)")
                        .dataType("VARCHAR")
                        .build();
        Assertions.assertTrue(dialect.supportDefaultValue(varcharColumn));
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(varcharColumn));

        BasicTypeDefine intColumn =
                BasicTypeDefine.builder().name("c2").columnType("INT").dataType("INT").build();
        Assertions.assertTrue(dialect.supportDefaultValue(intColumn));
        Assertions.assertFalse(dialect.needsQuotesWithDefaultValue(intColumn));

        BasicTypeDefine blobColumn =
                BasicTypeDefine.builder().name("c3").columnType("BLOB").dataType("BLOB").build();
        Assertions.assertFalse(dialect.supportDefaultValue(blobColumn));
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(blobColumn));
    }
}
