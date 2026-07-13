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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class DmdbDialectTest {
    @Test
    public void testIdentifierCaseSensitive() {
        DmdbDialectFactory factory = new DmdbDialectFactory();

        JdbcDialect dialect = factory.create();
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));
    }

    @Test
    void testValidateTableOptionsAcceptsSupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "MAIN");
        tableOptions.put("fillfactor", "80");
        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    void testValidateTableOptionsRejectsUnsupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("pctfree", "10");
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dialect.validateTableOptions(tableOptions));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Dameng"));
    }
}
