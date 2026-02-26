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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.singlestore;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/** Unit tests for {@link SingleStoreDialect}. */
public class SingleStoreDialectTest {

    @Test
    public void testDialectName() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertEquals(DatabaseIdentifier.SINGLESTORE, dialect.dialectName());
    }

    @Test
    public void testRowConverter() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertNotNull(dialect.getRowConverter());
        Assertions.assertEquals(
                DatabaseIdentifier.MYSQL, dialect.getRowConverter().converterName());
    }

    @Test
    public void testTypeConverter() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertNotNull(dialect.getTypeConverter());
    }

    @Test
    public void testQuoteIdentifier() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertEquals("`col`", dialect.quoteIdentifier("col"));
    }

    @Test
    public void testTableIdentifier() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertEquals("`mydb`.`mytable`", dialect.tableIdentifier("mydb", "mytable"));
    }

    @Test
    public void testUpsertStatement() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        String[] fieldNames = new String[] {"id", "name", "value"};
        String[] uniqueKeyFields = new String[] {"id"};
        Optional<String> upsert =
                dialect.getUpsertStatement("db", "t", fieldNames, uniqueKeyFields);
        Assertions.assertTrue(upsert.isPresent());
        Assertions.assertTrue(upsert.get().contains("ON DUPLICATE KEY UPDATE"));
        Assertions.assertTrue(upsert.get().contains("`id`=VALUES(`id`)"));
        Assertions.assertTrue(upsert.get().contains("`name`=VALUES(`name`)"));
        Assertions.assertTrue(upsert.get().contains("`value`=VALUES(`value`)"));
    }

    @Test
    public void testHashModForField() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        String expr = dialect.hashModForField("pk", 10);
        Assertions.assertTrue(expr.contains("CRC32"));
        Assertions.assertTrue(expr.contains("`pk`"));
        Assertions.assertTrue(expr.contains("10"));
    }

    @Test
    public void testDefaultParameter() {
        SingleStoreDialect dialect = new SingleStoreDialect();
        Assertions.assertTrue(dialect.defaultParameter().containsKey("rewriteBatchedStatements"));
        Assertions.assertEquals("true", dialect.defaultParameter().get("rewriteBatchedStatements"));
    }

    @Test
    public void testSingleStoreDialectFactoryAcceptsUrl() {
        SingleStoreDialectFactory factory = new SingleStoreDialectFactory();
        Assertions.assertTrue(factory.acceptsURL("jdbc:singlestore://localhost:3306/test"));
        Assertions.assertTrue(factory.acceptsURL("jdbc:singlestore:loadbalance://host1,host2/db"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:mysql://localhost:3306/test"));
        Assertions.assertFalse(factory.acceptsURL(null));
    }

    @Test
    public void testSingleStoreDialectFactoryDialectName() {
        SingleStoreDialectFactory factory = new SingleStoreDialectFactory();
        Assertions.assertEquals(DatabaseIdentifier.SINGLESTORE, factory.dialectFactoryName());
    }

    @Test
    public void testCreateDialect() {
        SingleStoreDialectFactory factory = new SingleStoreDialectFactory();
        JdbcDialect dialect = factory.create();
        Assertions.assertInstanceOf(SingleStoreDialect.class, dialect);
        JdbcDialect dialectWithFieldIde = factory.create("", "LOWERCASE");
        Assertions.assertInstanceOf(SingleStoreDialect.class, dialectWithFieldIde);
    }
}
