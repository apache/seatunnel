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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DmdbDialectFactory}. Tests cover factory name, URL acceptance, and dialect
 * creation.
 */
public class DmdbDialectFactoryTest {

    @Test
    public void testDialectFactoryName() {
        DmdbDialectFactory factory = new DmdbDialectFactory();
        Assertions.assertEquals("Dameng", factory.dialectFactoryName());
    }

    @Test
    public void testAcceptsURL() {
        DmdbDialectFactory factory = new DmdbDialectFactory();

        Assertions.assertTrue(factory.acceptsURL("jdbc:dm://localhost:5236"));
        Assertions.assertTrue(factory.acceptsURL("jdbc:dm:localhost:5236"));
        Assertions.assertTrue(factory.acceptsURL("jdbc:dm://localhost:5236?param=value"));

        Assertions.assertFalse(factory.acceptsURL("jdbc:mysql://localhost:3306"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:postgresql://localhost:5432"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:oracle:thin:@localhost:1521"));

        // Edge cases
        Assertions.assertFalse(factory.acceptsURL(""));
        Assertions.assertFalse(factory.acceptsURL("jdbc:dm"));
    }

    @Test
    public void testCreateDefault() {
        DmdbDialectFactory factory = new DmdbDialectFactory();
        JdbcDialect dialect = factory.create();

        Assertions.assertNotNull(dialect);
        Assertions.assertInstanceOf(DmdbDialect.class, dialect);
        Assertions.assertEquals("Dameng", dialect.dialectName());
    }

    @Test
    public void testCreateWithFieldIde() {
        DmdbDialectFactory factory = new DmdbDialectFactory();

        JdbcDialect dialectOriginal = factory.create(null, FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertInstanceOf(DmdbDialect.class, dialectOriginal);
        Assertions.assertEquals("\"test\"", dialectOriginal.quoteIdentifier("test"));

        JdbcDialect dialectUpper = factory.create(null, FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"TEST\"", dialectUpper.quoteIdentifier("test"));

        JdbcDialect dialectLower = factory.create(null, FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"test\"", dialectLower.quoteIdentifier("TEST"));
    }
}
