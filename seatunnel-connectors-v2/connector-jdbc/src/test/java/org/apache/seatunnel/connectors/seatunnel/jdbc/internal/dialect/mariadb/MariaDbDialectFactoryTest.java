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

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MariaDbDialectFactoryTest {

    @Test
    public void testFactoryAcceptsUrl() {
        MariaDbDialectFactory factory = new MariaDbDialectFactory();
        Assertions.assertEquals(DatabaseIdentifier.MARIADB, factory.dialectFactoryName());
        Assertions.assertTrue(factory.acceptsURL("jdbc:mariadb://localhost:3306/test"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:mysql://localhost:3306/test"));
    }

    @Test
    public void testDialectLoaderLoadByUrl() {
        JdbcDialect dialect =
                JdbcDialectLoader.load("jdbc:mariadb://localhost:3306/test", null, null);
        Assertions.assertNotNull(dialect);
        Assertions.assertTrue(dialect instanceof MariaDbDialect);
        Assertions.assertEquals(DatabaseIdentifier.MARIADB, dialect.dialectName());
    }

    @Test
    public void testDialectLoaderLoadByDialectName() {
        JdbcDialect dialect =
                JdbcDialectLoader.load(
                        "jdbc:other://localhost:3306/test", DatabaseIdentifier.MARIADB, null);
        Assertions.assertNotNull(dialect);
        Assertions.assertTrue(dialect instanceof MariaDbDialect);
        Assertions.assertEquals(DatabaseIdentifier.MARIADB, dialect.dialectName());
    }
}
