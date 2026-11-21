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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KingbaseDialectFactoryTest {

    private final KingbaseDialectFactory factory = new KingbaseDialectFactory();

    @Test
    public void testDialectFactoryName() {
        Assertions.assertEquals("KingBase", factory.dialectFactoryName());
    }

    @Test
    public void testAcceptsURL() {
        Assertions.assertTrue(factory.acceptsURL("jdbc:kingbase8://localhost:54321/test"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:mysql://localhost:3306/test"));
    }

    @Test
    public void testCreateNoArgs() {
        JdbcDialect dialect = factory.create();
        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertEquals(
                "KingbaseJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
    }

    @Test
    public void testCreateWithCompatibleModeAndFieldIde() {
        JdbcDialect dialect = factory.create("mysql", "UPPERCASE");
        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertInstanceOf(
                org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql
                        .MysqlJdbcRowConverter.class,
                dialect.getRowConverter());
    }

    @Test
    public void testCreateWithConfigAndExplicitCompatibleMode() {
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:kingbase8://localhost:54321/test")
                        .driverName("com.kingbase8.Driver")
                        .compatibleMode("mysql")
                        .build();

        JdbcDialect dialect = factory.create("mysql", "ORIGINAL", config);
        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertInstanceOf(
                org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql
                        .MysqlJdbcRowConverter.class,
                dialect.getRowConverter());
    }

    @Test
    public void testCreateWithNullCompatibleModeAndNullConfig() {
        // When both compatibleMode and config are null, should fallback to default Kingbase
        JdbcDialect dialect = factory.create(null, "ORIGINAL", null);
        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertEquals(
                "KingbaseJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
    }

    @Test
    public void testCreatePreservesExplicitModeOverConfig() {
        // Even if config suggests something else, explicit compatibleMode should win
        JdbcConnectionConfig config =
                JdbcConnectionConfig.builder()
                        .url("jdbc:kingbase8://localhost:54321/test")
                        .driverName("com.kingbase8.Driver")
                        .compatibleMode("oracle")
                        .build();

        JdbcDialect dialect = factory.create("mysql", "ORIGINAL", config);
        Assertions.assertInstanceOf(
                org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql
                        .MysqlJdbcRowConverter.class,
                dialect.getRowConverter());
    }
}
