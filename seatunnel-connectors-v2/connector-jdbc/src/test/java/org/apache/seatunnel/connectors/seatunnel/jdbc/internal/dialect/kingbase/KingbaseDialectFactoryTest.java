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
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Covers the mode-selection contract of {@link KingbaseDialectFactory}: the mode comes only from
 * the explicit {@code compatible_mode} value, and an absent mode deterministically selects the
 * native Kingbase behavior.
 *
 * <p>Every case passes a connection config whose URL points at an unroutable Documentation address.
 * Because this factory performs no live probe, none of them can block on the network; a
 * reintroduced probe would make these tests slow or hang rather than silently pass.
 */
public class KingbaseDialectFactoryTest {

    private static final String UNROUTABLE_URL = "jdbc:kingbase8://192.0.2.1:54321/test";

    private final KingbaseDialectFactory factory = new KingbaseDialectFactory();

    private static JdbcConnectionConfig unroutableConfig(String compatibleMode) {
        return JdbcConnectionConfig.builder()
                .url(UNROUTABLE_URL)
                .driverName("com.kingbase8.Driver")
                .compatibleMode(compatibleMode)
                .build();
    }

    @Test
    public void testDialectFactoryName() {
        Assertions.assertEquals("KingBase", factory.dialectFactoryName());
    }

    @Test
    public void testAcceptsURL() {
        Assertions.assertTrue(factory.acceptsURL(UNROUTABLE_URL));
        Assertions.assertFalse(factory.acceptsURL("jdbc:mysql://localhost:3306/test"));
        Assertions.assertFalse(factory.acceptsURL("jdbc:postgresql://localhost:5432/test"));
    }

    @Test
    public void testCreateNoArgs() {
        JdbcDialect dialect = factory.create();
        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(KingbaseTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testExplicitMysqlModeSelectsMysqlBehavior() {
        JdbcDialect dialect = factory.create("mysql", FieldIdeEnum.ORIGINAL.getValue());

        Assertions.assertInstanceOf(KingbaseDialect.class, dialect);
        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(MySqlTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testExplicitModeIsCaseInsensitiveAndTrimmed() {
        for (String mode : new String[] {"MYSQL", "MySql", "  mysql  "}) {
            JdbcDialect dialect = factory.create(mode, FieldIdeEnum.ORIGINAL.getValue());
            Assertions.assertInstanceOf(
                    MysqlJdbcRowConverter.class, dialect.getRowConverter(), "mode=" + mode);
        }
    }

    @Test
    public void testNonMysqlExplicitModesKeepNativeBehavior() {
        for (String mode : new String[] {"pg", "oracle", "PG", "sqlserver"}) {
            JdbcDialect dialect = factory.create(mode, FieldIdeEnum.ORIGINAL.getValue());
            Assertions.assertInstanceOf(
                    KingbaseJdbcRowConverter.class, dialect.getRowConverter(), "mode=" + mode);
        }
    }

    /** Absent mode plus a real config must not contact the database and must stay native. */
    @Test
    public void testAbsentModeWithConnectionConfigStaysNativeWithoutProbing() {
        JdbcDialect dialect =
                factory.create(null, FieldIdeEnum.ORIGINAL.getValue(), unroutableConfig(null));

        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(KingbaseTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    /** A mode carried only by the config is not used: the explicit argument is the sole source. */
    @Test
    public void testConfigCompatibleModeIsNotConsulted() {
        JdbcDialect dialect =
                factory.create("mysql", FieldIdeEnum.ORIGINAL.getValue(), unroutableConfig("pg"));

        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect.getRowConverter());
    }

    @Test
    public void testExplicitModeIsPreservedWhenConfigDisagrees() {
        JdbcDialect dialect =
                factory.create(
                        "oracle", FieldIdeEnum.ORIGINAL.getValue(), unroutableConfig("mysql"));

        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect.getRowConverter());
    }

    @Test
    public void testNullConfigAndNullModeFallsBackToNative() {
        JdbcDialect dialect = factory.create(null, FieldIdeEnum.ORIGINAL.getValue(), null);

        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect.getRowConverter());
    }

    @Test
    public void testFieldIdeIsPropagatedInMysqlMode() {
        KingbaseDialect dialect =
                (KingbaseDialect)
                        factory.create(
                                "mysql", FieldIdeEnum.UPPERCASE.getValue(), unroutableConfig(null));

        // MySQL-compatible quoting still honors field_ide instead of silently using ORIGINAL.
        Assertions.assertEquals(FieldIdeEnum.UPPERCASE.getValue(), dialect.fieldIde);

        Optional<String> upsert =
                dialect.getUpsertStatement(
                        "db", "tbl", new String[] {"id", "name"}, new String[] {"id"});
        Assertions.assertTrue(upsert.isPresent());
        Assertions.assertTrue(
                upsert.get().contains("ON DUPLICATE KEY UPDATE"),
                "expected MySQL-style upsert, got: " + upsert.get());
        // The delegated MysqlDialect must receive the same field_ide value.
        String expected =
                new MysqlDialect(FieldIdeEnum.UPPERCASE.getValue())
                        .getUpsertStatement(
                                "db", "tbl", new String[] {"id", "name"}, new String[] {"id"})
                        .orElseThrow(IllegalStateException::new);
        Assertions.assertEquals(expected, upsert.get());
    }

    @Test
    public void testNativeModeUsesOnConflictUpsert() {
        KingbaseDialect dialect =
                (KingbaseDialect) factory.create("pg", FieldIdeEnum.ORIGINAL.getValue());

        String upsert =
                dialect.getUpsertStatement(
                                "db", "tbl", new String[] {"id", "name"}, new String[] {"id"})
                        .orElseThrow(IllegalStateException::new);

        Assertions.assertTrue(upsert.contains("ON CONFLICT"), upsert);
        Assertions.assertFalse(upsert.contains("ON DUPLICATE KEY UPDATE"), upsert);
    }
}
