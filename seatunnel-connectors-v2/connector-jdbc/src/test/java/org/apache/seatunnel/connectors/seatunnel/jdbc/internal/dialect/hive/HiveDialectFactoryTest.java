package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.hive;

import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.inceptor.InceptorDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HiveDialectFactoryTest {

    @Test
    public void testWithCompatibleMode() {
        HiveDialectFactory hiveDialectFactory = new HiveDialectFactory();
        JdbcDialect inceptorDialect = hiveDialectFactory.create("inceptor", "");
        Assertions.assertTrue(inceptorDialect instanceof InceptorDialect);
        JdbcDialect hiveDialect = hiveDialectFactory.create("", "");
        Assertions.assertTrue(hiveDialect instanceof HiveDialect);
    }
}
