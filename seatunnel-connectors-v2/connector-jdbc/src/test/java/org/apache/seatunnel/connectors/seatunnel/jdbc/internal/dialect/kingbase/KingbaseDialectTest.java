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

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MysqlJdbcRowConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class KingbaseDialectTest {

    @Test
    public void testDialectName() {
        KingbaseDialect dialect = new KingbaseDialect();
        Assertions.assertEquals(DatabaseIdentifier.KINGBASE, dialect.dialectName());
    }

    @Test
    public void testDefaultConstructor() {
        KingbaseDialect dialect = new KingbaseDialect();
        Assertions.assertNotNull(dialect.getRowConverter());
        Assertions.assertEquals(
                "KingbaseJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
        Assertions.assertEquals(
                "KingbaseTypeMapper",
                dialect.getJdbcDialectTypeMapper().getClass().getSimpleName());
    }

    @Test
    public void testFieldIdeConstructor() {
        KingbaseDialect dialect = new KingbaseDialect(FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"TABLE_NAME\"", dialect.quoteIdentifier("table_name"));
    }

    @Test
    public void testMySQLCompatibleMode() {
        KingbaseDialect dialect = new KingbaseDialect("mysql", FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(MySqlTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testMySQLCompatibleModeCaseInsensitive() {
        KingbaseDialect dialect = new KingbaseDialect("MySQL", FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(MySqlTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testNullCompatibleModeDefaultsToKingbase() {
        KingbaseDialect dialect = new KingbaseDialect(null, FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect.getRowConverter());
        Assertions.assertInstanceOf(KingbaseTypeMapper.class, dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testMySQLCompatibleUpsertStatement() {
        KingbaseDialect dialect = new KingbaseDialect("mysql", FieldIdeEnum.ORIGINAL.getValue());
        String[] fieldNames = {"id", "name"};
        String[] pkNames = {"id"};

        Optional<String> upsert = dialect.getUpsertStatement("db", "table", fieldNames, pkNames);
        Assertions.assertTrue(upsert.isPresent());
        String sql = upsert.get();
        // MySQL upsert uses ON DUPLICATE KEY UPDATE
        Assertions.assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
    }

    @Test
    public void testKingbaseUpsertStatement() {
        KingbaseDialect dialect = new KingbaseDialect();
        String[] fieldNames = {"id", "name"};
        String[] pkNames = {"id"};

        Optional<String> upsert = dialect.getUpsertStatement("db", "table", fieldNames, pkNames);
        Assertions.assertTrue(upsert.isPresent());
        String sql = upsert.get();
        // Kingbase upsert uses ON CONFLICT ... DO UPDATE SET
        Assertions.assertTrue(sql.contains("ON CONFLICT"));
        Assertions.assertTrue(sql.contains("DO UPDATE SET"));
        Assertions.assertTrue(sql.contains("EXCLUDED"));
    }

    @Test
    public void testQuoteIdentifier() {
        KingbaseDialect dialect = new KingbaseDialect();
        Assertions.assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
        Assertions.assertEquals("\"schema\".\"table\"", dialect.quoteIdentifier("schema.table"));
    }

    @Test
    public void testQuoteIdentifierWithFieldIde() {
        KingbaseDialect dialect = new KingbaseDialect("mysql", FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"COLUMN_NAME\"", dialect.quoteIdentifier("column_name"));
    }

    @Test
    public void testQuoteDatabaseIdentifier() {
        KingbaseDialect dialect = new KingbaseDialect();
        Assertions.assertEquals("\"mydb\"", dialect.quoteDatabaseIdentifier("mydb"));
    }

    @Test
    public void testTableIdentifier() {
        KingbaseDialect dialect = new KingbaseDialect();
        Assertions.assertEquals("\"db\".\"table\"", dialect.tableIdentifier("db", "table"));
    }

    @Test
    public void testParseTablePath() {
        KingbaseDialect dialect = new KingbaseDialect();
        TablePath path = dialect.parse("database.schema.table");
        Assertions.assertEquals("database", path.getDatabaseName());
        Assertions.assertEquals("schema", path.getSchemaName());
        Assertions.assertEquals("table", path.getTableName());
    }

    @Test
    public void testImmutability() {
        KingbaseDialect dialect1 = new KingbaseDialect("mysql", FieldIdeEnum.UPPERCASE.getValue());
        // Row converter should be fixed after construction
        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect1.getRowConverter());

        // Even if we create another dialect with different config, first one is unchanged
        KingbaseDialect dialect2 = new KingbaseDialect("oracle", FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertInstanceOf(MysqlJdbcRowConverter.class, dialect1.getRowConverter());
        Assertions.assertInstanceOf(KingbaseJdbcRowConverter.class, dialect2.getRowConverter());
    }
}
