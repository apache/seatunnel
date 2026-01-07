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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb.DuckDBURLParser;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DuckDBSourceAndSinkTest {

    private static final String DATABASE_NAME = "default";
    private static final String SCHEMA_NAME = "main";
    private static final String TABLE_NAME = "test_Table";
    private static final String TABLE_NAME_COPY = "test_Table_copy";
    private static final String CATALOG_NAME = "duckdb";
    private static final String DB_FILE = "DuckDBSourceAndSinkTest.db";

    private static DuckDBCatalog catalog;
    private static String jdbcUrl;

    @BeforeAll
    public static void setUp() throws Exception {
        // Delete existing database file if it exists
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        // Setup JDBC connection
        jdbcUrl = "jdbc:duckdb:" + dbFile.getAbsolutePath();
        // Create catalog instance
        JdbcUrlUtil.UrlInfo urlInfo = DuckDBURLParser.parse(jdbcUrl);
        catalog = new DuckDBCatalog(CATALOG_NAME, urlInfo, SCHEMA_NAME);
        catalog.open();
    }

    @AfterAll
    public static void tearDown() {
        // Delete database file
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        catalog.close();
    }

    private String getCreateTableTemplate() {
        return "CREATE TABLE \"%s\".\"%s\" (\n"
                + "    c_boolean BOOLEAN,\n"
                + "    c_tinyint     TINYINT,\n"
                + "    c_smallint   SMALLINT,\n"
                + "    c_integer    INTEGER,\n"
                + "    c_bigint     BIGINT,\n"
                + "    c_hugeint    HUGEINT,\n"
                + "    c_utinyint   UTINYINT,\n"
                + "    c_usmallint  USMALLINT,\n"
                + "    c_uinteger   UINTEGER,\n"
                + "    c_ubigint    UBIGINT,\n"
                + "    c_uhugeint   UHUGEINT,\n"
                + "    c_real       REAL,\n"
                + "    c_double     DOUBLE,\n"
                + "    c_decimal    DECIMAL(18, 6),\n"
                + "    c_varchar    VARCHAR,\n"
                + "    c_varchar_n  VARCHAR(100),\n"
                + "    c_text       TEXT,\n"
                + "    c_char       CHAR(10),\n"
                + "    c_bpchar     BPCHAR(10),\n"
                + "    c_blob       BLOB,\n"
                + "    c_date           DATE,\n"
                + "    c_time           TIME,\n"
                + "    c_timestamp      TIMESTAMP,\n"
                + "    c_timestamptz    TIMESTAMP WITH TIME ZONE,\n"
                + "    c_interval       INTERVAL,\n"
                + "    c_uuid       UUID\n"
                + ");\n";
    }

    private List<String> getInsertRowSql(String schemaName, String tableName) {
        List<String> insertSqls = new ArrayList<>();
        insertSqls.add(
                String.format(
                        "INSERT INTO \"%s\".\"%s\" VALUES (\n"
                                + "    TRUE,\n"
                                + "    1,\n"
                                + "    2,\n"
                                + "    3,\n"
                                + "    4,\n"
                                + "    5,\n"
                                + "    6,\n"
                                + "    7,\n"
                                + "    8,\n"
                                + "    9,\n"
                                + "    10,\n"
                                + "    1.23,\n"
                                + "    4.56,\n"
                                + "    12345.678901,\n"
                                + "    'hello',\n"
                                + "    'varchar_100',\n"
                                + "    'text_value',\n"
                                + "    'char10',\n"
                                + "    'bpchar10',\n"
                                + "    X'010203',\n"
                                + "    DATE '2024-01-01',\n"
                                + "    TIME '12:34:56',\n"
                                + "    TIMESTAMP '2024-01-01 12:34:56',\n"
                                + "    TIMESTAMPTZ '2024-01-01 12:34:56+08',\n"
                                + "    INTERVAL '1 day 2 hours 3 minutes',\n"
                                + "    '550e8400-e29b-41d4-a716-446655440000'\n"
                                + ");",
                        schemaName, tableName));
        insertSqls.add(
                String.format(
                        "INSERT INTO \"%s\".\"%s\" VALUES (\n"
                                + "    FALSE,\n"
                                + "    -1,\n"
                                + "    -2,\n"
                                + "    -3,\n"
                                + "    -4,\n"
                                + "    -5,\n"
                                + "    1,\n"
                                + "    2,\n"
                                + "    3,\n"
                                + "    4,\n"
                                + "    5,\n"
                                + "    -1.23,\n"
                                + "    -4.56,\n"
                                + "    -98765.432100,\n"
                                + "    'world',\n"
                                + "    'varchar_test',\n"
                                + "    'another_text',\n"
                                + "    'char_val',\n"
                                + "    'bpcharval',\n"
                                + "    X'0A0B0C',\n"
                                + "    DATE '2025-06-30',\n"
                                + "    TIME '23:59:59',\n"
                                + "    TIMESTAMP '2025-06-30 23:59:59',\n"
                                + "    TIMESTAMPTZ '2025-06-30 23:59:59+00',\n"
                                + "    INTERVAL '2 days 4 hours',\n"
                                + "    '123e4567-e89b-12d3-a456-426614174000'\n"
                                + ");",
                        schemaName, tableName));
        return insertSqls;
    }
}
