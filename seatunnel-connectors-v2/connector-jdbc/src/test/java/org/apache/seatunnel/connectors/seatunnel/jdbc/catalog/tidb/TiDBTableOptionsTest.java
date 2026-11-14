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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** TiDB catalog inherits MySQL auto-create DDL; table_options use the same keys. */
public class TiDBTableOptionsTest {

    @Test
    public void testBuildCreateTableSqlWithTableOptions() {
        TablePath tablePath = TablePath.of("test_db", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0, false, null, "id"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .build();
        Map<String, String> options = new HashMap<>();
        options.put(MySqlCatalog.TABLE_OPTION_ENGINE, "InnoDB");
        options.put(MySqlCatalog.TABLE_OPTION_CHARSET, "utf8mb4");
        options.put(MySqlCatalog.TABLE_OPTION_COLLATE, "utf8mb4_unicode_ci");
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", "test_db", "test_table"),
                        tableSchema,
                        options,
                        Collections.emptyList(),
                        "tidb table with options");

        String createTableSql =
                MysqlCreateTableSqlBuilder.builder(
                                tablePath, catalogTable, MySqlTypeConverter.DEFAULT_INSTANCE, true)
                        .build(DatabaseIdentifier.TIDB);

        Assertions.assertTrue(createTableSql.contains("ENGINE = InnoDB"));
        Assertions.assertTrue(createTableSql.contains("DEFAULT CHARSET = utf8mb4"));
        Assertions.assertTrue(createTableSql.contains("COLLATE = utf8mb4_unicode_ci"));
    }

    /**
     * TiDB inherits the MySQL catalog, whose converter now accepts MySQL 9.0 VECTOR. A TiDB VECTOR
     * column must still be rejected rather than read as MySQL-layout bytes.
     */
    @Test
    void testTiDBCatalogStillRejectsVectorColumn() throws Exception {
        TiDBCatalog catalog =
                new TiDBCatalog(
                        DatabaseIdentifier.TIDB,
                        "root",
                        "",
                        JdbcUrlUtil.getUrlInfo("jdbc:mysql://localhost:4000/test"),
                        "com.mysql.cj.jdbc.Driver");
        ResultSet column = Mockito.mock(ResultSet.class);
        Mockito.when(column.getString("DATA_TYPE")).thenReturn("vector");
        Mockito.when(column.getString("COLUMN_NAME")).thenReturn("embedding");

        SeaTunnelRuntimeException error =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class, () -> catalog.buildColumn(column));
        Assertions.assertTrue(error.getMessage().contains("embedding"), error.getMessage());
        Assertions.assertTrue(error.getMessage().contains(DatabaseIdentifier.TIDB));
    }
}
