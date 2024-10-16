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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm.DamengCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase.OceanBaseCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalogFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.tidb.TiDBCatalogFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

public class PreviewActionTest {

    private static final CatalogTable CATALOG_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "test",
                                            BasicType.STRING_TYPE,
                                            (Long) null,
                                            true,
                                            null,
                                            ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    public void testMySQLPreviewAction() {
        MySqlCatalogFactory factory = new MySqlCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_DATABASE,
                "CREATE DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_DATABASE,
                "DROP DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE `testtable` (\n"
                        + "\t`test` LONGTEXT NULL COMMENT ''\n"
                        + ") COMMENT = 'comment';",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testDMPreviewAction() {
        DamengCatalogFactory factory = new DamengCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "Dameng",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.CREATE_DATABASE,
                                "CREATE DATABASE \"testddatabase\";",
                                Optional.empty()));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.DROP_DATABASE,
                                "DROP DATABASE \"testddatabase\";",
                                Optional.empty()));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE \"null\".\"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE \"testtable\"",
                Optional.empty());

        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE \"testtable\" (\n" + "\"test\" TEXT\n" + ")",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testOceanBasePreviewAction() {
        OceanBaseCatalogFactory factory = new OceanBaseCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("compatibleMode", "oracle");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.CREATE_DATABASE,
                                "CREATE DATABASE `testddatabase`;",
                                Optional.empty()));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.DROP_DATABASE,
                                "DROP DATABASE `testddatabase`;",
                                Optional.empty()));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE \"null\".\"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE \"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE \"testtable\" (\n" + "\"test\" VARCHAR2(4000)\n" + ")",
                Optional.of(CATALOG_TABLE));

        Catalog catalog2 =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("compatibleMode", "mysql");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        assertPreviewResult(
                catalog2,
                Catalog.ActionType.CREATE_DATABASE,
                "CREATE DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog2,
                Catalog.ActionType.DROP_DATABASE,
                "DROP DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog2,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog2,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog2,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE `testtable` (\n"
                        + "\t`test` LONGTEXT NULL COMMENT ''\n"
                        + ") COMMENT = 'comment';",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testOraclePreviewAction() {
        OracleCatalogFactory factory = new OracleCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.CREATE_DATABASE,
                                "CREATE DATABASE `testddatabase`;",
                                Optional.empty()));
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () ->
                        assertPreviewResult(
                                catalog,
                                Catalog.ActionType.DROP_DATABASE,
                                "DROP DATABASE `testddatabase`;",
                                Optional.empty()));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE \"null\".\"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE \"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE \"testtable\" (\n" + "\"test\" VARCHAR2(4000)\n" + ")",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testPostgresPreviewAction() {
        PostgresCatalogFactory factory = new PostgresCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_DATABASE,
                "CREATE DATABASE \"testddatabase\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_DATABASE,
                "DROP DATABASE \"testddatabase\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE  \"null\".\"testtable\"",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE \"null\".\"testtable\"",
                Optional.empty());

        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE \"testtable\" (\n" + "\"test\" text\n" + ");",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testSqlServerPreviewAction() {
        SqlServerCatalogFactory factory = new SqlServerCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put(
                                                "base-url",
                                                "jdbc:sqlserver://localhost:1433;databaseName=column_type_test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_DATABASE,
                "CREATE DATABASE testddatabase",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_DATABASE,
                "DROP DATABASE testddatabase;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE  [testddatabase].[testtable]",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE testddatabase.testtable",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "IF OBJECT_ID('[testddatabase].[testtable]', 'U') IS NULL \n"
                        + "BEGIN \n"
                        + "CREATE TABLE [testddatabase].[testtable] ( \n"
                        + "\t[test] NVARCHAR(MAX) NULL\n"
                        + ");\n"
                        + "EXEC testddatabase.sys.sp_addextendedproperty 'MS_Description', N'comment', 'schema', N'null', 'table', N'testtable';\n"
                        + "EXEC testddatabase.sys.sp_addextendedproperty 'MS_Description', N'', 'schema', N'null', 'table', N'testtable', 'column', N'test';\n"
                        + "\n"
                        + "END",
                Optional.of(CATALOG_TABLE));
    }

    @Test
    public void testTiDBPreviewAction() {
        TiDBCatalogFactory factory = new TiDBCatalogFactory();
        Catalog catalog =
                factory.createCatalog(
                        "test",
                        ReadonlyConfig.fromMap(
                                new HashMap<String, Object>() {
                                    {
                                        put("base-url", "jdbc:mysql://localhost:3306/test");
                                        put("username", "root");
                                        put("password", "root");
                                    }
                                }));
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_DATABASE,
                "CREATE DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_DATABASE,
                "DROP DATABASE `testddatabase`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.TRUNCATE_TABLE,
                "TRUNCATE TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.DROP_TABLE,
                "DROP TABLE `testddatabase`.`testtable`;",
                Optional.empty());
        assertPreviewResult(
                catalog,
                Catalog.ActionType.CREATE_TABLE,
                "CREATE TABLE `testtable` (\n"
                        + "\t`test` LONGTEXT NULL COMMENT ''\n"
                        + ") COMMENT = 'comment';",
                Optional.of(CATALOG_TABLE));
    }

    private void assertPreviewResult(
            Catalog catalog,
            Catalog.ActionType actionType,
            String expectedSql,
            Optional<CatalogTable> catalogTable) {
        PreviewResult previewResult =
                catalog.previewAction(
                        actionType, TablePath.of("testddatabase.testtable"), catalogTable);
        Assertions.assertInstanceOf(SQLPreviewResult.class, previewResult);
        Assertions.assertEquals(expectedSql, ((SQLPreviewResult) previewResult).getSql());
    }
}
