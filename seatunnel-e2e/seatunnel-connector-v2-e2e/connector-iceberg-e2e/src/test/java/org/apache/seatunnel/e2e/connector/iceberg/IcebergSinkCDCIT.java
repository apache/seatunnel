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

package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSourceConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType.HADOOP;
import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
@DisabledOnOs(OS.WINDOWS)
public class IcebergSinkCDCIT extends TestSuiteBase implements TestResource {

    private static final String CATALOG_DIR = "/tmp/seatunnel/iceberg/hadoop-cdc-sink/";

    private static final String NAMESPACE = "seatunnel_namespace";

    // mysql
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_USER_PASSWORD = "seatunnel";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("mysql/server-gtids/my.cnf")
                .withSetupSQL("mysql/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-mysql-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    private String zstdUrl() {
        return "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.5-5/zstd-jni-1.5.5-5.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + CATALOG_DIR);
                container.execInContainer("sh", "-c", "chmod -R 777 " + CATALOG_DIR);
                Container.ExecResult extraCommandsZSTD =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Iceberg/lib && cd /tmp/seatunnel/plugins/Iceberg/lib && wget "
                                        + zstdUrl());
                Assertions.assertEquals(
                        0, extraCommandsZSTD.getExitCode(), extraCommandsZSTD.getStderr());
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    private final String NAMESPACE_TAR = NAMESPACE + ".tar.gz";
    protected final ContainerExtendedFactory containerExtendedFactory =
            new ContainerExtendedFactory() {
                @Override
                public void extend(GenericContainer<?> container)
                        throws IOException, InterruptedException {
                    FileUtils.createNewDir(CATALOG_DIR);
                    container.execInContainer(
                            "sh",
                            "-c",
                            "cd "
                                    + CATALOG_DIR
                                    + " && tar -czvf "
                                    + NAMESPACE_TAR
                                    + " "
                                    + NAMESPACE);
                    container.copyFileFromContainer(
                            CATALOG_DIR + NAMESPACE_TAR, CATALOG_DIR + NAMESPACE_TAR);
                    extractFiles();
                }

                private void extractFiles() {
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command(
                            "sh", "-c", "cd " + CATALOG_DIR + " && tar -zxvf " + NAMESPACE_TAR);
                    try {
                        Process process = processBuilder.start();
                        int exitCode = process.waitFor();
                        if (exitCode == 0) {
                            log.info("Extract files successful.");
                        } else {
                            log.error("Extract files failed with exit code " + exitCode);
                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @TestTemplate
    public void testMysqlCdcCheckDataE2e(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/iceberg/mysql_cdc_to_iceberg.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        insertAndCheckData(container);
        upsertAndCheckData(container);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
    public void testMysqlCdcCheckSchemaChangeE2e(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/iceberg/mysql_cdc_to_iceberg_for_schema_change.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        initSourceTableData(MYSQL_DATABASE, SOURCE_TABLE);
        alterSchemaAndCheckIcebergSchema(container);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
    public void testMysqlCdcCheckMultiSchemaChangeE2e(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/iceberg/mysql_cdc_to_iceberg_for_schema_change.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        initSourceTableData(MYSQL_DATABASE, SOURCE_TABLE);
        alterMultiSchemaAndCheckIcebergSchema(container);
    }

    private void alterMultiSchemaAndCheckIcebergSchema(TestContainer container)
            throws InterruptedException, IOException {
        log.info("Starting multi-column schema evolution test cases");

        // Case 1: Test adding multiple columns in a single ALTER TABLE statement
        log.info("Case 1: Testing adding multiple columns in a single statement");
        String addField1 = "f_multi_add1";
        String addField2 = "f_multi_add2";
        String addField3 = "f_multi_add3";

        // Add multiple columns in a single ALTER TABLE statement
        String addMultiColumnsSql =
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN %s VARCHAR(255) DEFAULT 'multi-column-1', "
                                + "ADD COLUMN %s INT DEFAULT 42, "
                                + "ADD COLUMN %s FLOAT DEFAULT 3.14",
                        MYSQL_DATABASE, SOURCE_TABLE, addField1, addField2, addField3);
        executeSql(addMultiColumnsSql);

        // Insert data with the new columns
        String insertMultiColumnSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s, %s, %s) "
                                + "VALUES (200, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar multi-add', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'custom multi-column-1', 100, 9.99)",
                        MYSQL_DATABASE, SOURCE_TABLE, addField1, addField2, addField3);
        executeSql(insertMultiColumnSql);

        sleep(30000); // Wait for source capture data

        // Verify that multiple columns were added and data is correct
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();

                            // Verify all new columns exist
                            Types.NestedField field1 = schema.findField(addField1);
                            Types.NestedField field2 = schema.findField(addField2);
                            Types.NestedField field3 = schema.findField(addField3);

                            Assertions.assertNotNull(
                                    field1, "Column " + addField1 + " should exist");
                            Assertions.assertNotNull(
                                    field2, "Column " + addField2 + " should exist");
                            Assertions.assertNotNull(
                                    field3, "Column " + addField3 + " should exist");

                            // Verify data in the new columns
                            List<Record> records = loadIcebergTable();
                            boolean foundMultiColumnRecord = false;
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 200) {
                                    String stringValue = (String) record.getField(addField1);
                                    Integer intValue = (Integer) record.getField(addField2);
                                    Float floatValue = (Float) record.getField(addField3);

                                    Assertions.assertEquals("custom multi-column-1", stringValue);
                                    Assertions.assertEquals(100, intValue);
                                    Assertions.assertEquals(9.99f, floatValue, 0.01f);
                                    foundMultiColumnRecord = true;
                                }
                            }
                            Assertions.assertTrue(
                                    foundMultiColumnRecord,
                                    "Should find record with multiple new columns");
                        });
        // Case 2: Test modifying multiple column types in a single ALTER TABLE statement
        log.info("Case 2: Testing modifying multiple column types in a single statement");
        String modifyTypeField1 = "f_multi_type1";
        String modifyTypeField2 = "f_multi_type2";

        // Add columns first
        String addTypeColumnsSql =
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN %s VARCHAR(50) DEFAULT 'to-be-modified-type-1', "
                                + "ADD COLUMN %s INT DEFAULT 42",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyTypeField1, modifyTypeField2);
        executeSql(addTypeColumnsSql);

        // Insert data with the new columns
        String insertTypeColumnsSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s, %s) "
                                + "VALUES (300, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar for multi-type', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'original type value 1', 100)",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyTypeField1, modifyTypeField2);
        executeSql(insertTypeColumnsSql);

        sleep(30000); // Wait for source capture data

        // Now modify multiple column types in a single ALTER TABLE statement
        String modifyTypesSql =
                String.format(
                        "ALTER TABLE %s.%s MODIFY %s VARCHAR(500) DEFAULT 'modified-type-column-1', "
                                + "MODIFY %s BIGINT DEFAULT 1000",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyTypeField1, modifyTypeField2);
        executeSql(modifyTypesSql);

        // Insert data with the modified columns
        String insertAfterModifyTypesSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s, %s) "
                                + "VALUES (301, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar after multi-type', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'This is a much longer text value that would not fit in the original VARCHAR(50)', 2000)",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyTypeField1, modifyTypeField2);
        executeSql(insertAfterModifyTypesSql);

        sleep(30000); // Wait for source capture data

        // Verify that column types were modified and data is correct
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();

                            // Verify columns exist with correct types
                            Types.NestedField field1 = schema.findField(modifyTypeField1);
                            Types.NestedField field2 = schema.findField(modifyTypeField2);

                            Assertions.assertNotNull(
                                    field1, "Column " + modifyTypeField1 + " should exist");
                            Assertions.assertNotNull(
                                    field2, "Column " + modifyTypeField2 + " should exist");

                            // Verify data in the modified columns
                            List<Record> records = loadIcebergTable();
                            boolean foundModifiedRecord = false;
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 301) {
                                    String stringValue = (String) record.getField(modifyTypeField1);
                                    Long longValue = (Long) record.getField(modifyTypeField2);

                                    Assertions.assertEquals(
                                            "This is a much longer text value that would not fit in the original VARCHAR(50)",
                                            stringValue);
                                    Assertions.assertEquals(2000L, longValue.longValue());
                                    foundModifiedRecord = true;
                                }
                            }
                            Assertions.assertTrue(
                                    foundModifiedRecord,
                                    "Should find record with modified column types");
                        });
        // Case 3: Test modifying multiple columns in a single ALTER TABLE statement
        log.info("Case 3: Testing modifying multiple columns in a single statement");
        String modifyField1 = "f_multi_modify1";
        String modifyField2 = "f_multi_modify2";

        // Add columns first
        String addModifyColumnsSql =
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN %s VARCHAR(50) DEFAULT 'to-be-modified-1', "
                                + "ADD COLUMN %s INT DEFAULT 42",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyField1, modifyField2);
        executeSql(addModifyColumnsSql);

        // Insert data with the new columns
        String insertModifyColumnsSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s, %s) "
                                + "VALUES (400, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar for multi-modify', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'original multi-value for modify', 100)",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyField1, modifyField2);
        executeSql(insertModifyColumnsSql);

        sleep(30000); // Wait for source capture data

        // Now modify multiple columns in a single ALTER TABLE statement
        String modifyColumnsSql =
                String.format(
                        "ALTER TABLE %s.%s MODIFY %s TEXT, " + "MODIFY %s BIGINT DEFAULT 1000",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyField1, modifyField2);
        executeSql(modifyColumnsSql);

        // Insert data with the modified columns
        String insertAfterModifySql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s, %s) "
                                + "VALUES (401, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar after multi-modify', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'This is a much longer text value for multi-modify that would not fit in the original VARCHAR(50)', 3000)",
                        MYSQL_DATABASE, SOURCE_TABLE, modifyField1, modifyField2);
        executeSql(insertAfterModifySql);

        sleep(30000); // Wait for source capture data

        // Verify that columns were modified and data is correct
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();

                            // Verify columns exist with correct types
                            Types.NestedField fieldObj1 = schema.findField(modifyField1);
                            Types.NestedField fieldObj2 = schema.findField(modifyField2);

                            Assertions.assertNotNull(
                                    fieldObj1, "Column " + modifyField1 + " should exist");
                            Assertions.assertNotNull(
                                    fieldObj2, "Column " + modifyField2 + " should exist");

                            // Verify data in the modified columns
                            List<Record> records = loadIcebergTable();
                            boolean foundModifiedRecord = false;
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 401) {
                                    String stringValue = (String) record.getField(modifyField1);
                                    Long longValue = (Long) record.getField(modifyField2);

                                    Assertions.assertEquals(
                                            "This is a much longer text value for multi-modify that would not fit in the original VARCHAR(50)",
                                            stringValue);
                                    Assertions.assertEquals(3000L, longValue.longValue());
                                    foundModifiedRecord = true;
                                }
                            }
                            Assertions.assertTrue(
                                    foundModifiedRecord,
                                    "Should find record with modified columns");
                        });

        // Case 4: Test dropping multiple columns in a single ALTER TABLE statement
        // (AlterTableColumnsEvent)
        log.warn(
                "Case 4: Deleting multiple columns is not supported,unsupported table metadata field type 0 ");
    }

    private void alterSchemaAndCheckIcebergSchema(TestContainer container)
            throws InterruptedException, IOException {
        String addField = "f_string_add";
        // Init table data
        addTableColumn(MYSQL_DATABASE, SOURCE_TABLE, addField);
        insertAddColumnData(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);

        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();
                            Types.NestedField nestedField = schema.findField(addField);
                            Assertions.assertEquals(true, Objects.nonNull(nestedField));

                            List<Record> records = loadIcebergTable();
                            Assertions.assertEquals(4, records.size());
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                String f_string_add = (String) record.getField("f_string_add");
                                if (id == 100) {
                                    Assertions.assertEquals("add column field", f_string_add);
                                }
                            }
                        });

        String modifyField = "f_varchar";
        modifyTableColumn(MYSQL_DATABASE, SOURCE_TABLE, modifyField, "text");
        insertModifyColumnData(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);

        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<Record> records = loadIcebergTable();
                            Assertions.assertEquals(5, records.size());
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 101) {
                                    String f_varchar = (String) record.getField("f_varchar");
                                    Assertions.assertEquals(
                                            "This is a modified varchar field with longer text that would exceed the original varchar length",
                                            f_varchar);
                                }
                            }
                        });

        dropTableColumn(MYSQL_DATABASE, SOURCE_TABLE, addField);
        insertAfterDropColumnData(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);

        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();
                            Types.NestedField nestedField = schema.findField(addField);
                            // The column should be marked as deleted in Iceberg
                            Assertions.assertEquals(
                                    true, nestedField == null || !nestedField.isRequired());

                            List<Record> records = loadIcebergTable();
                            Assertions.assertEquals(6, records.size());
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 102) {
                                    // The dropped column should not be accessible or should be null
                                    try {
                                        Object droppedField = record.getField(addField);
                                        Assertions.assertNull(
                                                droppedField, "Dropped field should be null");
                                    } catch (Exception e) {
                                        log.info(
                                                "Field {} is not accessible after dropping, which is expected",
                                                addField);
                                    }
                                }
                            }
                        });

        // Testing changing a single column name
        String oldColumnName = "f_column_to_rename";
        String newColumnName = "f_renamed_column";

        // Add a column first
        String addColumnSql =
                String.format(
                        "ALTER TABLE %s.%s ADD COLUMN %s VARCHAR(255) DEFAULT 'to-be-renamed'",
                        MYSQL_DATABASE, SOURCE_TABLE, oldColumnName);
        executeSql(addColumnSql);

        // Insert data with the new column
        String insertSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year, %s) "
                                + "VALUES (150, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992, 'original column value')",
                        MYSQL_DATABASE, SOURCE_TABLE, oldColumnName);
        executeSql(insertSql);

        // Now rename the column
        String renameColumnSql =
                String.format(
                        "ALTER TABLE %s.%s CHANGE %s %s VARCHAR(255) DEFAULT 'renamed-column'",
                        MYSQL_DATABASE, SOURCE_TABLE, oldColumnName, newColumnName);
        executeSql(renameColumnSql);

        // Insert data with the renamed column
        String insertAfterRenameSql =
                String.format(
                        "INSERT INTO %s.%s (id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint, "
                                + "f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer, "
                                + "f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double, "
                                + "f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime, "
                                + "f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time, "
                                + "f_tinyint, f_tinyint_unsigned, f_json, f_year,  %s) "
                                + "VALUES (151, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, "
                                + "0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, "
                                + "0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, "
                                + "123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', "
                                + "'This is a text field', 'This is a tiny text field', 'test varchar after rename', '2022-04-27', '2022-04-27 14:30:00', "
                                + "'2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2', "
                                + "0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', "
                                + "12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992,  'renamed column value')",
                        MYSQL_DATABASE, SOURCE_TABLE, newColumnName);
        executeSql(insertAfterRenameSql);

        sleep(30000); // Wait for source capture data

        // Verify that column was renamed and data is correct
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            container.executeExtraCommands(containerExtendedFactory);
                            Schema schema = loadIcebergSchema();

                            // Verify old column is gone and new column exists
                            Types.NestedField oldField = schema.findField(oldColumnName);
                            Types.NestedField newField = schema.findField(newColumnName);

                            // Old column should be gone or marked as deleted
                            Assertions.assertTrue(
                                    oldField == null || !oldField.isRequired(),
                                    "Column "
                                            + oldColumnName
                                            + " should be deleted or marked optional");

                            // New column should exist
                            Assertions.assertNotNull(
                                    newField, "Column " + newColumnName + " should exist");

                            // Verify data in the renamed column
                            List<Record> records = loadIcebergTable();
                            boolean foundRenamedValue = false;
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                if (id == 151) {
                                    String renamedValue = (String) record.getField(newColumnName);
                                    Assertions.assertEquals("renamed column value", renamedValue);
                                    foundRenamedValue = true;
                                }
                            }
                            Assertions.assertTrue(
                                    foundRenamedValue, "Should find record with renamed column");
                        });
    }

    private void upsertAndCheckData(TestContainer container)
            throws InterruptedException, IOException {
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);

        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<Record> records = loadIcebergTable();
                            Assertions.assertEquals(4, records.size());
                            for (Record record : records) {
                                Integer id = (Integer) record.getField("id");
                                Long f_bigint = (Long) record.getField("f_bigint");
                                if (id == 3) {
                                    Assertions.assertEquals(10000, f_bigint);
                                }
                            }
                        });
    }

    private void insertAndCheckData(TestContainer container)
            throws InterruptedException, IOException {
        // Init table data
        initSourceTableData(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);

        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Assertions.assertEquals(3, loadIcebergTable().size());
                        });
    }

    private Schema loadIcebergSchema() {
        IcebergTableLoader tableLoader = getTableLoader();
        Table table = tableLoader.loadTable();
        return table.schema();
    }

    private List<Record> loadIcebergTable() {
        List<Record> results = new ArrayList<>();
        IcebergTableLoader tableLoader = getTableLoader();
        try {
            Table table = tableLoader.loadTable();
            try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                for (Record record : records) {
                    results.add(record);
                }
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return results;
    }

    @NotNull private static IcebergTableLoader getTableLoader() {
        Map<String, Object> configs = new HashMap<>();
        Map<String, Object> catalogProps = new HashMap<>();
        catalogProps.put("type", HADOOP.getType());
        catalogProps.put("warehouse", "file://" + CATALOG_DIR);
        configs.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(IcebergCommonOptions.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(IcebergCommonOptions.KEY_TABLE.key(), "iceberg_sink_table");
        configs.put(IcebergCommonOptions.CATALOG_PROPS.key(), catalogProps);
        IcebergTableLoader tableLoader =
                IcebergTableLoader.create(new IcebergSourceConfig(ReadonlyConfig.fromMap(configs)));
        tableLoader.open();
        return tableLoader;
    }

    private void dropTableColumn(String database, String tableName, String dropField) {
        executeSql("ALTER TABLE " + database + "." + tableName + " DROP COLUMN " + dropField);
    }

    private void addTableColumn(String database, String tableName, String addField) {
        executeSql(
                "ALTER TABLE " + database + "." + tableName + " ADD COLUMN " + addField + " text");
    }

    private void modifyTableColumn(
            String database, String tableName, String columnName, String newType) {
        executeSql(
                "ALTER TABLE "
                        + database
                        + "."
                        + tableName
                        + " MODIFY COLUMN "
                        + columnName
                        + " "
                        + newType);
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    // Execute SQL
    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        // close Container
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    private void initSourceTableData(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 1, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 2022 ),\n"
                        + "       ( 2, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                        + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321,\n"
                        + "         123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                        + "         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                        + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         112.345, '14:30:00', -128, 22, '{ \"key\": \"value\" }', 2013 ),\n"
                        + "       ( 3, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL, 0x74696E79626C6F62,\n"
                        + "         0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321, 123456789, 987654321, 123,\n"
                        + "         789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field', 'This is a text field',\n"
                        + "         'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00', '2023-04-27 11:08:40',\n"
                        + "         1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field', 112.345,\n"
                        + "         '14:30:00', -128, 22, '{ \"key\": \"value\" }', 2021 )");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 5, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year )\n"
                        + "VALUES ( 6, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");

        executeSql("UPDATE " + database + "." + tableName + " SET f_bigint = 10000 where id = 3");
    }

    private void insertAddColumnData(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year, f_string_add)\n"
                        + "VALUES ( 100, "
                        + "0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 , 'add column "
                        + "field')");
    }

    private void insertModifyColumnData(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year, f_string_add)\n"
                        + "VALUES ( 101, "
                        + "0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a modified varchar field with longer text that would exceed the original varchar length', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 , 'add column "
                        + "field')");
    }

    private void insertAfterDropColumnData(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( id, f_binary, f_blob, f_long_varbinary, f_longblob, f_tinyblob, f_varbinary, f_smallint,\n"
                        + "                                         f_smallint_unsigned, f_mediumint, f_mediumint_unsigned, f_int, f_int_unsigned, f_integer,\n"
                        + "                                         f_integer_unsigned, f_bigint, f_bigint_unsigned, f_numeric, f_decimal, f_float, f_double,\n"
                        + "                                         f_double_precision, f_longtext, f_mediumtext, f_text, f_tinytext, f_varchar, f_date, f_datetime,\n"
                        + "                                         f_timestamp, f_bit1, f_bit64, f_char, f_enum, f_mediumblob, f_long_varchar, f_real, f_time,\n"
                        + "                                         f_tinyint, f_tinyint_unsigned, f_json, f_year)\n"
                        + "VALUES ( 102, "
                        + "0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         123456789, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field after drop column', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992)");
    }
}
