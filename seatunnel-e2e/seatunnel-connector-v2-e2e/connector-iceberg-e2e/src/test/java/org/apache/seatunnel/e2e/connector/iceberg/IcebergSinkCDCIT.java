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
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SourceConfig;
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

                            // for next test.
                            dropTableColumn(MYSQL_DATABASE, SOURCE_TABLE, addField);
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
        configs.put(CommonConfig.KEY_CATALOG_NAME.key(), "seatunnel_test");
        configs.put(CommonConfig.KEY_NAMESPACE.key(), "seatunnel_namespace");
        configs.put(CommonConfig.KEY_TABLE.key(), "iceberg_sink_table");
        configs.put(CommonConfig.CATALOG_PROPS.key(), catalogProps);
        IcebergTableLoader tableLoader =
                IcebergTableLoader.create(new SourceConfig(ReadonlyConfig.fromMap(configs)));
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
}
