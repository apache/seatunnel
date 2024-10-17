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

package org.apache.seatunnel.e2e.connector.hudi;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK},
        disabledReason =
                "FLINK do not support local file catalog in hudi and Currently SPARK do not support cdc")
@Slf4j
public class HudiSinkCDCIT extends TestSuiteBase implements TestResource {

    // mysql
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_USER_PASSWORD = "seatunnel";
    private static final String MYSQL_DATABASE = "mysql_cdc";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";

    private static final String MYSQL_DRIVER =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";

    private static final String DATABASE = "st";
    private static final String TABLE_NAME = "st_test";
    private static final String TABLE_PATH = "/tmp/hudi/";
    private static final String NAMESPACE = "hudi";
    private static final String NAMESPACE_TAR = "hudi.tar.gz";

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private final Map<Integer, Record> records = new HashMap<>();

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

    protected final ContainerExtendedFactory containerExtendedFactory =
            new ContainerExtendedFactory() {
                @Override
                public void extend(GenericContainer<?> container)
                        throws IOException, InterruptedException {
                    container.execInContainer(
                            "sh",
                            "-c",
                            "cd /tmp" + " && tar -czvf " + NAMESPACE_TAR + " " + NAMESPACE);
                    container.copyFileFromContainer(
                            "/tmp/" + NAMESPACE_TAR, "/tmp/" + NAMESPACE_TAR);

                    extractFiles();
                }

                private void extractFiles() {
                    ProcessBuilder processBuilder = new ProcessBuilder();
                    processBuilder.command(
                            "sh", "-c", "cd /tmp" + " && tar -zxvf " + NAMESPACE_TAR);
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

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + TABLE_PATH);
                container.execInContainer("sh", "-c", "chmod -R 777  " + TABLE_PATH);
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "sh",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + MYSQL_DRIVER);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    private void insertRecord(Record record) {
        Integer id = record.getId();
        records.put(id, record);
    }

    private void deleteRecord(int id) {
        records.remove(id);
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        // close Container
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testMysqlCdc2Hudi(TestContainer container)
            throws IOException, InterruptedException {
        // Clear related content to ensure that multiple operations are not affected
        clearTable(MYSQL_DATABASE, SOURCE_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/hudi/mysql_cdc_to_hudi.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        // insert data and check
        insertAndCheckData(container);
        // upsert/delete data and check
        upsertAndCheckData(container);
    }

    private void insertAndCheckData(TestContainer container) throws InterruptedException {
        // Init table data
        initSourceTableData(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Path newestCommitFilePath =
                                    getNewestCommitFilePath(
                                            new File(
                                                    TABLE_PATH
                                                            + File.separator
                                                            + DATABASE
                                                            + File.separator
                                                            + TABLE_NAME));
                            ParquetReader<Group> reader =
                                    ParquetReader.builder(
                                                    new GroupReadSupport(), newestCommitFilePath)
                                            .withConf(configuration)
                                            .build();

                            // Read data and count rows
                            long rowCount = 0;
                            Group read = reader.read();
                            while (read != null) {
                                checkData(read);
                                read = reader.read();
                                rowCount++;
                            }
                            Assertions.assertEquals(3, rowCount);
                        });
        FileUtils.deleteFile(TABLE_PATH);
    }

    private void upsertAndCheckData(TestContainer container)
            throws InterruptedException, IOException {
        upsertDeleteSourceTable(MYSQL_DATABASE, SOURCE_TABLE);
        // Waiting 30s for source capture data
        sleep(30000);
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", LocalFileSystem.DEFAULT_FS);

        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy hudi to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Path newestCommitFilePath =
                                    getNewestCommitFilePath(
                                            new File(
                                                    TABLE_PATH
                                                            + File.separator
                                                            + DATABASE
                                                            + File.separator
                                                            + TABLE_NAME));
                            ParquetReader<Group> reader =
                                    ParquetReader.builder(
                                                    new GroupReadSupport(), newestCommitFilePath)
                                            .withConf(configuration)
                                            .build();
                            // Read data and count rows
                            long rowCount = 0;
                            Group read = reader.read();
                            while (read != null) {
                                checkData(read);
                                read = reader.read();
                                rowCount++;
                            }
                            Assertions.assertEquals(4, rowCount);
                        });
        FileUtils.deleteFile(TABLE_PATH);
    }

    public static Path getNewestCommitFilePath(File tablePathDir) throws IOException {
        File[] files = FileUtil.listFiles(tablePathDir);
        Long newestCommitTime =
                Arrays.stream(files)
                        .filter(file -> file.getName().endsWith(".parquet"))
                        .map(
                                file ->
                                        Long.parseLong(
                                                file.getName()
                                                        .substring(
                                                                file.getName().lastIndexOf("_") + 1,
                                                                file.getName()
                                                                        .lastIndexOf(".parquet"))))
                        .max(Long::compareTo)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Not found parquet file in " + tablePathDir));
        for (File file : files) {
            if (file.getName().endsWith(newestCommitTime + ".parquet")) {
                return new Path(file.toURI());
            }
        }
        throw new IllegalArgumentException("Not found parquet file in " + tablePathDir);
    }

    private void checkData(Group readRecord) {
        Integer id = readRecord.getInteger("id", 0);
        Record record = records.get(id);
        Assertions.assertNotNull(record);
        String f_json = readRecord.getString("f_json", 0);
        Long f_bigint = readRecord.getLong("f_bigint", 0);
        Assertions.assertEquals(
                JsonUtils.parseObject(record.getJson()), (JsonUtils.parseObject(f_json)));
        Assertions.assertEquals(record.getBigInt(), f_bigint);
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
        insertRecord(new Record(1, 123456789L, "{ \"key\": \"value\" }"));
        insertRecord(new Record(2, 123456789L, "{ \"key\": \"value\" }"));
        insertRecord(new Record(3, 123456789L, "{ \"key\": \"value\" }"));
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
                        + "VALUES ( 4, 0x61626374000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,\n"
                        + "         0x68656C6C6F, 0x18000000789C0BC9C82C5600A244859CFCBC7485B2C4A2A4CCBCC4A24A00697308D4, NULL,\n"
                        + "         0x74696E79626C6F62, 0x48656C6C6F20776F726C64, 12345, 54321, 123456, 654321, 1234567, 7654321, 1234567, 7654321,\n"
                        + "         1234567890, 987654321, 123, 789, 12.34, 56.78, 90.12, 'This is a long text field', 'This is a medium text field',\n"
                        + "         'This is a text field', 'This is a tiny text field', 'This is a varchar field', '2022-04-27', '2022-04-27 14:30:00',\n"
                        + "         '2023-04-27 11:08:40', 1, b'0101010101010101010101010101010101010101010101010101010101010101', 'C', 'enum2',\n"
                        + "         0x1B000000789C0BC9C82C5600A24485DCD494CCD25C85A49CFC2485B4CCD49C140083FF099A, 'This is a long varchar field',\n"
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1992 )");
        insertRecord(new Record(4, 1234567890L, "{ \"key\": \"value\" }"));

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
                        + "         12.345, '14:30:00', -128, 255, '{ \"key\": \"value\" }', 1999 )");
        insertRecord(new Record(5, 123456789L, "{ \"key\": \"value\" }"));

        executeSql("DELETE FROM " + database + "." + tableName + " where id = 2");
        deleteRecord(2);

        executeSql(
                "UPDATE "
                        + database
                        + "."
                        + tableName
                        + " SET f_bigint = 10000, f_json = '{ \"key\": \"value1\" }' where id = 3");
        insertRecord(new Record(3, 10000L, "{ \"key\": \"value1\" }"));
    }

    @Data
    @AllArgsConstructor
    static class Record {
        private Integer id;
        private Long bigInt;
        private String json;
    }
}
