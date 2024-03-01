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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

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
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.awaitility.Awaitility.given;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Spark and Flink engine can not auto create paimon table on worker node(e.g flink tm) by org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink.PaimonSink which can lead error")
@Slf4j
public class PaimonSinkCDCIT extends TestSuiteBase implements TestResource {

    private static final String CATALOG_DIR = "/tmp/paimon/";
    private static final String NAMESPACE = "seatunnel_namespace";
    private static final String MYSQL_HOST = "paimon-e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_USER_PASSWORD = "Abc!@#135_seatunnel";
    private static final String SOURCE_TABLE = "mysql_cdc_e2e_source_table";
    private static final String SOURCE_DATABASE = "db";
    private static final String DATABASE_SUFFIX = ".db";
    private static final String TARGET_TABLE = "st_test";
    private static final String TARGET_DATABASE = "seatunnel_namespace";
    private MySqlContainer mysqlContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Mysql container starting");
        this.mysqlContainer = startMySqlContainer();
        log.info("Mysql container started");
        initializeMysqlTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (mysqlContainer != null) {
            mysqlContainer.close();
        }
    }

    @TestTemplate
    public void testMysqlCDCSinkPaimon(TestContainer container) throws Exception {
        clearTable(SOURCE_DATABASE, SOURCE_TABLE);
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mysql_cdc_sink_paimon_case1.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        insertAndCheckData(container);
        upsertAndCheckData(container);
    }

    private MySqlContainer startMySqlContainer() {
        MySqlContainer container =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("mysql/server-gtids/my.cnf")
                        .withSetupSQL("mysql/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(SOURCE_DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-mysql-image")));

        Startables.deepStart(Stream.of(container)).join();
        return container;
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
                mysqlContainer.getJdbcUrl(),
                mysqlContainer.getUsername(),
                mysqlContainer.getPassword());
    }

    private void insertAndCheckData(TestContainer container)
            throws InterruptedException, IOException {
        // Init table data
        initSourceTableData(SOURCE_DATABASE, SOURCE_TABLE);
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
                            Assertions.assertEquals(3, loadPaimonData().size());
                        });
    }

    private void upsertAndCheckData(TestContainer container)
            throws InterruptedException, IOException {
        upsertDeleteSourceTable(SOURCE_DATABASE, SOURCE_TABLE);
        // Waiting 60s for source capture data
        sleep(30000);

        // stream stage
        given().ignoreExceptions()
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            // copy iceberg to local
                            container.executeExtraCommands(containerExtendedFactory);
                            List<PaimonRecord> internalRows = loadPaimonData();
                            Assertions.assertEquals(5, internalRows.size());
                            for (PaimonRecord paimonRecord : internalRows) {
                                if (paimonRecord.getPkId() == 3) {
                                    Assertions.assertEquals(150, paimonRecord.getScore());
                                }
                            }
                        });
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("sh", "-c", "mkdir -p " + CATALOG_DIR);
                container.execInContainer("sh", "-c", "chmod -R 777 " + CATALOG_DIR);
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
                                    + NAMESPACE
                                    + DATABASE_SUFFIX);
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
                        // 等待命令执行完成
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

    private List<PaimonRecord> loadPaimonData() throws Exception {
        Table table = getTable();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();
        List<PaimonRecord> result = new ArrayList<>();
        log.info(
                "====================================Paimon data===========================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        try (RecordReader<InternalRow> reader = tableRead.createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        result.add(
                                new PaimonRecord(
                                        row.getLong(0),
                                        row.getString(1).toString(),
                                        row.getInt(2)));
                        log.info(
                                "key_id:"
                                        + row.getLong(0)
                                        + ", name:"
                                        + row.getString(1)
                                        + ", score:"
                                        + row.getInt(2));
                    });
        }
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        log.info(
                "==========================================================================================");
        return result;
    }

    private Table getTable() {
        try {
            return getCatalog().getTable(getIdentifier());
        } catch (Catalog.TableNotExistException e) {
            // do something
            throw new RuntimeException("table not exist");
        }
    }

    private Identifier getIdentifier() {
        return Identifier.create(TARGET_DATABASE, TARGET_TABLE);
    }

    private Catalog getCatalog() {
        Options options = new Options();
        options.set("warehouse", "file://" + CATALOG_DIR);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        return catalog;
    }

    private void initializeMysqlTable() {
        String sql =
                String.format(
                        "create table if not exists %s.%s(\n"
                                + "    `pk_id`         bigint primary key,\n"
                                + "    `name`          varchar(255),\n"
                                + "    `score`         int\n"
                                + ")",
                        SOURCE_DATABASE, SOURCE_TABLE);
        executeSql(sql);
    }

    private void clearTable(String database, String tableName) {
        executeSql("truncate table " + database + "." + tableName);
    }

    private void initSourceTableData(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( pk_id, name, score )\n"
                        + "VALUES ( 1, 'person1', 100 ),\n"
                        + " ( 2, 'person2', 99 ),\n"
                        + " ( 3, 'person3', 98 );\n");
    }

    private void upsertDeleteSourceTable(String database, String tableName) {
        executeSql(
                "INSERT INTO "
                        + database
                        + "."
                        + tableName
                        + " ( pk_id, name, score )\n"
                        + "VALUES ( 4, 'person4', 100 ),\n"
                        + " ( 5, 'person5', 99 ),\n"
                        + " ( 7, 'person6', 98 );\n");
        executeSql("DELETE FROM " + database + "." + tableName + " where pk_id = 2");
        executeSql("UPDATE " + database + "." + tableName + " SET score = 150 where pk_id = 3");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class PaimonRecord {
        private Long pkId;
        private String name;
        private Integer score;
    }
}
