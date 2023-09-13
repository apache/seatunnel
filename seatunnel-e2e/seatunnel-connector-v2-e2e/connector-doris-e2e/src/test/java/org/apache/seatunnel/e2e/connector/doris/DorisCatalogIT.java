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

package org.apache.seatunnel.e2e.connector.doris;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class DorisCatalogIT extends TestSuiteBase implements TestResource {

    private static final String DOCKER_IMAGE = "zykkk/doris:1.2.2.1-avx2-x86_84";
    private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String HOST = "doris_cdc_e2e";
    private static final int DOCKER_QUERY_PORT = 9030;
    private static final int DOCKER_HTTP_PORT = 8030;
    private static final int QUERY_PORT = 9939;
    private static final int HTTP_PORT = 8939;
    private static final String URL = "jdbc:mysql://%s:" + QUERY_PORT;
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "e2e_table_sink";
    private static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";
    private static final String SET_SQL =
            "ADMIN SET FRONTEND CONFIG (\"enable_batch_delete_by_default\" = \"true\")";

    private GenericContainer<?> container;
    private Connection jdbcConnection;
    private DorisCatalog catalog;

    @Override
    public void startUp() throws Exception {
        container =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withPrivilegedMode(true)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(DOCKER_IMAGE)));
        container.setPortBindings(Lists.newArrayList(String.format("%s:%s", QUERY_PORT, DOCKER_QUERY_PORT),
                String.format("%s:%s", HTTP_PORT, DOCKER_HTTP_PORT)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("doris container started");
        given().ignoreExceptions()
                .await()
                .atMost(10000, TimeUnit.SECONDS)
                .untilAsserted(this::initializeJdbcConnection);
        initCatalog();
    }

    private void initCatalog() {
        String catalogName = "doris";
        String frontEndNodes = container.getHost() + ":" + HTTP_PORT;
        DorisConfig config = DorisConfig.of(ReadonlyConfig.fromMap(new HashMap<>()));
        catalog = new DorisCatalog(catalogName, frontEndNodes, QUERY_PORT, USERNAME, PASSWORD, config, DATABASE);
        catalog.open();
    }

    @Test
    public void testCatalog() {

        if (catalog == null) {
            return;
        }

        TablePath tablePath = TablePath.of(DATABASE, SINK_TABLE);

        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("k1", BasicType.INT_TYPE, 10, false, 0, "k1"));
        builder.column(PhysicalColumn.of("k2", BasicType.STRING_TYPE, 64, false, "", "k2"));
        builder.column(PhysicalColumn.of("v1", BasicType.DOUBLE_TYPE, 10, true, null, "v1"));
        builder.column(PhysicalColumn.of("v2", new DecimalType(10, 2), 0, false, 0.1, "v2"));
        builder.primaryKey(PrimaryKey.of("pk", Arrays.asList("k1", "k2")));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", DATABASE, SINK_TABLE),
                        builder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test");

        boolean dbCreated = false;

        if (!catalog.databaseExists(tablePath.getDatabaseName())) {
            catalog.createDatabase(tablePath, false);
            dbCreated = true;
        }

        Assertions.assertFalse(catalog.tableExists(tablePath));
        catalog.createTable(tablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(tablePath));

        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));

        if (dbCreated) {
            catalog.dropDatabase(tablePath, false);
            Assertions.assertFalse(catalog.databaseExists(tablePath.getDatabaseName()));
        }

    }

    @Override
    public void tearDown() throws Exception {
        if (container != null) {
            container.close();
        }
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }
        if (catalog != null) {
            catalog.close();
        }
    }

    private void initializeJdbcConnection()
            throws SQLException, ClassNotFoundException, InstantiationException,
            IllegalAccessException, MalformedURLException {
        URLClassLoader urlClassLoader =
                new URLClassLoader(
                        new URL[] {new URL(DRIVER_JAR)}, DorisCDCSinkIT.class.getClassLoader());
        Thread.currentThread().setContextClassLoader(urlClassLoader);
        Driver driver = (Driver) urlClassLoader.loadClass(DRIVER_CLASS).newInstance();
        Properties props = new Properties();
        props.put("user", USERNAME);
        props.put("password", PASSWORD);
        jdbcConnection = driver.connect(String.format(URL, container.getHost()), props);
        try (Statement statement = jdbcConnection.createStatement()) {
            statement.execute(SET_SQL);
        }
    }

}
