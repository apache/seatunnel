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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

/** 使用用户提供的驱动和已获许可的 Ingres 实例验证 JDBC Source。 */
@EnabledIfEnvironmentVariable(named = "INGRES_E2E", matches = "(?i)true")
public class JdbcIngresIT extends TestSuiteBase {
    private URLClassLoader driverLoader;
    private Connection connection;
    private Path driverJar;
    private String table;
    private boolean tableCreated;
    private Config connectionConfig;

    @BeforeAll
    void prepareIngres() throws Exception {
        String url = requireEnvironment("INGRES_E2E_URL");
        String user = requireEnvironment("INGRES_E2E_USER");
        String password = requireEnvironment("INGRES_E2E_PASSWORD");
        String owner = requireEnvironment("INGRES_E2E_OWNER");
        Assertions.assertTrue(
                url.startsWith("jdbc:ingres:"), "INGRES_E2E_URL must use jdbc:ingres:");
        Assertions.assertFalse(owner.contains("."), "INGRES_E2E_OWNER must not contain a dot");
        driverJar = Paths.get(requireEnvironment("INGRES_E2E_DRIVER_JAR")).toAbsolutePath();
        Assertions.assertTrue(
                Files.isRegularFile(driverJar), "INGRES_E2E_DRIVER_JAR must be a file");
        driverLoader =
                new URLClassLoader(
                        new URL[] {driverJar.toUri().toURL()}, getClass().getClassLoader());
        Driver driver =
                (Driver)
                        Class.forName("com.ingres.jdbc.IngresDriver", true, driverLoader)
                                .getDeclaredConstructor()
                                .newInstance();
        Properties credentials = new Properties();
        credentials.setProperty("user", user);
        credentials.setProperty("password", password);
        connection = driver.connect(url, credentials);
        Assertions.assertNotNull(connection, "The supplied driver must accept the Ingres URL");
        connection.setAutoCommit(true);
        String tableName =
                "st_ingres_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        table = quote(owner) + "." + quote(tableName);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "CREATE TABLE "
                            + table
                            + " (\"id\" INTEGER NOT NULL PRIMARY KEY, \"Order\" VARCHAR(40), \"amount\" DECIMAL(10, 2))");
            tableCreated = true;
        }
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO "
                                + table
                                + " (\"id\", \"Order\", \"amount\") VALUES (?, ?, ?)")) {
            for (int id = 1; id <= 2; id++) {
                insert.setInt(1, id);
                insert.setString(2, id == 1 ? "quoted column" : null);
                insert.setBigDecimal(3, new java.math.BigDecimal("14.25"));
                insert.executeUpdate();
            }
        }
        connectionConfig =
                ConfigFactory.empty()
                        .withValue("source.Jdbc.url", ConfigValueFactory.fromAnyRef(url))
                        .withValue("source.Jdbc.username", ConfigValueFactory.fromAnyRef(user))
                        .withValue("source.Jdbc.password", ConfigValueFactory.fromAnyRef(password))
                        .withValue(
                                "source.Jdbc.table_path",
                                ConfigValueFactory.fromAnyRef(owner + "." + tableName));
    }

    @TestTemplate
    void testIngresSource(TestContainer container) throws Exception {
        container.executeExtraCommands(
                engine -> {
                    Container.ExecResult mkdir =
                            engine.execInContainer(
                                    "mkdir", "-p", "/tmp/seatunnel/plugins/Jdbc/lib");
                    Assertions.assertEquals(0, mkdir.getExitCode());
                    engine.copyFileToContainer(
                            MountableFile.forHostPath(driverJar),
                            "/tmp/seatunnel/plugins/Jdbc/lib/iijdbc.jar");
                });
        try {
            // 分别验证两行，避免总行数正确但 NULL 或主键值错误时误通过。
            for (int id = 1; id <= 2; id++) {
                byte[] config =
                        connectionConfig
                                .withValue(
                                        "source.Jdbc.where_condition",
                                        ConfigValueFactory.fromAnyRef("where \"id\" = " + id))
                                .withValue("ingres_expected_id", ConfigValueFactory.fromAnyRef(id))
                                .withValue(
                                        "ingres_string_rule",
                                        ConfigValueFactory.fromAnyRef(
                                                id == 1 ? "NOT_NULL" : "NULL"))
                                .root()
                                .render(ConfigRenderOptions.concise())
                                .getBytes(StandardCharsets.UTF_8);
                // 配置仅写入测试容器，不将凭据放入命令行或资源目录。
                container.executeExtraCommands(
                        engine ->
                                engine.copyFileToContainer(
                                        Transferable.of(config, 0600),
                                        "/tmp/ingres-jdbc-connection.conf"));
                Container.ExecResult result =
                        container.executeJob("/jdbc_ingres_source_to_assert.conf");
                Assertions.assertEquals(0, result.getExitCode());
            }
        } finally {
            container.executeExtraCommands(
                    engine ->
                            engine.execInContainer("rm", "-f", "/tmp/ingres-jdbc-connection.conf"));
        }
    }

    @AfterAll
    void cleanUpIngres() throws Exception {
        try {
            if (connection != null) {
                try (Connection ignored = connection) {
                    if (tableCreated) {
                        try (Statement statement = connection.createStatement()) {
                            statement.executeUpdate("DROP TABLE " + table);
                        }
                    }
                }
            }
        } finally {
            if (driverLoader != null) {
                driverLoader.close();
            }
        }
    }

    private static String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String requireEnvironment(String name) {
        String value = System.getenv(name);
        Assertions.assertNotNull(value, name + " must be set when INGRES_E2E=true");
        Assertions.assertFalse(value.isEmpty(), name + " must not be empty");
        return value;
    }
}
