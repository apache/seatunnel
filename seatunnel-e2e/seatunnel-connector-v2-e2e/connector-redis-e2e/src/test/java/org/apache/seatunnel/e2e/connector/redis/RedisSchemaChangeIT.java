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

package org.apache.seatunnel.e2e.connector.redis;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.mysql.cj.jdbc.Driver;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Only Zeta supports schema evolution for this streaming CDC case.")
public class RedisSchemaChangeIT extends TestSuiteBase implements TestResource {
    private static final String MYSQL_HOST = "mysql_cdc_redis_e2e";
    private static final String REDIS_HOST = "redis_cdc_e2e";
    private static final int REDIS_PORT = 6379;
    private static final String REDIS_PASSWORD = "SeaTunnel";
    private static final String REDIS_IMAGE = "redis:7";
    private static final String MYSQL_IMAGE = "mysql:8.0.43";

    private static final String DATABASE = "shop";
    private static final String TABLE = "schema_events";
    private static final String MYSQL_USER_NAME = "root";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String KEY_1 = "schema-change:1";
    private static final String KEY_2 = "schema-change:2";
    private static final String JOB_CONFIG = "/mysqlcdc_to_redis_with_schema_change.conf";
    private static final String INCREMENTAL_READ_MARKER =
            "Start incremental read task for incremental split";
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";

    private static final MySQLContainer<?> MYSQL_CONTAINER = createMySqlContainer();

    private GenericContainer<?> redisContainer;
    private Connection mysqlConnection;
    private Jedis jedis;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            RedisSchemaChangeIT::copyMySQLDriverToContainer;

    @BeforeAll
    @Override
    public void startUp() throws SQLException {
        redisContainer =
                new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(REDIS_HOST)
                        .withExposedPorts(REDIS_PORT)
                        .withCommand("redis-server --requirepass " + REDIS_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(REDIS_IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        Startables.deepStart(Stream.of(MYSQL_CONTAINER, redisContainer)).join();

        mysqlConnection =
                DriverManager.getConnection(
                        MYSQL_CONTAINER.getJdbcUrl(),
                        MYSQL_CONTAINER.getUsername(),
                        MYSQL_CONTAINER.getPassword());
        jedis = new Jedis(redisContainer.getHost(), redisContainer.getFirstMappedPort());
        jedis.auth(REDIS_PASSWORD);
        Assertions.assertEquals("PONG", jedis.ping());

        executeSql(
                "CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw'",
                "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, "
                        + "REPLICATION CLIENT, DROP, LOCK TABLES ON *.* TO 'st_user_source'@'%'",
                "FLUSH PRIVILEGES",
                "DROP TABLE IF EXISTS " + DATABASE + "." + TABLE,
                "CREATE TABLE "
                        + DATABASE
                        + "."
                        + TABLE
                        + " ("
                        + "id BIGINT NOT NULL PRIMARY KEY,"
                        + "name VARCHAR(64),"
                        + "legacy_note VARCHAR(128))",
                "INSERT INTO "
                        + DATABASE
                        + "."
                        + TABLE
                        + "(id,name,legacy_note) VALUES "
                        + "(1,'before-change','to-be-dropped')");
        jedis.del(KEY_1, KEY_2);
    }

    @TestTemplate
    public void testMysqlCdcToRedisSchemaEvolution(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(JOB_CONFIG, jobId);
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }
                        });

        try {
            awaitRedisJson(
                    jobFuture,
                    KEY_1,
                    json -> {
                        Assertions.assertEquals(3, json.size());
                        Assertions.assertEquals(1L, json.get("id").asLong());
                        Assertions.assertEquals("before-change", json.get("name").asText());
                        Assertions.assertEquals("to-be-dropped", json.get("legacy_note").asText());
                    });
            waitForIncrementalRead(container, jobFuture);

            executeSql(
                    "ALTER TABLE "
                            + DATABASE
                            + "."
                            + TABLE
                            + " ADD COLUMN email VARCHAR(128) NULL AFTER name",
                    "UPDATE "
                            + DATABASE
                            + "."
                            + TABLE
                            + " SET email='alice@example.test' WHERE id=1");

            awaitRedisJson(
                    jobFuture,
                    KEY_1,
                    json -> {
                        Assertions.assertEquals(4, json.size());
                        Assertions.assertEquals("alice@example.test", json.get("email").asText());
                        Assertions.assertEquals("to-be-dropped", json.get("legacy_note").asText());
                    });

            executeSql(
                    "ALTER TABLE "
                            + DATABASE
                            + "."
                            + TABLE
                            + " ADD COLUMN score INT NULL AFTER email",
                    "UPDATE " + DATABASE + "." + TABLE + " SET score=42 WHERE id=1",
                    "INSERT INTO "
                            + DATABASE
                            + "."
                            + TABLE
                            + "(id,name,email,score,legacy_note) VALUES "
                            + "(2,'second-row','second@example.test',84,'second-legacy')");

            awaitRedisJson(
                    jobFuture,
                    KEY_1,
                    json -> {
                        Assertions.assertEquals(5, json.size());
                        Assertions.assertEquals("alice@example.test", json.get("email").asText());
                        Assertions.assertEquals(42, json.get("score").asInt());
                    });
            awaitRedisJson(
                    jobFuture,
                    KEY_2,
                    json -> {
                        Assertions.assertEquals(5, json.size());
                        Assertions.assertEquals("second-row", json.get("name").asText());
                        Assertions.assertEquals("second@example.test", json.get("email").asText());
                        Assertions.assertEquals(84, json.get("score").asInt());
                        Assertions.assertEquals("second-legacy", json.get("legacy_note").asText());
                    });

            executeSql(
                    "ALTER TABLE " + DATABASE + "." + TABLE + " DROP COLUMN legacy_note",
                    "UPDATE "
                            + DATABASE
                            + "."
                            + TABLE
                            + " SET name='after-drop' WHERE id IN (1,2)");

            awaitRedisJson(jobFuture, KEY_1, RedisSchemaChangeIT::assertAfterDrop);
            awaitRedisJson(jobFuture, KEY_2, RedisSchemaChangeIT::assertAfterDrop);
        } finally {
            Throwable cleanupFailure = stopJobAndAwait(container, jobId, jobFuture);
            try {
                jedis.del(KEY_1, KEY_2);
            } catch (Throwable failure) {
                cleanupFailure = mergeFailure(cleanupFailure, failure);
            }
            rethrowFailure(cleanupFailure);
        }
    }

    private static MySQLContainer<?> createMySqlContainer() {
        return new MySQLContainer<>(DockerImageName.parse(MYSQL_IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withCommand(
                        "--server-id=223344",
                        "--log-bin=mysql-bin",
                        "--binlog-format=ROW",
                        "--gtid-mode=ON",
                        "--enforce-gtid-consistency=ON")
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private void awaitRedisJson(
            CompletableFuture<Container.ExecResult> jobFuture,
            String key,
            Consumer<ObjectNode> assertion) {
        await().atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertJobHasNotFinished(jobFuture);
                            String value = jedis.get(key);
                            Assertions.assertNotNull(value, "Redis key is not available: " + key);
                            assertion.accept(JsonUtils.parseObject(value));
                        });
    }

    private void waitForIncrementalRead(
            TestContainer container, CompletableFuture<Container.ExecResult> jobFuture) {
        await().atMost(180, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertJobHasNotFinished(jobFuture);
                            String serverLogs = container.getServerLogs();
                            Assertions.assertTrue(
                                    serverLogs.contains(INCREMENTAL_READ_MARKER),
                                    "Incremental reader has not started yet");
                            Assertions.assertTrue(
                                    serverLogs.contains(DATABASE + "." + TABLE),
                                    "Incremental reader has not captured the source table");
                        });
    }

    private void executeSql(String... sqlStatements) {
        try (Statement statement = mysqlConnection.createStatement()) {
            for (String sql : sqlStatements) {
                statement.execute(sql);
                String statementType = sql.trim().split("\\s+", 2)[0].toUpperCase(Locale.ROOT);
                log.info("Executed MySQL {} statement", statementType);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute MySQL schema evolution SQL", e);
        }
    }

    private static void assertAfterDrop(ObjectNode json) {
        Assertions.assertEquals(4, json.size());
        Assertions.assertEquals("after-drop", json.get("name").asText());
        Assertions.assertTrue(json.has("email"));
        Assertions.assertTrue(json.has("score"));
        Assertions.assertFalse(json.has("legacy_note"));
    }

    private static void assertJobHasNotFinished(CompletableFuture<Container.ExecResult> jobFuture) {
        if (!jobFuture.isDone()) {
            return;
        }
        try {
            Container.ExecResult result = jobFuture.getNow(null);
            Assertions.fail(
                    "Streaming job finished before Redis assertion. exitCode="
                            + result.getExitCode()
                            + ", stderr="
                            + result.getStderr());
        } catch (CompletionException e) {
            Assertions.fail("Streaming job failed before Redis assertion", e.getCause());
        }
    }

    private static Throwable stopJobAndAwait(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture) {
        Throwable failure = null;
        if (!jobFuture.isDone()) {
            try {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            } catch (Throwable cancelFailure) {
                failure = mergeFailure(failure, cancelFailure);
            }
        }
        try {
            Container.ExecResult result = jobFuture.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        } catch (Throwable jobFailure) {
            failure = mergeFailure(failure, jobFailure);
        }
        return failure;
    }

    private static Throwable mergeFailure(Throwable previousFailure, Throwable failure) {
        if (previousFailure == null) {
            return failure;
        }
        previousFailure.addSuppressed(failure);
        return previousFailure;
    }

    private static void rethrowFailure(Throwable failure) throws Exception {
        if (failure == null) {
            return;
        }
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new AssertionError("Failed to stop schema evolution test job", failure);
    }

    private static void copyMySQLDriverToContainer(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult mkdirResult =
                container.execInContainer("bash", "-c", "mkdir -p " + MYSQL_CDC_PLUGIN_LIB);
        Assertions.assertEquals(0, mkdirResult.getExitCode(), mkdirResult.getStderr());

        Path driverJarPath = mysqlDriverJarPath();
        container.copyFileToContainer(
                MountableFile.forHostPath(driverJarPath),
                MYSQL_CDC_PLUGIN_LIB + "/" + driverJarPath.getFileName());
    }

    private static Path mysqlDriverJarPath() {
        try {
            Path driverJarPath =
                    Paths.get(
                            Driver.class
                                    .getProtectionDomain()
                                    .getCodeSource()
                                    .getLocation()
                                    .toURI());
            Assertions.assertTrue(Files.isRegularFile(driverJarPath));
            return driverJarPath;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to resolve MySQL JDBC driver jar from the test classpath", e);
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        Throwable failure = null;
        failure = closeResource(jedis, failure);
        failure = closeResource(mysqlConnection, failure);
        failure = closeResource(redisContainer, failure);
        failure = closeResource(MYSQL_CONTAINER, failure);
        if (failure != null) {
            throw new Exception(
                    "Failed to close one or more schema evolution test resources", failure);
        }
    }

    private static Throwable closeResource(AutoCloseable resource, Throwable previousFailure) {
        if (Objects.isNull(resource)) {
            return previousFailure;
        }
        try {
            resource.close();
        } catch (Throwable failure) {
            if (previousFailure == null) {
                return failure;
            }
            previousFailure.addSuppressed(failure);
        }
        return previousFailure;
    }
}
