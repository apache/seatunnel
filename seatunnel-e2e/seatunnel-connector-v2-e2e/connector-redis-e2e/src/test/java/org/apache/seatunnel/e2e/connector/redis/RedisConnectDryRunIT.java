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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.sink.RedisSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.redis.source.RedisSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the factory-level connect dry-run contract against Redis without submitting a job. */
@Slf4j
public class RedisConnectDryRunIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "redis:7";
    private static final int REDIS_PORT = 6379;
    private static final String PASSWORD = "test-only-password";
    private static final String USER = "dry-run-user";
    private static final String USER_PASSWORD = "test-only-user-password";
    private static final int DB_NUM = 3;

    private GenericContainer<?> redis;
    private Jedis admin;

    @BeforeAll
    @Override
    public void startUp() {
        redis =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withExposedPorts(REDIS_PORT)
                        .withCommand("redis-server --requirepass " + PASSWORD)
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        redis.start();
        log.info("Password-protected Redis dry-run fixture started");
        admin = new Jedis(redis.getHost(), redis.getFirstMappedPort());
        admin.auth(PASSWORD);
        // A named ACL user, so that the user option is exercised against a real server.
        admin.aclSetUser(USER, "on", ">" + USER_PASSWORD, "+@all", "~*");
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            if (redis != null) {
                redis.stop();
            }
        }
    }

    @Test
    public void testSourceAndSinkValidationHaveNoKeyOrAclSideEffects() throws Exception {
        List<String> aclBefore = admin.aclList();

        List<CatalogTable> tables = validateSource(null, PASSWORD, redis.getFirstMappedPort());
        validateSink(null, PASSWORD, redis.getFirstMappedPort());
        validateSource(USER, USER_PASSWORD, redis.getFirstMappedPort());
        validateSink(USER, USER_PASSWORD, redis.getFirstMappedPort());

        assertEquals(1, tables.size());
        // No key space is created, neither in the default db nor in the configured db_num.
        admin.select(0);
        assertEquals(0L, admin.dbSize());
        admin.select(DB_NUM);
        assertEquals(0L, admin.dbSize());
        // Validating with a named user must not create or alter any ACL entry.
        assertEquals(aclBefore, admin.aclList());
    }

    @Test
    public void testIncorrectPasswordFailsValidation() {
        assertConnectionError(() -> validateSource(null, "incorrect", redis.getFirstMappedPort()));
        assertConnectionError(() -> validateSink(null, "incorrect", redis.getFirstMappedPort()));
    }

    @Test
    public void testMissingPasswordFailsValidation() {
        assertConnectionError(() -> validateSource(null, null, redis.getFirstMappedPort()));
        assertConnectionError(() -> validateSink(null, null, redis.getFirstMappedPort()));
    }

    @Test
    public void testNamedUserCredentialsAreVerified() {
        int port = redis.getFirstMappedPort();
        // The named user's password is checked, not the server-wide requirepass.
        assertConnectionError(() -> validateSource(USER, PASSWORD, port));
        assertConnectionError(() -> validateSink(USER, PASSWORD, port));
        assertConnectionError(() -> validateSource("unknown-user", USER_PASSWORD, port));
        assertConnectionError(() -> validateSink("unknown-user", USER_PASSWORD, port));
    }

    @Test
    public void testUnreachableServerFailsValidation() throws Exception {
        // A listener that drops every connection stands in for an unreachable Redis; unlike a
        // released ephemeral port it cannot be taken over by another process mid-test.
        try (ServerSocket server = droppingServer()) {
            String host = server.getInetAddress().getHostAddress();
            int port = server.getLocalPort();
            assertConnectionError(() -> validateSource(null, PASSWORD, host, port));
            assertConnectionError(() -> validateSink(null, PASSWORD, host, port));
        }
    }

    private List<CatalogTable> validateSource(String user, String password, int port)
            throws Exception {
        return validateSource(user, password, redis.getHost(), port);
    }

    private List<CatalogTable> validateSource(String user, String password, String host, int port)
            throws Exception {
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(options(user, password, host, port)),
                        getClass().getClassLoader());
        SupportSourceDryRunValidation validation = new RedisSourceFactory();
        List<CatalogTable> tables = validation.inferSchemaForDryRun(context);
        validation.validateConnectionForDryRun(context, tables);
        return tables;
    }

    private void validateSink(String user, String password, int port) throws Exception {
        validateSink(user, password, redis.getHost(), port);
    }

    private void validateSink(String user, String password, String host, int port)
            throws Exception {
        Map<String, Object> options = options(user, password, host, port);
        options.remove("keys");
        options.put("key", "id");
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        catalogTable(),
                        ReadonlyConfig.fromMap(options),
                        getClass().getClassLoader());
        SupportSinkDryRunValidation validation = new RedisSinkFactory();
        validation.validateConnectionForDryRun(context);
    }

    private static Map<String, Object> options(
            String user, String password, String host, int port) {
        Map<String, Object> options = new HashMap<>();
        options.put("host", host);
        options.put("port", port);
        options.put("db_num", DB_NUM);
        options.put("keys", "dry-run-*");
        options.put("data_type", "KEY");
        if (user != null) {
            options.put("user", user);
        }
        if (password != null) {
            options.put("auth", password);
        }
        return options;
    }

    private static void assertConnectionError(Validation validation) {
        RedisConnectorException exception =
                assertThrows(RedisConnectorException.class, validation::run);
        assertEquals(RedisErrorCode.REDIS_CONNECTION_ERROR, exception.getSeaTunnelErrorCode());
        assertFalse(exception.getMessage().contains(PASSWORD), exception.getMessage());
        assertFalse(exception.getMessage().contains(USER_PASSWORD), exception.getMessage());
        assertTrue(exception.getMessage().contains("dry-run"), exception.getMessage());
    }

    /** Listens on a loopback port and closes every accepted connection without answering. */
    private static ServerSocket droppingServer() throws IOException {
        ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        Thread acceptor =
                new Thread(
                        () -> {
                            while (!server.isClosed()) {
                                try (Socket ignored = server.accept()) {
                                    // Close immediately so the client sees a dropped connection.
                                } catch (IOException e) {
                                    // The server socket was closed by the test; stop accepting.
                                }
                            }
                        },
                        "redis-dry-run-dropping-server");
        acceptor.setDaemon(true);
        acceptor.start();
        return server;
    }

    private static CatalogTable catalogTable() {
        TableSchema schema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "default", null, "dry_run"),
                schema,
                new HashMap<>(),
                new ArrayList<>(),
                null,
                "catalog");
    }

    @FunctionalInterface
    private interface Validation {
        void run() throws Exception;
    }
}
