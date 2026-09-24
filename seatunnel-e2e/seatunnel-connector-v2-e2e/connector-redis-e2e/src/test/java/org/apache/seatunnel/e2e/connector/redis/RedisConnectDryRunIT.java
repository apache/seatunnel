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
import java.net.ServerSocket;
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

        List<CatalogTable> tables = validateSource(PASSWORD, redis.getFirstMappedPort());
        validateSink(PASSWORD, redis.getFirstMappedPort());

        assertEquals(1, tables.size());
        // No key space is created, neither in the default db nor in the configured db_num.
        admin.select(0);
        assertEquals(0L, admin.dbSize());
        admin.select(DB_NUM);
        assertEquals(0L, admin.dbSize());
        // The configured user option must not trigger ACL SETUSER.
        assertEquals(aclBefore, admin.aclList());
    }

    @Test
    public void testIncorrectPasswordFailsValidation() {
        assertConnectionError(() -> validateSource("incorrect", redis.getFirstMappedPort()));
        assertConnectionError(() -> validateSink("incorrect", redis.getFirstMappedPort()));
    }

    @Test
    public void testMissingPasswordFailsValidation() {
        assertConnectionError(() -> validateSource(null, redis.getFirstMappedPort()));
        assertConnectionError(() -> validateSink(null, redis.getFirstMappedPort()));
    }

    @Test
    public void testUnreachablePortFailsValidation() throws IOException {
        int closedPort = closedPort();
        assertConnectionError(() -> validateSource(PASSWORD, closedPort));
        assertConnectionError(() -> validateSink(PASSWORD, closedPort));
    }

    private List<CatalogTable> validateSource(String password, int port) throws Exception {
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        ReadonlyConfig.fromMap(options(password, port)),
                        getClass().getClassLoader());
        SupportSourceDryRunValidation validation = new RedisSourceFactory();
        List<CatalogTable> tables = validation.inferSchemaForDryRun(context);
        validation.validateConnectionForDryRun(context, tables);
        return tables;
    }

    private void validateSink(String password, int port) throws Exception {
        Map<String, Object> options = options(password, port);
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

    private Map<String, Object> options(String password, int port) {
        Map<String, Object> options = new HashMap<>();
        options.put("host", redis.getHost());
        options.put("port", port);
        options.put("db_num", DB_NUM);
        options.put("keys", "dry-run-*");
        options.put("data_type", "KEY");
        options.put("user", "dry-run-must-not-create-this-user");
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
        assertTrue(exception.getMessage().contains("dry-run"), exception.getMessage());
    }

    private static int closedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
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
