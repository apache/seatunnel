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

package org.apache.seatunnel.e2e.connector.v2.mongodb;

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
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
public class MongodbTimerFlushIT extends TestSuiteBase implements TestResource {

    private static final String MONGODB_IMAGE = "mongo:latest";
    private static final String MONGODB_HOST = "mongodb_timer_flush_e2e";
    private static final int MONGODB_PORT = 27017;
    private static final String MONGODB_DATABASE = "test_db";
    private static final String MONGODB_COLLECTION = "mongodb_timer_flush";
    private static final String MYSQL_HOST = "mysql_mongodb_timer_flush_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String MYSQL_DATABASE = "shop";
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";
    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase = new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);
    private GenericContainer<?> mongodbContainer;
    private MongoClient mongoClient;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(com.mysql.cj.jdbc.Driver.class)
                            .copyTo(container, MYSQL_CDC_PLUGIN_LIB);

    @BeforeAll
    @Override
    public void startUp() {
        mongodbContainer =
                new GenericContainer<>(DockerImageName.parse(MONGODB_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MONGODB_HOST)
                        .withExposedPorts(MONGODB_PORT)
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MONGODB_IMAGE)));
        Startables.deepStart(Stream.of(mongodbContainer, MYSQL_CONTAINER)).join();

        await().ignoreExceptions()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initializeMongoClient);
        shopDatabase.createAndInitialize();
        mongoClient.getDatabase(MONGODB_DATABASE).getCollection(MONGODB_COLLECTION).drop();
    }

    @TestTemplate
    public void testMongodbTimerFlush(TestContainer testContainer) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return testContainer.executeJob(
                                        "/mysqlcdc_to_mongodb_timer_flush.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(
                                        jobFuture,
                                        "The streaming job terminated before reaching RUNNING");
                                Assertions.assertEquals(
                                        "RUNNING", testContainer.getJobStatus(jobId));
                            });

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(
                                        jobFuture,
                                        "The streaming job terminated before timer flush published the snapshot");
                                Assertions.assertEquals(9, documentCount());
                            });

            try (Connection connection = shopDatabase.getJdbcConnection();
                    Statement statement = connection.createStatement()) {
                statement.executeUpdate(
                        "INSERT INTO products (id, name, description, weight) "
                                + "VALUES (110, 'timer-flush', 'timer-flush probe', 1.0)");
            }

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertJobStillRunning(
                                        jobFuture,
                                        "The streaming job terminated before timer flush published the binlog event");
                                Assertions.assertEquals(10, documentCount());
                            });
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = testContainer.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }

        Container.ExecResult jobResult = jobFuture.get(120, TimeUnit.SECONDS);
        Assertions.assertEquals(0, jobResult.getExitCode(), jobResult.getStderr());
    }

    private long documentCount() {
        return mongoClient
                .getDatabase(MONGODB_DATABASE)
                .getCollection(MONGODB_COLLECTION)
                .countDocuments();
    }

    private void assertJobStillRunning(
            CompletableFuture<Container.ExecResult> jobFuture, String message) throws Exception {
        if (jobFuture.isDone()) {
            Container.ExecResult jobResult = jobFuture.get();
            Assertions.fail(message + ":\n" + jobResult.getStderr());
        }
    }

    private void initializeMongoClient() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        mongoClient =
                MongoClients.create(
                        String.format(
                                "mongodb://%s:%d",
                                mongodbContainer.getHost(),
                                mongodbContainer.getMappedPort(MONGODB_PORT)));
        mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
        MYSQL_CONTAINER.close();
    }
}
