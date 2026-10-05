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

package org.apache.seatunnel.e2e.connector.elasticsearch;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mysql.cj.jdbc.Driver;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
public class ElasticsearchTimerFlushIT extends TestSuiteBase implements TestResource {

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private static final String MYSQL_HOST = "mysql_cdc_timer_flush_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String DATABASE = "shop";
    private static final String MYSQL_CDC_PLUGIN_LIB = "/tmp/seatunnel/plugins/MySQL-CDC/lib";

    private final UniqueDatabase shopDatabase = new UniqueDatabase(MYSQL_CONTAINER, DATABASE);

    private ElasticsearchContainer elasticsearchContainer;
    private EsRestClient esRestClient;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        elasticsearchContainer =
                new ElasticsearchContainer(
                                DockerImageName.parse("elasticsearch:8.9.0")
                                        .asCompatibleSubstituteFor(
                                                "docker.elastic.co/elasticsearch/elasticsearch"))
                        .withNetwork(NETWORK)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withNetworkAliases("elasticsearch")
                        .withPassword("elasticsearch")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("elasticsearch:8.9.0")));
        Startables.deepStart(Stream.of(elasticsearchContainer, MYSQL_CONTAINER)).join();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(
                "hosts",
                Lists.newArrayList("https://" + elasticsearchContainer.getHttpHostAddress()));
        configMap.put("username", "elastic");
        configMap.put("password", "elasticsearch");
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);
        esRestClient = EsRestClient.createInstance(ReadonlyConfig.fromMap(configMap));

        shopDatabase.createAndInitialize();
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> DependencyJar.of(Driver.class).copyTo(container, MYSQL_CDC_PLUGIN_LIB);

    @TestTemplate
    public void testElasticsearchTimerFlush(TestContainer testContainer) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return testContainer.executeJob(
                                        "/elasticsearch/mysqlcdc_to_elasticsearch_timer_flush.conf",
                                        jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                if (jobFuture.isDone()) {
                                    Container.ExecResult jobResult = jobFuture.get();
                                    Assertions.fail(
                                            "The streaming job terminated before reaching RUNNING: "
                                                    + jobResult.getStderr());
                                }
                                Assertions.assertEquals(
                                        "RUNNING", testContainer.getJobStatus(jobId));
                            });

            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the snapshot");
                                Assertions.assertEquals(9, getDocumentCount());
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
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the binlog event");
                                Assertions.assertEquals(10, getDocumentCount());
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

    private long getDocumentCount() throws Exception {
        return esRestClient.getIndexDocsCount("st_timer_flush").get(0).getDocsCount();
    }

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(esRestClient)) {
            esRestClient.close();
        }
        if (Objects.nonNull(elasticsearchContainer)) {
            elasticsearchContainer.close();
        }
    }
}
