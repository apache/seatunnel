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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

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

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

/**
 * Validates the documented MySQL CDC to Elasticsearch recipe with filtering, normalization,
 * metadata enrichment, and derived fields.
 */
@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "This recipe validates a MySQL CDC streaming pipeline and is kept on the zeta engine to match the getting-started documentation path.")
public class ElasticsearchRecipeIT extends TestSuiteBase implements TestResource {

    // MySQL hostname referenced by the documented job config.
    private static final String MYSQL_HOST = "mysql_cdc_elasticsearch_recipe";
    // Source database consumed by the CDC job.
    private static final String MYSQL_DATABASE = "crm";
    // Application user used for incremental source changes.
    private static final String MYSQL_USER_NAME = "mysqluser";
    // Shared password for the MySQL test users.
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    // Administrative MySQL user used to create the CDC account.
    private static final String MYSQL_ROOT_USER_NAME = "root";
    // CDC reader account configured inside the documented job.
    private static final String MYSQL_CDC_USER_NAME = "st_user_source";
    // Elasticsearch index populated by the documented sink config.
    private static final String INDEX_NAME = "recipe_customer_profile";
    // Elasticsearch service used by the recipe job.
    private ElasticsearchContainer elasticsearchContainer;
    // MySQL service used by the recipe job.
    private MySqlContainer mysqlContainer;
    // Helper that materializes the documented source table and seed rows.
    private UniqueDatabase crmDatabase;

    // Injects the MySQL JDBC driver required by the MySQL-CDC connector.
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container ->
                    DependencyJar.of(com.mysql.cj.jdbc.Driver.class)
                            .copyTo(container, "/tmp/seatunnel/plugins/MySQL-CDC/lib");

    /**
     * Starts the external systems and prepares the source data used by the documented recipe.
     *
     * <p>The source and sink hostnames must remain aligned with the submitted HOCON resource.
     */
    @BeforeEach
    @Override
    public void startUp() {
        elasticsearchContainer =
                new ElasticsearchContainer(
                                DockerImageName.parse(
                                        "docker.elastic.co/elasticsearch/elasticsearch:8.9.0"))
                        .withNetwork(NETWORK)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withNetworkAliases("elasticsearch")
                        .withPassword("elasticsearch")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("elasticsearch:8.9.0")));
        mysqlContainer =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));
        Startables.deepStart(Stream.of(elasticsearchContainer, mysqlContainer)).join();
        createCdcUser();
        crmDatabase = new UniqueDatabase(mysqlContainer, MYSQL_DATABASE);
        crmDatabase.setTemplateName("recipe_crm_customer_profile").createAndInitialize();
    }

    /**
     * Verifies snapshot filtering and field shaping followed by CDC update and insert events.
     *
     * @param container SeaTunnel runtime that executes the documented streaming job
     */
    @TestTemplate
    public void testMysqlCdcToElasticsearchRecipe(TestContainer container) {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        AtomicReference<Throwable> jobFailure = new AtomicReference<>();
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(
                                "/elasticsearch/mysqlcdc_to_elasticsearch_recipe.conf", jobId);
                    } catch (Exception e) {
                        jobFailure.set(e);
                        throw new RuntimeException(e);
                    }
                });

        await().atMost(120, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            assertNoJobFailure(jobFailure);
                            Map<Integer, JsonNode> docs = fetchIndexDocs();
                            Assertions.assertEquals(1, docs.size());
                            JsonNode doc1001 = docs.get(1001);
                            Assertions.assertNotNull(doc1001);
                            Assertions.assertEquals("Alice Zhang", doc1001.get("name").asText());
                            Assertions.assertEquals("13800001111", doc1001.get("phone").asText());
                            Assertions.assertEquals("ACTIVE", doc1001.get("status_name").asText());
                            Assertions.assertEquals("crm", doc1001.get("source_database").asText());
                            Assertions.assertEquals(
                                    "customer_profile", doc1001.get("source_table").asText());
                            Assertions.assertEquals(
                                    "mysql_cdc", doc1001.get("sync_source").asText());
                            Assertions.assertFalse(docs.containsKey(900));
                        });

        executeSql(
                "UPDATE crm.customer_profile SET name = ' Alice Zhang ', phone = '138-9999-0000', status = 2 WHERE id = 1001",
                "INSERT INTO crm.customer_profile (id, name, phone, email, status, city) VALUES "
                        + "(1003, 'Carol Wang', '137-1234-8888', 'carol@example.com', 1, 'Hangzhou')");

        await().atMost(120, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            assertNoJobFailure(jobFailure);
                            Map<Integer, JsonNode> docs = fetchIndexDocs();
                            Assertions.assertEquals(2, docs.size());

                            JsonNode doc1001 = docs.get(1001);
                            JsonNode doc1003 = docs.get(1003);
                            Assertions.assertNotNull(doc1001);
                            Assertions.assertNotNull(doc1003);

                            Assertions.assertEquals("FROZEN", doc1001.get("status_name").asText());
                            Assertions.assertEquals("13899990000", doc1001.get("phone").asText());
                            Assertions.assertEquals("Alice Zhang", doc1001.get("name").asText());
                            Assertions.assertEquals("+U", doc1001.get("row_kind").asText());

                            Assertions.assertEquals("ACTIVE", doc1003.get("status_name").asText());
                            Assertions.assertEquals("13712348888", doc1003.get("phone").asText());
                            Assertions.assertEquals("+I", doc1003.get("row_kind").asText());
                            Assertions.assertEquals(
                                    "mysql_cdc", doc1003.get("sync_source").asText());
                        });

        assertNoJobFailure(jobFailure);
        Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
        try {
            Container.ExecResult cancelResult = container.cancelJob(jobId);
            Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to cancel the recipe streaming job", e);
        }
    }

    /**
     * Stops the external systems created for the recipe test.
     *
     * <p>Each template invocation owns isolated MySQL and Elasticsearch containers.
     */
    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(elasticsearchContainer)) {
            elasticsearchContainer.close();
        }
        if (Objects.nonNull(mysqlContainer)) {
            mysqlContainer.close();
        }
    }

    // Executes incremental source changes with the application user.
    private void executeSql(String... statements) {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysqlContainer.getJdbcUrl(MYSQL_DATABASE),
                                MYSQL_USER_NAME,
                                MYSQL_USER_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute MySQL SQL", e);
        }
    }

    // Creates the CDC account used by the documented source config.
    private void createCdcUser() {
        executeAdminSql(
                "CREATE USER IF NOT EXISTS '"
                        + MYSQL_CDC_USER_NAME
                        + "'@'%' IDENTIFIED BY '"
                        + MYSQL_USER_PASSWORD
                        + "'",
                "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO '"
                        + MYSQL_CDC_USER_NAME
                        + "'@'%'");
    }

    // Executes MySQL account setup statements as the container administrator.
    private void executeAdminSql(String... statements) {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysqlContainer.getJdbcUrl(MYSQL_DATABASE),
                                MYSQL_ROOT_USER_NAME,
                                MYSQL_USER_PASSWORD);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute MySQL admin SQL", e);
        }
    }

    // Reads all indexed documents and maps them by the documented primary key.
    private Map<Integer, JsonNode> fetchIndexDocs() throws Exception {
        Container.ExecResult indexCountResult =
                elasticsearchContainer.execInContainer(
                        "bash",
                        "-c",
                        "curl -k -u elastic:elasticsearch -H \"Content-Type:application/json\" "
                                + "-d '{\"from\":0,\"size\":100,\"query\":{\"match_all\":{}}}' "
                                + "https://localhost:9200/"
                                + INDEX_NAME
                                + "/_search");
        Assertions.assertEquals(0, indexCountResult.getExitCode(), indexCountResult.getStderr());
        ObjectNode jsonNode = JsonUtils.parseObject(indexCountResult.getStdout());
        JsonNode hits = jsonNode.get("hits").get("hits");
        Map<Integer, JsonNode> docs = new HashMap<>();
        for (JsonNode hit : hits) {
            JsonNode source = hit.get("_source");
            docs.put(source.get("id").asInt(), source);
        }
        return docs;
    }

    // Surfaces an asynchronous SeaTunnel job failure while Awaitility polls sink results.
    private static void assertNoJobFailure(AtomicReference<Throwable> jobFailure) {
        if (jobFailure.get() != null) {
            Assertions.fail(jobFailure.get());
        }
    }
}
