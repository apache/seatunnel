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
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.JsonUtils;
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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public class ElasticsearchSchemaChangeIT extends TestSuiteBase implements TestResource {

    private ElasticsearchContainer container;

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);
    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";
    private static final String DATABASE = "shop";
    protected static final String DRIVER_JAR =
            "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    private final UniqueDatabase shopDatabase = new UniqueDatabase(MYSQL_CONTAINER, DATABASE);

    private EsRestClient esRestClient;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        container =
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
        Startables.deepStart(Stream.of(container)).join();
        log.info("Elasticsearch container started");
        // Create configuration for EsRestClient
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("hosts", Lists.newArrayList("https://" + container.getHttpHostAddress()));
        configMap.put("username", "elastic");
        configMap.put("password", "elasticsearch");
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        esRestClient = EsRestClient.createInstance(config);

        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        shopDatabase.createAndInitialize();
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));
        mySqlContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", 3306, 3306)));
        return mySqlContainer;
    }

    @TestTemplate
    public void testSchemaChange(TestContainer container) throws InterruptedException {

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        String jobConfigFile = "/elasticsearch/mysqlcdc_to_elasticsearch_with_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile, jobId);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        TimeUnit.SECONDS.sleep(20);
        shopDatabase.setTemplateName("add_columns").createAndInitialize();

        await().atMost(120, TimeUnit.SECONDS)
                .pollInterval(3, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            Container.ExecResult execResult =
                                    this.container.execInContainer(
                                            "bash",
                                            "-c",
                                            "curl -k -u elastic:elasticsearch https://localhost:9200/schema_change_index/_mapping");
                            ObjectNode jsonNodes = JsonUtils.parseObject(execResult.getStdout());
                            JsonNode schemaChangeIndex =
                                    jsonNodes
                                            .get("schema_change_index")
                                            .get("mappings")
                                            .get("properties");
                            Assertions.assertEquals(
                                    schemaChangeIndex.get("add_column1").get("type").asText(),
                                    "text");
                            Assertions.assertEquals(
                                    schemaChangeIndex.get("add_column2").get("type").asText(),
                                    "integer");
                            Assertions.assertEquals(
                                    schemaChangeIndex.get("add_column3").get("type").asText(),
                                    "float");
                            Assertions.assertEquals(
                                    schemaChangeIndex.get("add_column4").get("type").asText(),
                                    "date");
                            Container.ExecResult indexCountResult =
                                    this.container.execInContainer(
                                            "bash",
                                            "-c",
                                            "curl -k -u elastic:elasticsearch -H \"Content-Type:application/json\" -d '{ \"from\": 0, \"size\": 10000, \"query\": { \"match_all\": {}}}' https://localhost:9200/schema_change_index/_search");
                            log.info("indexCountResult: {}", indexCountResult.getStdout());
                            ObjectNode jsonNode =
                                    JsonUtils.parseObject(indexCountResult.getStdout());
                            JsonNode hits = jsonNode.get("hits");
                            long totalCount = hits.get("total").get("value").asLong();
                            Assertions.assertEquals(18L, totalCount);

                            hits.get("hits")
                                    .forEach(
                                            hit -> {
                                                JsonNode source = hit.get("_source");
                                                int id = source.get("id").asInt();
                                                if (id >= 119 && id <= 127) {
                                                    Assertions.assertTrue(
                                                            source.has("add_column1"));
                                                    Assertions.assertFalse(
                                                            source.get("add_column1").isNull());
                                                    Assertions.assertTrue(
                                                            source.has("add_column2"));
                                                    Assertions.assertFalse(
                                                            source.get("add_column2").isNull());
                                                }
                                            });
                        });
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(esRestClient)) {
            esRestClient.close();
        }
        container.close();
    }
}
