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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * E2E test for Elasticsearch Runtime Fields feature (available in Elasticsearch 7.11+) Runtime
 * fields allow computing field values at query time without reindexing data
 */
@Slf4j
public class ElasticsearchRuntimeFieldsIT extends TestSuiteBase implements TestResource {

    private ElasticsearchContainer container;
    private EsRestClient esRestClient;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

        // Create test index with sample data
        createTestIndexWithData();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (esRestClient != null) {
            esRestClient.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    /** Create test index with sample data for runtime fields testing */
    private void createTestIndexWithData() throws IOException, InterruptedException {
        String indexName = "st_index_runtime";

        // Create index with explicit mapping for timestamp field
        String mapping =
                "{"
                        + "  \"mappings\": {"
                        + "    \"properties\": {"
                        + "      \"c_string\": { \"type\": \"keyword\" },"
                        + "      \"c_int\": { \"type\": \"integer\" },"
                        + "      \"c_timestamp\": { \"type\": \"date\" }"
                        + "    }"
                        + "  }"
                        + "}";
        esRestClient.createIndex(indexName, mapping);
        log.info("Created index with mapping: {}", indexName);

        // Prepare test data
        List<String> testData = generateTestData();

        // Bulk insert data
        StringBuilder bulkRequestBody = new StringBuilder();
        for (String doc : testData) {
            bulkRequestBody
                    .append("{\"index\":{\"_index\":\"")
                    .append(indexName)
                    .append("\"}}\n")
                    .append(doc)
                    .append("\n");
        }

        BulkResponse response = esRestClient.bulk(bulkRequestBody.toString());
        Assertions.assertFalse(response.isErrors(), "Bulk insert should not have errors");
        log.info("Inserted {} documents into index: {}", testData.size(), indexName);

        // Wait for index refresh
        Thread.sleep(2000);
    }

    /**
     * Generate test data with timestamp and numeric fields for runtime field computation Using
     * fixed dates for predictable runtime field results
     */
    private List<String> generateTestData() throws IOException {
        List<String> testData = new ArrayList<>();

        // Use a fixed date: 2024-01-15 (Monday) for predictable day_of_week
        Map<String, Object> doc = new HashMap<>();
        doc.put("c_string", "test_1");
        doc.put("c_int", 10);
        doc.put("c_timestamp", "2024-01-15T10:00:00");
        testData.add(OBJECT_MAPPER.writeValueAsString(doc));

        return testData;
    }

    /**
     * Test Elasticsearch source with runtime fields Runtime fields are computed at query time: -
     * day_of_week: extracts day of week from timestamp - c_int_doubled: doubles the c_int value -
     * full_name: concatenates c_string with '_computed'
     */
    @TestTemplate
    public void testElasticsearchSourceWithRuntimeFields(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/elasticsearch/elasticsearch_source_with_runtime_fields.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), "Job should complete successfully");

        log.info("Runtime fields test completed successfully");
        log.info("Job output: {}", execResult.getStdout());
    }
}
