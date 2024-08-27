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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchCatalog;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.io.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ElasticsearchIT extends TestSuiteBase implements TestResource {

    private List<String> testDataset;

    private ElasticsearchContainer container;

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
        esRestClient =
                EsRestClient.createInstance(
                        Lists.newArrayList("https://" + container.getHttpHostAddress()),
                        Optional.of("elastic"),
                        Optional.of("elasticsearch"),
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty());
        testDataset = generateTestDataSet();
        createIndexDocs();
        createIndexWithFullType();
        createIndexForResourceNull();
    }

    /** create a index,and bulk some documents */
    private void createIndexDocs() {
        StringBuilder requestBody = new StringBuilder();
        Map<String, String> indexInner = new HashMap<>();
        indexInner.put("_index", "st");

        Map<String, Map<String, String>> indexParam = new HashMap<>();
        indexParam.put("index", indexInner);
        String indexHeader = "{\"index\":{\"_index\":\"st_index\"}\n";
        for (int i = 0; i < testDataset.size(); i++) {
            String row = testDataset.get(i);
            requestBody.append(indexHeader);
            requestBody.append(row);
            requestBody.append("\n");
        }
        esRestClient.bulk(requestBody.toString());
    }

    private void createIndexWithFullType() throws IOException, InterruptedException {
        String mapping =
                IOUtils.toString(
                        ContainerUtil.getResourcesFile(
                                        "/elasticsearch/st_index_full_type_mapping.json")
                                .toURI(),
                        StandardCharsets.UTF_8);
        esRestClient.createIndex("st_index_full_type", mapping);
        BulkResponse response =
                esRestClient.bulk(
                        "{ \"index\" : { \"_index\" : \"st_index_full_type\", \"_id\" : \"1\" } }\n"
                                + IOUtils.toString(
                                                ContainerUtil.getResourcesFile(
                                                                "/elasticsearch/st_index_full_type_data.json")
                                                        .toURI(),
                                                StandardCharsets.UTF_8)
                                        .replace("\n", "")
                                + "\n");
        Assertions.assertFalse(response.isErrors(), response.getResponse());
        // waiting index refresh
        Thread.sleep(2000L);
        Assertions.assertEquals(
                2, esRestClient.getIndexDocsCount("st_index_full_type").get(0).getDocsCount());
    }

    private void createIndexForResourceNull() throws IOException {
        String mapping =
                IOUtils.toString(
                        ContainerUtil.getResourcesFile(
                                        "/elasticsearch/st_index_source_without_schema_and_sink.json")
                                .toURI(),
                        StandardCharsets.UTF_8);
        esRestClient.createIndex("st_index4", mapping);
    }

    @TestTemplate
    public void testElasticsearch(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> sinkData = readSinkData("st_index2");
        // for DSL is: {"range":{"c_int":{"gte":10,"lte":20}}}
        Assertions.assertIterableEquals(mapTestDatasetForDSL(), sinkData);
    }

    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK do not support multiple table read")
    @TestTemplate
    public void testElasticsearchWithMultiSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/fakesource_to_elasticsearch_multi_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> source5 =
                Lists.newArrayList(
                        "id",
                        "c_bool",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal",
                        "c_string");
        List<String> source6 =
                Lists.newArrayList(
                        "id",
                        "c_bool",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal");
        List<String> sinkIndexData5 = readMultiSinkData("st_index5", source5);
        List<String> sinkIndexData6 = readMultiSinkData("st_index6", source6);
        String stIndex5 =
                "{\"c_smallint\":2,\"c_string\":\"NEW\",\"c_float\":4.3,\"c_double\":5.3,\"c_decimal\":6.3,\"id\":1,\"c_int\":3,\"c_bigint\":4,\"c_bool\":true,\"c_tinyint\":1}";
        String stIndex6 =
                "{\"c_smallint\":2,\"c_float\":4.3,\"c_double\":5.3,\"c_decimal\":6.3,\"id\":1,\"c_int\":3,\"c_bigint\":4,\"c_bool\":true,\"c_tinyint\":1}";
        Assertions.assertIterableEquals(Lists.newArrayList(stIndex5), sinkIndexData5);
        Assertions.assertIterableEquals(Lists.newArrayList(stIndex6), sinkIndexData6);
    }

    @TestTemplate
    public void testElasticsearchWithFullType(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink_full_type.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Thread.sleep(2000L);
        Assertions.assertEquals(
                1,
                esRestClient.getIndexDocsCount("st_index_full_type_target").get(0).getDocsCount());
    }

    @TestTemplate
    public void testElasticsearchWithoutSchema(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult =
                container.executeJob(
                        "/elasticsearch/elasticsearch_source_without_schema_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> sinkData = readSinkDataWithOutSchema();
        // for DSL is: {"range":{"c_int":{"gte":10,"lte":20}}}
        Assertions.assertIterableEquals(mapTestDatasetForDSL(), sinkData);
    }

    private List<String> generateTestDataSet() throws JsonProcessingException {
        String[] fields =
                new String[] {
                    "c_map",
                    "c_array",
                    "c_string",
                    "c_boolean",
                    "c_tinyint",
                    "c_smallint",
                    "c_bigint",
                    "c_float",
                    "c_double",
                    "c_decimal",
                    "c_bytes",
                    "c_int",
                    "c_date",
                    "c_timestamp"
                };

        List<String> documents = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> doc = new HashMap<>();
            Object[] values =
                    new Object[] {
                        Collections.singletonMap("key", Short.parseShort(String.valueOf(i))),
                        new Byte[] {Byte.parseByte("1"), Byte.parseByte("2"), Byte.parseByte("3")},
                        "string",
                        Boolean.FALSE,
                        Byte.parseByte("1"),
                        Short.parseShort("1"),
                        i,
                        Long.parseLong("1"),
                        Float.parseFloat("1.1"),
                        Double.parseDouble("1.1"),
                        BigDecimal.valueOf(11, 1),
                        "test".getBytes(),
                        LocalDate.now().toString(),
                        System.currentTimeMillis()
                    };
            for (int j = 0; j < fields.length; j++) {
                doc.put(fields[j], values[j]);
            }
            documents.add(objectMapper.writeValueAsString(doc));
        }
        return documents;
    }

    private List<String> readSinkDataWithOutSchema() throws InterruptedException {
        Map<String, BasicTypeDefine<EsType>> esFieldType =
                esRestClient.getFieldTypeMapping("st_index4", Lists.newArrayList());
        Thread.sleep(2000);
        List<String> source = new ArrayList<>(esFieldType.keySet());
        return getDocsWithTransformDate(source, "st_index4");
    }

    private List<String> readSinkData(String index) throws InterruptedException {
        // wait for index refresh
        Thread.sleep(2000);
        List<String> source =
                Lists.newArrayList(
                        "c_map",
                        "c_array",
                        "c_string",
                        "c_boolean",
                        "c_tinyint",
                        "c_smallint",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal",
                        "c_bytes",
                        "c_int",
                        "c_date",
                        "c_timestamp");
        return getDocsWithTransformTimestamp(source, index);
    }

    private List<String> readMultiSinkData(String index, List<String> source)
            throws InterruptedException {
        // wait for index refresh
        Thread.sleep(2000);
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", Maps.newHashMap());

        ScrollResult scrollResult = esRestClient.searchByScroll(index, source, query, "1m", 1000);
        scrollResult
                .getDocs()
                .forEach(
                        x -> {
                            x.remove("_index");
                            x.remove("_type");
                            x.remove("_id");
                        });
        List<String> docs =
                scrollResult.getDocs().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        o -> Integer.valueOf(o.get("c_int").toString())))
                        .map(JsonUtils::toJsonString)
                        .collect(Collectors.toList());
        return docs;
    }

    private List<String> getDocsWithTransformTimestamp(List<String> source, String index) {
        HashMap<String, Object> rangeParam = new HashMap<>();
        rangeParam.put("gte", 10);
        rangeParam.put("lte", 20);
        HashMap<String, Object> range = new HashMap<>();
        range.put("c_int", rangeParam);
        Map<String, Object> query = new HashMap<>();
        query.put("range", range);
        ScrollResult scrollResult = esRestClient.searchByScroll(index, source, query, "1m", 1000);
        scrollResult
                .getDocs()
                .forEach(
                        x -> {
                            x.remove("_index");
                            x.remove("_type");
                            x.remove("_id");
                            x.replace(
                                    "c_timestamp",
                                    LocalDateTime.parse(x.get("c_timestamp").toString())
                                            .toInstant(ZoneOffset.UTC)
                                            .toEpochMilli());
                        });
        List<String> docs =
                scrollResult.getDocs().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        o -> Integer.valueOf(o.get("c_int").toString())))
                        .map(JsonUtils::toJsonString)
                        .collect(Collectors.toList());
        return docs;
    }

    private List<String> getDocsWithTransformDate(List<String> source, String index) {
        HashMap<String, Object> rangeParam = new HashMap<>();
        rangeParam.put("gte", 10);
        rangeParam.put("lte", 20);
        HashMap<String, Object> range = new HashMap<>();
        range.put("c_int", rangeParam);
        Map<String, Object> query = new HashMap<>();
        query.put("range", range);
        ScrollResult scrollResult = esRestClient.searchByScroll(index, source, query, "1m", 1000);
        scrollResult
                .getDocs()
                .forEach(
                        x -> {
                            x.remove("_index");
                            x.remove("_type");
                            x.remove("_id");
                            x.replace(
                                    "c_date",
                                    LocalDate.parse(
                                                    x.get("c_date").toString(),
                                                    DateTimeFormatter.ofPattern(
                                                            "yyyy-MM-dd'T'HH:mm"))
                                            .toString());
                        });
        List<String> docs =
                scrollResult.getDocs().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        o -> Integer.valueOf(o.get("c_int").toString())))
                        .map(JsonUtils::toJsonString)
                        .collect(Collectors.toList());
        return docs;
    }

    private List<String> mapTestDatasetForDSL() {
        return testDataset.stream()
                .map(JsonUtils::parseObject)
                .filter(
                        node -> {
                            if (node.hasNonNull("c_int")) {
                                int cInt = node.get("c_int").asInt();
                                return cInt >= 10 && cInt <= 20;
                            }
                            return false;
                        })
                .map(JsonNode::toString)
                .collect(Collectors.toList());
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(esRestClient)) {
            esRestClient.close();
        }
        container.close();
    }

    @Test
    public void testCatalog() throws InterruptedException, JsonProcessingException {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("username", "elastic");
        configMap.put("password", "elasticsearch");
        configMap.put(
                "hosts", Collections.singletonList("https://" + container.getHttpHostAddress()));
        configMap.put("index", "st_index3");
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);
        configMap.put("index_type", "st");

        final ElasticSearchCatalog elasticSearchCatalog =
                new ElasticSearchCatalog("Elasticsearch", "", ReadonlyConfig.fromMap(configMap));
        elasticSearchCatalog.open();

        TablePath tablePath = TablePath.of("", "st_index3");

        // Verify index does not exist initially
        final boolean existsBefore = elasticSearchCatalog.tableExists(tablePath);
        Assertions.assertFalse(existsBefore, "Index should not exist initially");

        // Create index
        elasticSearchCatalog.createTable(tablePath, null, false);
        final boolean existsAfter = elasticSearchCatalog.tableExists(tablePath);
        Assertions.assertTrue(existsAfter, "Index should be created");

        // Generate and add multiple records
        List<String> data = generateTestData();
        StringBuilder requestBody = new StringBuilder();
        String indexHeader = "{\"index\":{\"_index\":\"st_index3\"}}\n";
        for (String record : data) {
            requestBody.append(indexHeader);
            requestBody.append(record);
            requestBody.append("\n");
        }
        esRestClient.bulk(requestBody.toString());
        Thread.sleep(2000); // Wait for data to be indexed

        // Verify data exists
        List<String> sourceFields = Arrays.asList("field1", "field2");
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
        ScrollResult scrollResult =
                esRestClient.searchByScroll("st_index3", sourceFields, query, "1m", 100);
        Assertions.assertFalse(scrollResult.getDocs().isEmpty(), "Data should exist in the index");

        // Truncate the table
        elasticSearchCatalog.truncateTable(tablePath, false);
        Thread.sleep(2000); // Wait for data to be indexed

        // Verify data is deleted
        scrollResult = esRestClient.searchByScroll("st_index3", sourceFields, query, "1m", 100);
        Assertions.assertTrue(
                scrollResult.getDocs().isEmpty(), "Data should be deleted from the index");

        // Drop the table
        elasticSearchCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(
                elasticSearchCatalog.tableExists(tablePath), "Index should be dropped");

        elasticSearchCatalog.close();
    }

    private List<String> generateTestData() throws JsonProcessingException {
        List<String> data = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("field1", "value" + i);
            record.put("field2", i);
            data.add(objectMapper.writeValueAsString(record));
        }
        return data;
    }
}
