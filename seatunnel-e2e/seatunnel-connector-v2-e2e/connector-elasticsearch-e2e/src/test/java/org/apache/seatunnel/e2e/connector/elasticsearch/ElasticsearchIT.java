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
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchCatalog;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.io.IOUtils;

import org.awaitility.Awaitility;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ElasticsearchIT extends TestSuiteBase implements TestResource {

    private static final long INDEX_REFRESH_MILL_DELAY = 5000L;

    private List<String> testDataset1;

    private List<String> testDataset2;

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
        testDataset1 = generateTestDataSet1();
        testDataset2 = generateTestDataSet2();
        createIndexForResourceNull("st_index");
        createIndexDocs();
        createIndexWithFullType();
        createIndexForResourceNull("st_index4");
        createIndexWithNestType();
    }

    /** create a index,and bulk some documents */
    private void createIndexDocs() {
        createIndexDocsByName("st_index");
    }

    private void createIndexDocsByName(String indexName) {
        createIndexDocsByName(indexName, testDataset1);
    }

    private void createIndexDocsByName(String indexName, List<String> testDataSet) {
        StringBuilder requestBody = new StringBuilder();
        String indexHeader = String.format("{\"index\":{\"_index\":\"%s\"}\n", indexName);
        for (int i = 0; i < testDataSet.size(); i++) {
            String row = testDataSet.get(i);
            requestBody.append(indexHeader);
            requestBody.append(row);
            requestBody.append("\n");
        }
        esRestClient.bulk(requestBody.toString());
    }

    private void createIndexWithNestType() throws IOException, InterruptedException {
        String mapping =
                IOUtils.toString(
                        ContainerUtil.getResourcesFile("/elasticsearch/st_index_nest_mapping.json")
                                .toURI(),
                        StandardCharsets.UTF_8);
        esRestClient.createIndex("st_index_nest", mapping);
        esRestClient.createIndex("st_index_nest_copy", mapping);
        BulkResponse response =
                esRestClient.bulk(
                        "{ \"index\" : { \"_index\" : \"st_index_nest\", \"_id\" : \"1\" } }\n"
                                + IOUtils.toString(
                                                ContainerUtil.getResourcesFile(
                                                                "/elasticsearch/st_index_nest_data.json")
                                                        .toURI(),
                                                StandardCharsets.UTF_8)
                                        .replace("\n", "")
                                + "\n");
        Assertions.assertFalse(response.isErrors(), response.getResponse());
        // waiting index refresh
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        Assertions.assertEquals(
                3, esRestClient.getIndexDocsCount("st_index_nest").get(0).getDocsCount());
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
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        Assertions.assertEquals(
                2, esRestClient.getIndexDocsCount("st_index_full_type").get(0).getDocsCount());
    }

    private void createIndexForResourceNull(String indexName) throws IOException {
        String mapping =
                IOUtils.toString(
                        ContainerUtil.getResourcesFile(
                                        "/elasticsearch/st_index_source_without_schema_and_sink.json")
                                .toURI(),
                        StandardCharsets.UTF_8);
        esRestClient.createIndex(indexName, mapping);
    }

    @TestTemplate
    public void testElasticsearchWithSchema(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> sinkData = readSinkDataWithSchema("st_index2");
        // for DSL is: {"range":{"c_int":{"gte":10,"lte":20}}}
        Assertions.assertIterableEquals(mapTestDatasetForDSL(), sinkData);
    }

    @TestTemplate
    public void testElasticsearchWithNestSchema(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/elasticsearch_source_and_sink_with_nest.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        List<String> sinkData = readSinkDataWithNestSchema("st_index_nest_copy");
        String data =
                "{\"address\":[{\"zipcode\":\"10001\",\"city\":\"New York\",\"street\":\"123 Main St\"},"
                        + "{\"zipcode\":\"90001\",\"city\":\"Los Angeles\",\"street\":\"456 Elm St\"}],\"name\":\"John Doe\"}";

        Assertions.assertIterableEquals(Lists.newArrayList(data), sinkData);
    }

    @TestTemplate
    public void testElasticsSearchWithMultiSourceByFilter(TestContainer container)
            throws InterruptedException, IOException {
        // read read_filter_index1,read_filter_index2
        // write into read_filter_index1_copy,read_filter_index2_copy
        createIndexDocsByName("read_filter_index1", testDataset1);
        createIndexDocsByName("read_filter_index2", testDataset2);

        Container.ExecResult execResult =
                container.executeJob(
                        "/elasticsearch/elasticsearch_multi_source_and_sink_by_filter.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        HashMap<String, Object> rangeParam = new HashMap<>();
        rangeParam.put("gte", 10);
        rangeParam.put("lte", 20);
        HashMap<String, Object> range1 = new HashMap<>();
        range1.put("c_int", rangeParam);
        Map<String, Object> query1 = new HashMap<>();
        query1.put("range", range1);

        Map<String, Object> query2 = new HashMap<>();
        HashMap<String, Object> range2 = new HashMap<>();
        range2.put("c_int2", rangeParam);
        query2.put("range", range2);

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(INDEX_REFRESH_MILL_DELAY));
        Set<String> sinkData1 =
                new HashSet<>(
                        getDocsWithTransformDate(
                                // read all field
                                Collections.emptyList(),
                                // read indexName
                                "read_filter_index1_copy",
                                // allowed c_null serialized if null
                                Lists.newArrayList("c_null"),
                                // query condition
                                query1,
                                // transformDate field:c_date
                                Lists.newArrayList("c_date"),
                                // order field
                                "c_int"));

        List<String> index1Data =
                mapTestDatasetForDSL(
                        // use testDataset1
                        testDataset1,
                        // filter testDataset1 match sinkData1
                        doc -> {
                            if (doc.has("c_int")) {
                                int cInt = doc.get("c_int").asInt();
                                return cInt >= 10 && cInt <= 20;
                            }
                            return false;
                        },
                        // mapping document all field to string
                        JsonNode::toString);
        Assertions.assertEquals(sinkData1.size(), index1Data.size());
        index1Data.forEach(sinkData1::remove);
        // data is completely consistent, and the size is zero after deletion
        Assertions.assertEquals(0, sinkData1.size());

        List<String> index2Data =
                mapTestDatasetForDSL(
                        testDataset2,
                        // use customer predicate filter data to match sinkData2
                        doc -> {
                            if (doc.has("c_int2")) {
                                int cInt = doc.get("c_int2").asInt();
                                return cInt >= 10 && cInt <= 20;
                            }
                            return false;
                        },
                        // mapping doc to string,keep only three fields
                        doc -> {
                            Map<String, Object> map = new HashMap<>();
                            map.put("c_int2", doc.get("c_int2"));
                            map.put("c_null2", doc.get("c_null2"));
                            map.put("c_date2", doc.get("c_date2"));
                            return JsonUtils.toJsonString(map);
                        });

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(INDEX_REFRESH_MILL_DELAY));
        Set<String> sinkData2 =
                new HashSet<>(
                        getDocsWithTransformDate(
                                // read three fields from index
                                Lists.newArrayList("c_int2", "c_null2", "c_date2"),
                                "read_filter_index2_copy",
                                //// allowed c_null serialized if null
                                Lists.newArrayList("c_null2"),
                                query2,
                                // // transformDate field:c_date2
                                Lists.newArrayList("c_date2"),
                                // order by c_int2
                                "c_int2"));
        Assertions.assertEquals(sinkData2.size(), index2Data.size());
        index2Data.forEach(sinkData2::remove);
        Assertions.assertEquals(0, sinkData2.size());
    }

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
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        Assertions.assertEquals(
                1,
                esRestClient.getIndexDocsCount("st_index_full_type_target").get(0).getDocsCount());
    }

    @TestTemplate
    public void testFakeSourceToElasticsearchWithUpperCaseIndex(TestContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Container.ExecResult execResult =
                                container.executeJob(
                                        "/elasticsearch/fakesource_to_elasticsearch_with_upper_case_index.conf");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .ignoreExceptions()
                .pollInterval(3, TimeUnit.SECONDS)
                .pollDelay(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    20,
                                    esRestClient
                                            .getIndexDocsCount("st_fake_table")
                                            .get(0)
                                            .getDocsCount());
                        });
    }

    @TestTemplate
    public void testElasticsearchWithoutSchema(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult =
                container.executeJob(
                        "/elasticsearch/elasticsearch_source_without_schema_and_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> sinkData = readSinkDataWithOutSchema("st_index4");
        // for DSL is: {"range":{"c_int":{"gte":10,"lte":20}}}
        Assertions.assertIterableEquals(mapTestDatasetForDSL(), sinkData);
    }

    private List<String> generateTestDataSet1() throws JsonProcessingException {
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
                    "c_timestamp",
                    "c_null"
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
                        Long.parseLong("1"),
                        Float.parseFloat("1.1"),
                        Double.parseDouble("1.1"),
                        BigDecimal.valueOf(11, 1),
                        "test".getBytes(),
                        i,
                        LocalDate.now().toString(),
                        System.currentTimeMillis(),
                        // Null values are also a basic use case for testing
                        null
                    };
            for (int j = 0; j < fields.length; j++) {
                doc.put(fields[j], values[j]);
            }
            documents.add(objectMapper.writeValueAsString(doc));
        }
        return documents;
    }

    private List<String> generateTestDataSet2() throws JsonProcessingException {
        String[] fields =
                new String[] {
                    "c_map2",
                    "c_array2",
                    "c_string2",
                    "c_boolean2",
                    "c_tinyint2",
                    "c_smallint2",
                    "c_bigint2",
                    "c_float2",
                    "c_double2",
                    "c_decimal2",
                    "c_bytes2",
                    "c_int2",
                    "c_date2",
                    "c_timestamp2",
                    "c_null2"
                };

        List<String> documents = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (int i = 0; i < 100; i++) {
            Map<String, Object> doc = new HashMap<>();
            Object[] values =
                    new Object[] {
                        Collections.singletonMap("key2", Short.parseShort(String.valueOf(i))),
                        new Byte[] {
                            Byte.parseByte("11"), Byte.parseByte("22"), Byte.parseByte("33")
                        },
                        "string2",
                        Boolean.FALSE,
                        Byte.parseByte("2"),
                        Short.parseShort("2"),
                        Long.parseLong("2"),
                        Float.parseFloat("2.2"),
                        Double.parseDouble("2.2"),
                        BigDecimal.valueOf(22, 1),
                        "test2".getBytes(),
                        i,
                        LocalDate.now().toString(),
                        System.currentTimeMillis(),
                        // Null values are also a basic use case for testing
                        null
                    };
            for (int j = 0; j < fields.length; j++) {
                doc.put(fields[j], values[j]);
            }
            documents.add(objectMapper.writeValueAsString(doc));
        }
        return documents;
    }

    private List<String> readSinkDataWithOutSchema(String indexName) throws InterruptedException {
        Map<String, BasicTypeDefine<EsType>> esFieldType =
                esRestClient.getFieldTypeMapping(indexName, Lists.newArrayList());
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        List<String> source = new ArrayList<>(esFieldType.keySet());
        return getDocsWithTransformDate(source, indexName);
    }

    // Null values are also a basic use case for testing
    // To ensure consistency in comparisons, we need to explicitly serialize null values.
    private List<String> readSinkDataWithOutSchema(String indexName, List<String> nullAllowedFields)
            throws InterruptedException {
        Map<String, BasicTypeDefine<EsType>> esFieldType =
                esRestClient.getFieldTypeMapping(indexName, Lists.newArrayList());
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        List<String> source = new ArrayList<>(esFieldType.keySet());
        return getDocsWithTransformDate(source, indexName, nullAllowedFields);
    }

    // The timestamp type in Elasticsearch is incompatible with that in Seatunnel,
    // and we need to handle the conversion here.
    private List<String> readSinkDataWithSchema(String index) throws InterruptedException {
        // wait for index refresh
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
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
                        "c_timestamp",
                        "c_null");
        return getDocsWithTransformTimestamp(source, index);
    }

    private List<String> readSinkDataWithNestSchema(String index) throws InterruptedException {
        // wait for index refresh
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        List<String> source = Lists.newArrayList("name", "address");
        return getDocsWithNestType(source, index);
    }

    private List<String> readMultiSinkData(String index, List<String> source)
            throws InterruptedException {
        // wait for index refresh
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
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

    private List<String> getDocsWithNestType(List<String> source, String index) {
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
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
                        .map(JsonUtils::toJsonString)
                        .collect(Collectors.toList());
        return docs;
    }

    private List<String> getDocsWithTransformDate(List<String> source, String index) {
        return getDocsWithTransformDate(source, index, Collections.emptyList());
    }

    /**
     * use default query: c_int >= 10 and c_int <=20
     *
     * @param source The field to be read
     * @param index indexName
     * @param nullAllowedFields If the value of the field is null, it will be serialized to 'null'
     * @return serialized data as jsonString
     */
    private List<String> getDocsWithTransformDate(
            List<String> source, String index, List<String> nullAllowedFields) {
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
                            for (String field : nullAllowedFields) {
                                if (!x.containsKey(field)) {
                                    x.put(field, null);
                                }
                            }
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

    /**
     * use customer query read data
     *
     * @param source The field to be read
     * @param index read index
     * @param nullAllowedFields If the value of the field is null, it will be serialized to 'null'
     * @param query dls query
     * @param dateFields dateField will format with yyyy-MM-dd'T'HH:mm
     * @param orderField how to oder data
     * @return serialized data as jsonString
     */
    private List<String> getDocsWithTransformDate(
            List<String> source,
            String index,
            List<String> nullAllowedFields,
            Map<String, Object> query,
            List<String> dateFields,
            String orderField) {
        ScrollResult scrollResult = esRestClient.searchByScroll(index, source, query, "1m", 1000);
        scrollResult
                .getDocs()
                .forEach(
                        x -> {
                            x.remove("_index");
                            x.remove("_type");
                            x.remove("_id");
                            for (String field : nullAllowedFields) {
                                if (!x.containsKey(field)) {
                                    x.put(field, null);
                                }
                            }
                            for (String dateField : dateFields) {
                                if (x.containsKey(dateField)) {
                                    x.replace(
                                            dateField,
                                            LocalDate.parse(
                                                            x.get(dateField).toString(),
                                                            DateTimeFormatter.ofPattern(
                                                                    "yyyy-MM-dd'T'HH:mm"))
                                                    .toString());
                                }
                            }
                        });
        List<String> docs =
                scrollResult.getDocs().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        o -> Integer.parseInt(o.get(orderField).toString())))
                        .map(JsonUtils::toJsonString)
                        .collect(Collectors.toList());
        return docs;
    }

    /**
     * default testDataset1
     *
     * @return testDataset1 as jsonString array
     */
    private List<String> mapTestDatasetForDSL() {
        return mapTestDatasetForDSL(testDataset1);
    }

    /**
     * default query filter,c_int >=10 and c_int <= 20
     *
     * @param testDataset testDataset
     * @return c_int >=10 and c_int <= 20 filtered data
     */
    private List<String> mapTestDatasetForDSL(List<String> testDataset) {
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

    private List<String> mapTestDatasetForNest(List<String> testDataset) {
        return testDataset.stream()
                .map(JsonUtils::parseObject)
                .map(JsonNode::toString)
                .collect(Collectors.toList());
    }

    /**
     * Use custom filtering criteria to query data
     *
     * @param testDataset testDataset
     * @param predicate customer query filter
     * @param mapStrFunc mapping doc to string
     * @return filtered data
     */
    private List<String> mapTestDatasetForDSL(
            List<String> testDataset,
            Predicate<ObjectNode> predicate,
            Function<ObjectNode, String> mapStrFunc) {
        return testDataset.stream()
                .map(JsonUtils::parseObject)
                .filter(predicate)
                .map(mapStrFunc)
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
        Thread.sleep(INDEX_REFRESH_MILL_DELAY); // Wait for data to be indexed

        // Verify data exists
        List<String> sourceFields = Arrays.asList("field1", "field2");
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", new HashMap<>());
        ScrollResult scrollResult =
                esRestClient.searchByScroll("st_index3", sourceFields, query, "1m", 100);
        Assertions.assertFalse(scrollResult.getDocs().isEmpty(), "Data should exist in the index");

        // Truncate the table
        elasticSearchCatalog.truncateTable(tablePath, false);
        Thread.sleep(INDEX_REFRESH_MILL_DELAY); // Wait for data to be indexed

        // Verify data is deleted
        scrollResult = esRestClient.searchByScroll("st_index3", sourceFields, query, "1m", 100);
        Assertions.assertTrue(
                scrollResult.getDocs().isEmpty(), "Data should be deleted from the index");

        // Drop the table
        elasticSearchCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(
                elasticSearchCatalog.tableExists(tablePath), "Index should be dropped");

        // st_index always exist
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> elasticSearchCatalog.createDatabase(TablePath.of("", "st_index"), false));
        Assertions.assertDoesNotThrow(
                () -> elasticSearchCatalog.createDatabase(TablePath.of("", "st_index"), true));

        // create index
        Assertions.assertDoesNotThrow(
                () -> elasticSearchCatalog.createTable(TablePath.of("", "tmp_index"), null, false));
        Assertions.assertDoesNotThrow(
                () -> elasticSearchCatalog.dropDatabase(TablePath.of("", "tmp_index"), false));
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> elasticSearchCatalog.dropDatabase(TablePath.of("", "tmp_index"), false));

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

    /**
     * elastic query all dsl
     *
     * @return elastic query all dsl
     */
    private Map<String, Object> queryAll() {
        //  "query": {
        //    "match_all": {}
        //  }
        Map<String, Object> matchAll = new HashMap<>();
        matchAll.put("match_all", new HashMap<>());
        return matchAll;
    }
}
