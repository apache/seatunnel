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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog.ElasticSearchCatalog;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
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

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FakeToElasticsearchIT extends TestSuiteBase implements TestResource {

    private ElasticsearchContainer container;

    private EsRestClient esRestClient;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        container =
                new ElasticsearchContainer(
                                DockerImageName.parse("elasticsearch:8.0.0")
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
                                        DockerLoggerFactory.getLogger("elasticsearch:8.0.0")));
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

    }

    @TestTemplate
    public void testFakeToElasticsearch(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/elasticsearch/fake_to_elasticsearch.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        List<String> sinkData = readSinkData();
        // old es value: c_json_string : "{\"key\":\"value\"}"
        // new es value: c_json_string : {"key":"value"}
        System.out.println(sinkData.toString());
    }

    private List<String> readSinkData() throws InterruptedException {
        // wait for index refresh
        Thread.sleep(2000);
        List<String> source =
                Lists.newArrayList(
                        "c_map",
                        "c_array",
                        "c_json_string",
                        "c_boolean",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_decimal",
                        "c_bytes",
                        "c_date",
                        "c_timestamp");
        Map<String, Object> query = new HashMap<>();
        HashMap<String, Object> rangeParam = new HashMap<>();
        rangeParam.put("gte", 0);
        rangeParam.put("lte", 20);
        HashMap<String, Object> range = new HashMap<>();
        range.put("c_int", rangeParam);
        query.put("range", range);
        ScrollResult scrollResult =
                esRestClient.searchByScroll("st_index2", source, query, "1m", 1000);
        scrollResult
                .getDocs()
                .forEach(
                        x -> {
                            x.remove("_index");
                            x.remove("_type");
                            x.remove("_id");
                            // I don’t know if converting the test cases in this way complies with
                            // the CI specification
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


    @AfterEach
    @Override
    public void tearDown() {
        if (Objects.nonNull(esRestClient)) {
            esRestClient.close();
        }
        container.close();
    }

}
