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
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Runs the sink and every search API of the connector against an Elasticsearch 9 cluster. */
public class Elasticsearch9IT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "elasticsearch:9.5.5";
    private static final String INDEX = "st_index_es9";

    private ElasticsearchContainer container;
    private EsRestClient esRestClient;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        container =
                new ElasticsearchContainer(
                                DockerImageName.parse(IMAGE)
                                        .asCompatibleSubstituteFor(
                                                "docker.elastic.co/elasticsearch/elasticsearch"))
                        .withNetwork(NETWORK)
                        .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                        .withNetworkAliases("elasticsearch9")
                        .withPassword("elasticsearch")
                        .withStartupAttempts(5)
                        .withStartupTimeout(Duration.ofMinutes(5))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        Startables.deepStart(Stream.of(container)).join();

        Map<String, Object> configMap = new HashMap<>();
        configMap.put("hosts", Lists.newArrayList("https://" + container.getHttpHostAddress()));
        configMap.put("username", "elastic");
        configMap.put("password", "elasticsearch");
        configMap.put("tls_verify_certificate", false);
        configMap.put("tls_verify_hostname", false);
        esRestClient = EsRestClient.createInstance(ReadonlyConfig.fromMap(configMap));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (esRestClient != null) {
            esRestClient.close();
        }
        if (container != null) {
            container.close();
        }
    }

    @TestTemplate
    public void testSinkAndSearchApis(TestContainer testContainer) throws Exception {
        // Start each engine without the index, so the documents checked below come from this run.
        if (esRestClient.checkIndexExist(INDEX)) {
            esRestClient.dropIndex(INDEX);
        }
        assertJobSucceeds(testContainer, "/elasticsearch/fakesource_to_elasticsearch9.conf");

        // The sink applied the insert, update and delete rows by primary key. Poll, because
        // the written documents become searchable only after the next index refresh.
        Map<String, Object> expected = new TreeMap<>();
        expected.put("1", "a");
        expected.put("2", "b2");
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, readNamesById()));

        assertJobSucceeds(testContainer, "/elasticsearch/elasticsearch9_source_with_scroll.conf");
        assertJobSucceeds(testContainer, "/elasticsearch/elasticsearch9_source_with_pit.conf");
        assertJobSucceeds(testContainer, "/elasticsearch/elasticsearch9_source_with_sql.conf");
    }

    private Map<String, Object> readNamesById() {
        Map<String, Object> query = new HashMap<>();
        query.put("match_all", Maps.newHashMap());
        ScrollResult result =
                esRestClient.searchByScroll(INDEX, Arrays.asList("id", "name"), query, "1m", 100);
        Map<String, Object> names = new TreeMap<>();
        result.getDocs().forEach(doc -> names.put(doc.get("_id").toString(), doc.get("name")));
        return names;
    }

    private void assertJobSucceeds(TestContainer testContainer, String config) throws Exception {
        Container.ExecResult execResult = testContainer.executeJob(config);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
