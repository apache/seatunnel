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

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class ElasticSearchIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.17.6";
    private static final String HOST = "elasticsearch-e2e";
    private static final int PORT = 9200;
    private RestClient restClient;
    private ObjectMapper objectMapper;

    private ElasticsearchContainer elasticsearchContainer;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.elasticsearchContainer = new ElasticsearchContainer(DockerImageName.parse(IMAGE))
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withExposedPorts(PORT)
                .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                .waitingFor(new HostPortWaitStrategy().withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(elasticsearchContainer)).join();
        log.info("Elasticsearch container started");

        List<String> hosts = Arrays.asList(elasticsearchContainer.getHttpHostAddress());
        this.restClient = EsRestClient.createInstance(hosts, null, null).getRestClient();
        this.objectMapper = new ObjectMapper();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        this.elasticsearchContainer.close();
        this.restClient.close();
    }

    @TestTemplate
    public void testElasticSearch(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake-to-elasticsearch.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        ObjectNode objectNode = search("seatunnel_test_e2e");
        int hits = objectNode.get("hits").get("total").get("value").asInt();
        Assertions.assertEquals(1, hits);
    }

    private ObjectNode search(String index) throws IOException {
        Request request = new Request("POST", String.format("/%s/_search", index));
        Response response = restClient.performRequest(request);
        String result = EntityUtils.toString(response.getEntity());
        return objectMapper.readValue(result, ObjectNode.class);
    }
}
