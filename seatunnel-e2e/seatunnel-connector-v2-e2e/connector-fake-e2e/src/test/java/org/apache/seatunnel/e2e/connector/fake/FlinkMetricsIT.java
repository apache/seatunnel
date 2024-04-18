/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.flink.Flink13Container;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.SEATUNNEL})
public class FlinkMetricsIT extends TestSuiteBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkMetricsIT.class);

    @TestTemplate
    public void testFlinkMetrics(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult executeResult =
                container.executeJob("/fake_to_assert_verify_flink_metrics.conf");
        Assertions.assertEquals(0, executeResult.getExitCode());
        final String jobListUrl = "http://%s:8081/jobs/overview";
        final String jobDetailsUrl = "http://%s:8081/jobs/%s";
        final String jobAccumulatorUrl = "http://%s:8081/jobs/%s/vertices/%s/accumulators";
        final String jobManagerHost;
        String dockerHost = System.getenv("DOCKER_HOST");
        if (dockerHost == null) {
            jobManagerHost = "localhost";
        } else {
            URI uri = URI.create(dockerHost);
            jobManagerHost = uri.getHost();
        }
        // create http client
        CloseableHttpClient httpClient = HttpClients.createDefault();

        // get job id
        HttpGet httpGet = new HttpGet(String.format(jobListUrl, jobManagerHost));
        CloseableHttpResponse response = httpClient.execute(httpGet);
        Assertions.assertEquals(response.getStatusLine().getStatusCode(), 200);
        String responseContent = EntityUtils.toString(response.getEntity());
        ObjectNode jsonNode = JsonUtils.parseObject(responseContent);
        String jobId = jsonNode.get("jobs").get(0).get("jid").asText();
        Assertions.assertNotNull(jobId);

        // get job vertices
        httpGet = new HttpGet(String.format(jobDetailsUrl, jobManagerHost, jobId));
        response = httpClient.execute(httpGet);
        Assertions.assertEquals(response.getStatusLine().getStatusCode(), 200);

        responseContent = EntityUtils.toString(response.getEntity());
        jsonNode = JsonUtils.parseObject(responseContent);
        String verticeId = jsonNode.get("vertices").get(0).get("id").asText();

        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(10L, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            HttpGet httpGetTemp =
                                    new HttpGet(
                                            String.format(
                                                    jobAccumulatorUrl,
                                                    jobManagerHost,
                                                    jobId,
                                                    verticeId));
                            CloseableHttpResponse responseTemp = httpClient.execute(httpGetTemp);
                            String responseContentTemp =
                                    EntityUtils.toString(responseTemp.getEntity());
                            JsonNode jsonNodeTemp = JsonUtils.parseObject(responseContentTemp);
                            JsonNode metrics = jsonNodeTemp.get("user-accumulators");
                            int size = metrics.size();
                            if (size <= 0) {
                                throw new IllegalStateException(
                                        "Flink metrics not synchronized yet, next round");
                            }
                        });

        // get metrics
        httpGet = new HttpGet(String.format(jobAccumulatorUrl, jobManagerHost, jobId, verticeId));
        response = httpClient.execute(httpGet);
        responseContent = EntityUtils.toString(response.getEntity());
        jsonNode = JsonUtils.parseObject(responseContent);
        JsonNode metrics = jsonNode.get("user-accumulators");

        int size = metrics.size();

        Assertions.assertTrue(size > 0);

        Map<String, String> metricsMap = new HashMap<>();

        for (JsonNode metric : metrics) {
            String name = metric.get("name").asText();
            String value = metric.get("value").asText();
            metricsMap.put(name, value);
        }

        String sourceReceivedCount = metricsMap.get(MetricNames.SOURCE_RECEIVED_COUNT);
        String sourceReceivedBytes = metricsMap.get(MetricNames.SOURCE_RECEIVED_BYTES);

        Assertions.assertEquals(5, Integer.valueOf(sourceReceivedCount));
        Assertions.assertEquals(2160, Integer.valueOf(sourceReceivedBytes));

        // Due to limitations in Flink 13 version and code, the metrics on the writer side cannot be
        // aggregated into the global accumulator and can only be viewed in the operator based on
        // parallelism dimensions
        if (!(container instanceof Flink13Container)) {
            String sinkWriteCount = metricsMap.get(MetricNames.SINK_WRITE_COUNT);
            String sinkWriteBytes = metricsMap.get(MetricNames.SINK_WRITE_BYTES);
            Assertions.assertEquals(5, Integer.valueOf(sinkWriteCount));
            Assertions.assertEquals(2160, Integer.valueOf(sinkWriteBytes));
        }

        httpClient.close();
    }
}
