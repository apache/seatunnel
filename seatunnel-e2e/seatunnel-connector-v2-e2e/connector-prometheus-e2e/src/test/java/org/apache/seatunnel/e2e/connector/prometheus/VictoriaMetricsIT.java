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
package org.apache.seatunnel.e2e.connector.prometheus;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;

import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class VictoriaMetricsIT extends TestSuiteBase implements TestResource {
    private static final String IMAGE = "victoriametrics/victoria-metrics:v1.103.0";

    private GenericContainer<?> victoriaMetricsContainer;

    private static final String HOST = "victoria-metrics-host";

    private static final long INDEX_REFRESH_MILL_DELAY = 30000L;

    @BeforeAll
    @Override
    public void startUp() throws UnknownHostException {
        String host = InetAddress.getLocalHost().getHostAddress();
        victoriaMetricsContainer =
                new GenericContainer<>(IMAGE)
                        .withExposedPorts(8428)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withEnv("TZ", "Asia/Shanghai")
                        .withCommand(
                                "--httpListenAddr=0.0.0.0:8428",
                                "--search.minStalenessInterval=0s",
                                "--storageDataPath=/victoria-metrics-data")
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        ;

        victoriaMetricsContainer.setPortBindings(
                Lists.newArrayList(String.format("%s:8428", "8428")));
        Startables.deepStart(Stream.of(victoriaMetricsContainer)).join();
        log.info("victoriaMetrics container started");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (victoriaMetricsContainer != null) {
            victoriaMetricsContainer.stop();
        }
    }

    @TestTemplate
    public void testVictoriaMetricsSinkAndSource(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult =
                container.executeJob("/victoriaMetrics_remote_write.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // waiting  refresh
        Thread.sleep(INDEX_REFRESH_MILL_DELAY);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String host = InetAddress.getLocalHost().getHostAddress();
        HttpGet httpGet = new HttpGet("http://" + host + ":8428/api/v1/query?query=metric_1");
        CloseableHttpResponse response = httpClient.execute(httpGet);
        String responseContent = EntityUtils.toString(response.getEntity());
        List<Metric> metrics =
                JsonUtils.toList(
                        JsonPath.read(responseContent, "$.data.result.*").toString(), Metric.class);

        Metric metric = metrics.get(0);

        log.info("response:{},metric:{}", responseContent, metrics);
        Assertions.assertEquals(response.getStatusLine().getStatusCode(), 200);

        Assertions.assertEquals(metric.getMetric().get("__name__"), "metric_1");
        Assertions.assertEquals(metric.getValue().get(1), "1.23");

        Container.ExecResult execResultForInstant =
                container.executeJob("/VictoriaMetrics_instant_json_to_assert.conf");
        Assertions.assertEquals(0, execResultForInstant.getExitCode());
    }

    @Data
    public static class Metric {

        private Map<String, String> metric;

        private List<String> value;
    }
}
