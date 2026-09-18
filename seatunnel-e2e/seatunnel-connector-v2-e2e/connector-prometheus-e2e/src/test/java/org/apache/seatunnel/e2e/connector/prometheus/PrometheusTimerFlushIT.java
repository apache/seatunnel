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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

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
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.jayway.jsonpath.JsonPath;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
public class PrometheusTimerFlushIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "bitnamilegacy/prometheus:2.53.0";

    private static final String HOST = "prometheus-host";

    private static final String METRIC_NAME = "timer_flush_metric";

    private GenericContainer<?> prometheusContainer;

    @BeforeAll
    @Override
    public void startUp() {
        this.prometheusContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withEnv("TZ", "Asia/Shanghai")
                        .withExposedPorts(9090)
                        .withCommand(
                                "--config.file=/opt/bitnami/prometheus/conf/prometheus.yml",
                                "--web.enable-remote-write-receiver")
                        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
                        .waitingFor(
                                new HostPortWaitStrategy()
                                        .withStartupTimeout(Duration.ofMinutes(2)));
        Startables.deepStart(Stream.of(prometheusContainer)).join();
        log.info("Prometheus container started");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (prometheusContainer != null) {
            prometheusContainer.stop();
        }
    }

    @TestTemplate
    public void testPrometheusTimerFlush(TestContainer container) throws Exception {
        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob("/prometheus_timer_flush.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        try {
            // Wait until the streaming job is actually running.
            await().atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                if (jobFuture.isDone()) {
                                    Container.ExecResult jobResult = jobFuture.get();
                                    Assertions.fail(
                                            "The streaming job terminated before reaching RUNNING: "
                                                    + jobResult.getStderr());
                                }
                                Assertions.assertEquals("RUNNING", container.getJobStatus(jobId));
                            });

            // batch_size (100) is larger than the single buffered row, and the checkpoint
            // interval is long, so the row can only reach Prometheus through the engine timer
            // flush. Assert it arrives while the job is still running (before the writer closes).
            await().atMost(120, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the buffered row");
                                Metric metric = queryMetric(METRIC_NAME);
                                Assertions.assertNotNull(
                                        metric, "Prometheus has not received the buffered row yet");
                                Assertions.assertEquals(
                                        METRIC_NAME, metric.getMetric().get("__name__"));
                                Assertions.assertEquals("2.34", metric.getValue().get(1));
                            });
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }
    }

    private Metric queryMetric(String metricName) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet =
                    new HttpGet(
                            "http://"
                                    + prometheusContainer.getHost()
                                    + ":"
                                    + prometheusContainer.getMappedPort(9090)
                                    + "/api/v1/query?query="
                                    + metricName);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                String responseContent = EntityUtils.toString(response.getEntity());
                List<Metric> metrics =
                        JsonUtils.toList(
                                JsonPath.read(responseContent, "$.data.result.*").toString(),
                                Metric.class);
                return metrics.isEmpty() ? null : metrics.get(0);
            }
        }
    }

    @Data
    public static class Metric {

        private Map<String, String> metric;

        private List<String> value;
    }
}
