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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.stream.Stream;

@Slf4j
public class PrometheusIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "bitnami/prometheus:2.53.0";

    private GenericContainer<?> prometheusContainer;

    private static final String HOST = "prometheus-host";

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
        prometheusContainer.setPortBindings(Lists.newArrayList(String.format("%s:9090", "9090")));
        Startables.deepStart(Stream.of(prometheusContainer)).join();
        log.info("Mongodb container started");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (prometheusContainer != null) {
            prometheusContainer.stop();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "The version(3.25.4) introduced by Prometheus is higher than the version(2.5.0) in spark"
                            + " hadoop,do not support AbstractMessageLite$Builder.addAll")
    public void testPrometheusSinkAndSource(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult execResult = container.executeJob("/prometheus_remote_write.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult execResult1 =
                container.executeJob("/prometheus_instant_json_to_assert.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
    }
}
