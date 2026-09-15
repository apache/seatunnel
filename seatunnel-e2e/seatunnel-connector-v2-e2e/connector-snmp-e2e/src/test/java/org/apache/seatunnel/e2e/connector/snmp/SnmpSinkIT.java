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
package org.apache.seatunnel.e2e.connector.snmp;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.snmp4j.Snmp;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "The first SNMP sink E2E slice targets the Zeta engine")
public class SnmpSinkIT extends TestSuiteBase implements TestResource {

    private static final DockerImageName JAVA_IMAGE =
            DockerImageName.parse("eclipse-temurin:11-jre-jammy");
    private static final String EXPECTED_SET = "1.3.6.1.2.1.1.4.0=seatunnel-e2e";

    private GenericContainer<?> snmpAgent;

    @Override
    @BeforeAll
    public void startUp() throws URISyntaxException {
        Path testClasses =
                Paths.get(
                        SnmpAgent.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        Path snmp4jJar =
                Paths.get(Snmp.class.getProtectionDomain().getCodeSource().getLocation().toURI());

        snmpAgent =
                new GenericContainer<>(JAVA_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("snmp-agent")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(testClasses), "/opt/snmp-agent/classes")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(snmp4jJar), "/opt/snmp-agent/snmp4j.jar")
                        .withCommand(
                                "java",
                                "-cp",
                                "/opt/snmp-agent/classes:/opt/snmp-agent/snmp4j.jar",
                                SnmpAgent.class.getName())
                        .waitingFor(
                                Wait.forLogMessage(".*snmp-agent-ready.*\\n", 1)
                                        .withStartupTimeout(Duration.ofMinutes(1)));
        Startables.deepStart(Stream.of(snmpAgent)).join();
    }

    @Override
    public void tearDown() {
        if (snmpAgent != null) {
            snmpAgent.close();
        }
    }

    @TestTemplate
    public void testFakeSourceWritesSnmpSet(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/fake_to_snmp.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(
                        () -> {
                            Container.ExecResult result =
                                    snmpAgent.execInContainer("cat", "/tmp/snmp-set.txt");
                            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
                            Assertions.assertEquals(EXPECTED_SET, result.getStdout().trim());
                        });
    }
}
