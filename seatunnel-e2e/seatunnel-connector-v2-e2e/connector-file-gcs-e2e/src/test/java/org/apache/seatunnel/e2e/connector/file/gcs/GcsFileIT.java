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

package org.apache.seatunnel.e2e.connector.file.gcs;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.util.stream.Stream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.FLINK},
        disabledReason = "The GCS connector requires Hadoop 3, but these images use Hadoop 2.7")
public class GcsFileIT extends TestSuiteBase implements TestResource {

    private static final String FAKE_GCS_IMAGE = "fsouza/fake-gcs-server:1.56.1";
    private static final String FAKE_GCS_HOST = "fake-gcs";
    private static final int FAKE_GCS_PORT = 4443;
    private static final String JOB_CONFIG = "/gcs/gcs_file_to_assert.conf";

    private GenericContainer<?> fakeGcs;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName image = DockerImageName.parse(FAKE_GCS_IMAGE);
        fakeGcs =
                new GenericContainer<>(image)
                        .withCommand(
                                "-scheme",
                                "http",
                                "-port",
                                String.valueOf(FAKE_GCS_PORT),
                                "-external-url",
                                "http://" + FAKE_GCS_HOST + ":" + FAKE_GCS_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FAKE_GCS_HOST)
                        .withExposedPorts(FAKE_GCS_PORT)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("gcs/data/e2e.json"),
                                "/data/seatunnel-gcs-test/input/e2e.json")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                image.asCanonicalNameString())))
                        .waitingFor(
                                Wait.forHttp("/storage/v1/b")
                                        .forPort(FAKE_GCS_PORT)
                                        .forStatusCode(200));
        Startables.deepStart(Stream.of(fakeGcs)).join();
        SeaTunnelContainer.enableGcsOpenCensusThreadExemption();
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (fakeGcs != null) {
                fakeGcs.close();
            }
        } finally {
            SeaTunnelContainer.disableGcsOpenCensusThreadExemption();
        }
    }

    @TestTemplate
    public void testReadJsonFromGcs(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob(JOB_CONFIG);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
