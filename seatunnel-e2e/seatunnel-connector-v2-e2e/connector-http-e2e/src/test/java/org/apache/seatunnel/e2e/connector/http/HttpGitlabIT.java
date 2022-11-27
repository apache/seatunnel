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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;

public class HttpGitlabIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    private GenericContainer<?> mockserverContainer;

    @BeforeAll
    @Override
    public void startUp() {
        this.mockserverContainer = new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases("mockserver")
            .withExposedPorts(1080)
            .withCopyFileToContainer(MountableFile.forHostPath(new File(Objects.requireNonNull(HttpIT.class.getResource(
                    "/mockserver-gitlab-config.json")).getPath()).getAbsolutePath()),
                "/tmp/mockserver-gitlab-config.json")
            .withEnv("MOCKSERVER_INITIALIZATION_JSON_PATH", "/tmp/mockserver-gitlab-config.json")
            .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)))
            .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));
        Startables.deepStart(Stream.of(mockserverContainer)).join();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mockserverContainer != null) {
            mockserverContainer.stop();
        }
    }

    @TestTemplate
    public void testHttpGitlabSourceToAssertSink(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/gitlab_json_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
