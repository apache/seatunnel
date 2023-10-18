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
import java.net.URL;
import java.util.Optional;
import java.util.stream.Stream;

public class HttpIT extends TestSuiteBase implements TestResource {

    private static final String TMP_DIR = "/tmp";

    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    private GenericContainer<?> mockserverContainer;

    @BeforeAll
    @Override
    public void startUp() {
        Optional<URL> resource =
                Optional.ofNullable(HttpIT.class.getResource(getMockServerConfig()));
        this.mockserverContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mockserver")
                        .withExposedPorts(1080)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(
                                        new File(
                                                        resource.orElseThrow(
                                                                        () ->
                                                                                new IllegalArgumentException(
                                                                                        "Can not get config file of mockServer"))
                                                                .getPath())
                                                .getAbsolutePath()),
                                TMP_DIR + getMockServerConfig())
                        .withEnv(
                                "MOCKSERVER_INITIALIZATION_JSON_PATH",
                                TMP_DIR + getMockServerConfig())
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
    public void testSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        // normal http
        Container.ExecResult execResult1 = container.executeJob("/http_json_to_assert.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());

        // http github
        Container.ExecResult execResult2 = container.executeJob("/github_json_to_assert.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());

        // http gitlab
        Container.ExecResult execResult3 = container.executeJob("/gitlab_json_to_assert.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());

        // http content json
        Container.ExecResult execResult4 = container.executeJob("/http_contentjson_to_assert.conf");
        Assertions.assertEquals(0, execResult4.getExitCode());

        // http jsonpath
        Container.ExecResult execResult5 = container.executeJob("/http_jsonpath_to_assert.conf");
        Assertions.assertEquals(0, execResult5.getExitCode());

        // http jira
        Container.ExecResult execResult6 = container.executeJob("/jira_json_to_assert.conf");
        Assertions.assertEquals(0, execResult6.getExitCode());

        // http klaviyo
        Container.ExecResult execResult7 = container.executeJob("/klaviyo_json_to_assert.conf");
        Assertions.assertEquals(0, execResult7.getExitCode());

        // http lemlist
        Container.ExecResult execResult8 = container.executeJob("/lemlist_json_to_assert.conf");
        Assertions.assertEquals(0, execResult8.getExitCode());

        // http notion
        Container.ExecResult execResult9 = container.executeJob("/notion_json_to_assert.conf");
        Assertions.assertEquals(0, execResult9.getExitCode());

        // http onesignal
        Container.ExecResult execResult10 = container.executeJob("/onesignal_json_to_assert.conf");
        Assertions.assertEquals(0, execResult10.getExitCode());

        // http persistiq
        Container.ExecResult execResult11 = container.executeJob("/persistiq_json_to_assert.conf");
        Assertions.assertEquals(0, execResult11.getExitCode());

        // http httpMultiLine
        Container.ExecResult execResult12 =
                container.executeJob("/http_multilinejson_to_assert.conf");
        Assertions.assertEquals(0, execResult12.getExitCode());

        // http httpFormRequestbody
        Container.ExecResult execResult13 =
                container.executeJob("/http_formrequestbody_to_assert.conf");
        Assertions.assertEquals(0, execResult13.getExitCode());

        // http httpJsonRequestBody
        Container.ExecResult execResult14 =
                container.executeJob("/http_jsonrequestbody_to_assert.conf");
        Assertions.assertEquals(0, execResult14.getExitCode());
    }

    public String getMockServerConfig() {
        return "/mockserver-config.json";
    }
}
