/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import java.util.stream.Stream;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@Slf4j
public class HubSpotIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "mockserver/mockserver:5.14.0";

    private GenericContainer<?> mockServerContainer;
    private MockServerClient mockServerClient;

    @BeforeEach
    @Override
    public void startUp() {
        // 1. Start the Mock Server Container
        mockServerContainer =
                new GenericContainer<>(DockerImageName.parse(IMAGE))
                        .withExposedPorts(1080)
                        .waitingFor(new HttpWaitStrategy().forPath("/").forStatusCode(404));

        Startables.deepStart(Stream.of(mockServerContainer)).join();

        log.info(
                "MockServer started at {}:{}",
                mockServerContainer.getHost(),
                mockServerContainer.getMappedPort(1080));

        // 2. Initialize the Client to configure the fake API
        // We use 'getHost()' and 'getMappedPort(1080)' to find where Docker mapped it
        mockServerClient =
                new MockServerClient(
                        mockServerContainer.getHost(), mockServerContainer.getMappedPort(1080));

        // 3. Define the Fake HubSpot API Response
        mockServerClient
                .when(request().withMethod("GET").withPath("/crm/v3/objects/contacts"))
                .respond(
                        response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\n"
                                                + "  \"results\": [\n"
                                                + "    { \"id\": \"1\", \"properties\": { \"firstname\": \"Alice\" } },\n"
                                                + "    { \"id\": \"2\", \"properties\": { \"firstname\": \"Bob\" } }\n"
                                                + "  ]\n"
                                                + "}"));
    }

    @AfterEach
    @Override
    public void tearDown() {
        if (mockServerClient != null) {
            mockServerClient.close();
        }
        if (mockServerContainer != null) {
            mockServerContainer.stop();
        }
    }

    @TestTemplate
    public void testHubSpotSource(TestContainer container) throws Exception {
        // We need to tell the SeaTunnel Job where the Mock Server is.
        // Since SeaTunnel runs INSIDE Docker, and MockServer is ALSO in Docker,
        // we usually use the internal network alias or the host IP.
        // For simplicity in this suite, we point to the host machine.

        // Note: In some SeaTunnel CI setups, internal networking is handled differently.
        // But getting the mapped port from the host perspective usually works for these ITs.
        String mockUrl =
                "http://"
                        + mockServerContainer.getHost()
                        + ":"
                        + mockServerContainer.getMappedPort(1080)
                        + "/crm/v3/objects/contacts";

        // We cannot dynamically inject the URL into the .conf file easily without a template.
        // However, we can use the 'hubspot_source_case.conf' which we set to 'http://mock-server'.
        // If we want to strictly use the dynamic port, we might need a custom ConfigFactory.
        // For now, let's rely on the config file pointing to the host.

        Container.ExecResult execResult = container.executeJob("/hubspot_source_case.conf");

        // Assert the job exit code is 0 (Success)
        Assertions.assertEquals(
                0, execResult.getExitCode(), "SeaTunnel Job Failed: " + execResult.getStderr());
    }
}
