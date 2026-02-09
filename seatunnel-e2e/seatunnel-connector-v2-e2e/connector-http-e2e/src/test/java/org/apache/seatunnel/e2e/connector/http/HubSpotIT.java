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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

public class HubSpotIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(HubSpotIT.class);
    private static final String MOCKSERVER_IMAGE = "mockserver/mockserver:5.14.0";
    private GenericContainer<?> mockServerContainer;
    private MockServerClient mockServerClient;

    @BeforeAll
    @Override
    public void startUp() {
        this.mockServerContainer =
                new GenericContainer<>(DockerImageName.parse(MOCKSERVER_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("mock-server")
                        .withExposedPorts(1080)
                        .waitingFor(
                                Wait.forHttp("/mockserver/status")
                                        .withMethod("PUT")
                                        .forStatusCode(200))
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        this.mockServerContainer.start();

        this.mockServerClient =
                new MockServerClient(
                        this.mockServerContainer.getHost(),
                        this.mockServerContainer.getMappedPort(1080));

        this.initMockServer();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (this.mockServerContainer != null) {
            this.mockServerContainer.close();
        }
        if (this.mockServerClient != null) {
            this.mockServerClient.close();
        }
    }

    private void initMockServer() {
        this.mockServerClient
                .when(HttpRequest.request().withMethod("GET").withPath("/crm/v3/objects/contacts"))
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\n"
                                                + "  \"results\": [\n"
                                                + "    {\n"
                                                + "      \"id\": \"101\",\n"
                                                + "      \"properties\": \"simple_test_value\"\n"
                                                + "    }\n"
                                                + "  ]\n"
                                                + "}"));
    }

    @TestTemplate
    public void testHubSpotSource(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/hubspot_source_case.conf");
        // Check for success code 0. If this fails, the job crashed.
        Assertions.assertEquals(0, result.getExitCode(), "Job failed to execute. Check logs.");
    }
}
