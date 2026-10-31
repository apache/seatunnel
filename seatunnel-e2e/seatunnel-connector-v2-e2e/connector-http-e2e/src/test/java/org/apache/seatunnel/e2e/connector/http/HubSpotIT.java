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

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HubSpotIT extends TestSuiteBase {

    private static ClientAndServer mockServer;

    @BeforeAll
    public static void setup() throws Exception {

        // Expose the host port so the Docker container can reach it
        Testcontainers.exposeHostPorts(8089);

        // Start MockServer on port 8089 to match our conf file
        mockServer = ClientAndServer.startClientAndServer(8089);

        // Read mock JSON files
        String page1Json =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        HubSpotIT.class.getResource("/mock_page1.json").toURI())));
        String page2Json =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        HubSpotIT.class.getResource("/mock_page2.json").toURI())));

        // Stub Page 2 FIRST (More specific match: expects the '?after=token-123' param)
        mockServer
                .when(
                        HttpRequest.request()
                                .withMethod("GET")
                                .withPath("/crm/v3/objects/contacts")
                                .withQueryStringParameter("after", "token-123"))
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(page2Json));

        // Stub Page 1 SECOND (Fallback match: initial request with no token param)
        mockServer
                .when(HttpRequest.request().withMethod("GET").withPath("/crm/v3/objects/contacts"))
                .respond(
                        HttpResponse.response()
                                .withStatusCode(200)
                                .withHeader("Content-Type", "application/json")
                                .withBody(page1Json));
    }

    @AfterAll
    public static void tearDown() {
        if (mockServer != null) {
            mockServer.stop();
        }
    }

    @TestTemplate
    public void testHubspotSourcePagination(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/hubspot_source_to_assert.conf");
        assertEquals(0, execResult.getExitCode(), "SeaTunnel job should exit with code 0");
    }
}
