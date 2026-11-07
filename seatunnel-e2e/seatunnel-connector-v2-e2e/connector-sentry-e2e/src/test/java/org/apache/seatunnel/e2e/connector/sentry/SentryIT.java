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

package org.apache.seatunnel.e2e.connector.sentry;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.mockserver.client.MockServerClient;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.NottableOptionalString;
import org.mockserver.model.NottableString;
import org.mockserver.verify.VerificationTimes;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Engine integration with a deterministic API fixture, not a live Sentry deployment. */
public class SentryIT extends TestSuiteBase implements TestResource {
    private static final String ENDPOINT =
            "http://sentry-fixture:1080/api/0/projects/acme/demo/events/";
    private GenericContainer<?> fixture;
    private MockServerClient client;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("mockserver/mockserver:5.14.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("sentry-fixture")
                        .withExposedPorts(1080)
                        .withEnv("MOCKSERVER_LOG_LEVEL", "WARN")
                        .waitingFor(Wait.forHttp("/").forStatusCode(404));
        fixture.start();
        client = new MockServerClient(fixture.getHost(), fixture.getMappedPort(1080));
    }

    @AfterAll
    @Override
    public void tearDown() {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            try {
                if (fixture != null) {
                    fixture.stop();
                }
            } finally {
                NETWORK.close();
            }
        }
    }

    @TestTemplate
    public void readsAllPagesAfterRateLimitThroughEngine(TestContainer container) throws Exception {
        client.reset();
        HttpRequest first = firstRequest();
        HttpRequest second = request().withQueryStringParameter("cursor", "0:2:0");
        client.when(first, Times.exactly(1))
                .respond(
                        HttpResponse.response().withStatusCode(429).withHeader("Retry-After", "0"));
        client.when(first).respond(response(true, "[" + event("a") + "," + event("b") + "]"));
        client.when(second).respond(response(false, "[" + event("c") + "]"));
        Container.ExecResult result = container.executeJob("/sentry_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        client.verify(first, VerificationTimes.exactly(2));
        client.verify(second, VerificationTimes.exactly(1));
        client.verify(
                HttpRequest.request().withPath("/api/0/projects/acme/demo/events/"),
                VerificationTimes.exactly(3));
    }

    static HttpRequest firstRequest() {
        return request()
                .withQueryStringParameter(
                        NottableOptionalString.optional("cursor"), NottableString.not(".*"));
    }

    static HttpRequest request() {
        return HttpRequest.request()
                .withMethod("GET")
                .withPath("/api/0/projects/acme/demo/events/")
                .withHeader("Authorization", "Bearer mock-token")
                .withQueryStringParameter("start", "2026-01-01T00:00:00Z")
                .withQueryStringParameter("end", "2026-01-02T00:00:00Z")
                .withQueryStringParameter("per_page", "2")
                .withQueryStringParameter("full", "false")
                .withQueryStringParameter("sample", "false");
    }

    private HttpResponse response(boolean more, String body) {
        return HttpResponse.response()
                .withHeader("Content-Type", "application/json")
                .withHeader(
                        "Link",
                        "<" + ENDPOINT + "?cursor=0:2:0>; rel=\"next\"; results=\"" + more + "\"")
                .withBody(body);
    }

    private String event(String id) {
        return "{\"eventID\":\""
                + id
                + "\",\"groupID\":\"42\",\"projectID\":\"7\",\"dateCreated\":\"2026-01-01T12:00:00Z\",\"title\":\"Sentry event\",\"message\":null,\"platform\":\"java\"}";
    }
}
