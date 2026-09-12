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

package org.apache.seatunnel.e2e.connector.paypal;

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
import org.mockserver.verify.VerificationTimes;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

public class PayPalIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> fixture;
    private MockServerClient client;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("mockserver/mockserver:5.14.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("paypal-fixture")
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
    public void readsAllRecordsThroughOAuthExpiryRefresh(TestContainer container) throws Exception {
        client.reset();
        HttpRequest oauth =
                HttpRequest.request()
                        .withMethod("POST")
                        .withPath("/v1/oauth2/token")
                        .withHeader(
                                "Authorization",
                                "Basic "
                                        + Base64.getEncoder()
                                                .encodeToString(
                                                        "mock-client:mock-secret"
                                                                .getBytes(StandardCharsets.UTF_8)))
                        .withBody("grant_type=client_credentials");
        client.when(oauth, Times.exactly(1))
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\"access_token\":\"first-token\",\"token_type\":\"Bearer\",\"expires_in\":1}"));
        client.when(oauth)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        "{\"access_token\":\"next-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));
        HttpRequest first = request(1, "first-token");
        HttpRequest last = request(2, "next-token");
        client.when(first)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withDelay(TimeUnit.MILLISECONDS, 1500)
                                .withBody(report(1, row("T0006") + "," + row("T1107"))));
        client.when(last)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(report(2, row("T0006"))));
        Container.ExecResult result = container.executeJob("/paypal_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        client.verify(oauth, VerificationTimes.exactly(2));
        client.verify(first, VerificationTimes.exactly(1));
        client.verify(last, VerificationTimes.exactly(1));
        client.verify(
                HttpRequest.request().withPath("/v1/reporting/transactions"),
                VerificationTimes.exactly(2));
    }

    private HttpRequest request(int page, String token) {
        return HttpRequest.request()
                .withMethod("GET")
                .withPath("/v1/reporting/transactions")
                .withHeader("Authorization", "Bearer " + token)
                .withHeader("PayPal-Enforce-ISO8601-Format", "true")
                .withQueryStringParameter("start_date", "2026-01-01T00:00:00Z")
                .withQueryStringParameter("end_date", "2026-01-02T00:00:00Z")
                .withQueryStringParameter("fields", "all")
                .withQueryStringParameter("balance_affecting_records_only", "N")
                .withQueryStringParameter("page_size", "2")
                .withQueryStringParameter("page", Integer.toString(page));
    }

    private String report(int page, String records) {
        return "{\"account_number\":\"ACCOUNT\",\"start_date\":\"2026-01-01T00:00:00Z\",\"end_date\":\"2026-01-02T00:00:00Z\",\"page\":"
                + page
                + ",\"total_pages\":2,\"total_items\":3,\"transaction_details\":["
                + records
                + "]}";
    }

    private String row(String event) {
        return "{\"transaction_info\":{\"transaction_id\":\"SAME-ID\",\"transaction_event_code\":\""
                + event
                + "\",\"transaction_amount\":{\"value\":\"0.123\",\"currency_code\":\"TND\"},\"fee_amount\":null}}";
    }
}
