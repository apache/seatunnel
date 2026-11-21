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

package org.apache.seatunnel.e2e.connector.google.analytics4;

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
import org.mockserver.model.JsonBody;
import org.mockserver.verify.VerificationTimes;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class GoogleAnalytics4IT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> fixture;
    private MockServerClient client;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("mockserver/mockserver:5.14.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("ga4-fixture")
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
                // Engine callbacks finish before this single-class module's fixture teardown.
                NETWORK.close();
            }
        }
    }

    @TestTemplate
    void readsTypedPaginatedReport(TestContainer container) throws Exception {
        client.reset();
        HttpRequest first = request("0");
        HttpRequest last = request("2");
        client.when(first)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(
                                        report(
                                                row("CA", "10", "1.25")
                                                        + ","
                                                        + row("DE", "20", "2.50"))));
        client.when(last)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(report(row("US", "30", "3.75"))));
        Container.ExecResult result = container.executeJob("/google_analytics4_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        client.verify(first, VerificationTimes.exactly(1));
        client.verify(last, VerificationTimes.exactly(1));
    }

    private HttpRequest request(String offset) {
        return HttpRequest.request()
                .withMethod("POST")
                .withPath("/v1beta/properties/123:runReport")
                .withBody(
                        JsonBody.json(
                                "{\"dimensions\":[{\"name\":\"country\"}],"
                                        + "\"metrics\":[{\"name\":\"activeUsers\"},{\"name\":\"purchaseRevenue\"}],"
                                        + "\"dateRanges\":[{\"startDate\":\"2024-01-01\",\"endDate\":\"2024-01-31\"}],"
                                        + "\"orderBys\":[{\"dimension\":{\"dimensionName\":\"country\",\"orderType\":\"ALPHANUMERIC\"}}],"
                                        + "\"offset\":\""
                                        + offset
                                        + "\",\"limit\":\"2\",\"returnPropertyQuota\":true,\"keepEmptyRows\":true}"));
    }

    private String report(String rows) {
        return "{\"dimensionHeaders\":[{\"name\":\"country\"}],"
                + "\"metricHeaders\":[{\"name\":\"activeUsers\",\"type\":\"TYPE_INTEGER\"},"
                + "{\"name\":\"purchaseRevenue\",\"type\":\"TYPE_CURRENCY\"}],"
                + "\"rowCount\":3,\"rows\":["
                + rows
                + "],"
                + "\"metadata\":{\"currencyCode\":\"USD\",\"timeZone\":\"UTC\"}}";
    }

    private String row(String country, String users, String revenue) {
        return "{\"dimensionValues\":[{\"value\":\""
                + country
                + "\"}],"
                + "\"metricValues\":[{\"value\":\""
                + users
                + "\"},{\"value\":\""
                + revenue
                + "\"}]}";
    }
}
