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

package org.apache.seatunnel.e2e.connector.tiktok.ads;

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
import org.mockserver.verify.VerificationTimes;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TikTokAdsIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> fixture;
    private MockServerClient client;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("mockserver/mockserver:5.14.0"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("tiktok-fixture")
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
                // Engine callbacks finish before this single-class module's teardown.
                NETWORK.close();
            }
        }
    }

    @TestTemplate
    protected void readsTypedPaginatedReport(TestContainer container) throws Exception {
        client.reset();
        HttpRequest first = request("1");
        HttpRequest last = request("2");
        client.when(first)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(report(1, row("100") + "," + row("101"))));
        client.when(last)
                .respond(
                        HttpResponse.response()
                                .withHeader("Content-Type", "application/json")
                                .withBody(report(2, row("102"))));
        Container.ExecResult result = container.executeJob("/tiktok_ads_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        client.verify(first, VerificationTimes.exactly(1));
        client.verify(last, VerificationTimes.exactly(1));
        client.verify(
                HttpRequest.request().withPath("/open_api/v1.3/report/integrated/get/"),
                VerificationTimes.exactly(2));
    }

    private HttpRequest request(String page) {
        return HttpRequest.request()
                .withMethod("GET")
                .withPath("/open_api/v1.3/report/integrated/get/")
                .withHeader("Access-Token", "mock-token")
                .withQueryStringParameter("advertiser_id", "123456789")
                .withQueryStringParameter("report_type", "BASIC")
                .withQueryStringParameter("service_type", "AUCTION")
                .withQueryStringParameter("data_level", "AUCTION_AD")
                .withQueryStringParameter("dimensions", "[\"ad_id\",\"stat_time_day\"]")
                .withQueryStringParameter("metrics", "[\"spend\",\"impressions\",\"clicks\"]")
                .withQueryStringParameter("start_date", "2026-09-01")
                .withQueryStringParameter("end_date", "2026-09-02")
                .withQueryStringParameter("query_lifetime", "false")
                .withQueryStringParameter("query_mode", "REGULAR")
                .withQueryStringParameter("page", page)
                .withQueryStringParameter("page_size", "2");
    }

    private String report(int page, String rows) {
        return "{\"code\":0,\"message\":\"OK\",\"data\":{\"page_info\":{\"page\":"
                + page
                + ",\"page_size\":2,\"total_number\":3,\"total_page\":2},\"list\":["
                + rows
                + "]}}";
    }

    private String row(String id) {
        return "{\"dimensions\":{\"ad_id\":\""
                + id
                + "\",\"stat_time_day\":\"2026-09-01 00:00:00\"},"
                + "\"metrics\":{\"spend\":\"1.234567\",\"impressions\":\"100\",\"clicks\":\"2\"}}";
    }
}
