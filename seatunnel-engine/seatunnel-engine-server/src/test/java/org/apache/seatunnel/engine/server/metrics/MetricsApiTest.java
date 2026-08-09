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

package org.apache.seatunnel.engine.server.metrics;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import io.restassured.config.HttpClientConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.response.Response;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;

@DisabledOnOs(OS.WINDOWS)
public class MetricsApiTest {

    private static final String METRICS_URL =
            "http://localhost:8080" + RestConstant.REST_URL_METRICS;

    /**
     * Upper bound for the endpoint to become serviceable. Generous on purpose: the value only
     * decides how long a genuinely broken endpoint takes to report, never how long a healthy one
     * takes.
     */
    private static final long READY_TIMEOUT_SECONDS = 60;

    /**
     * Per-request connect/read timeout, well under {@link #READY_TIMEOUT_SECONDS}. Without an
     * explicit timeout a listener that accepts a connection but stalls mid-response blocks the
     * poll's evaluation thread indefinitely: Awaitility's atMost bound is only checked between poll
     * attempts, so a hung request never returns to be retried and never surfaces the diagnostic
     * body this test exists to capture.
     */
    private static final int REQUEST_TIMEOUT_MILLIS = 5_000;

    /**
     * Caps the response body embedded in a failure message so a large exposition payload does not
     * flood CI logs across up to 60 retried assertions.
     */
    private static final int MAX_LOGGED_BODY_CHARS = 4_000;

    private static HazelcastInstanceImpl instance;

    @BeforeAll
    public static void before() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(8080);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @Test
    public void metricsApiTest() {
        // The HTTP listener accepts requests as soon as the member reaches STARTED, but the
        // registered collectors read coordinator-owned state that is still being wired up at that
        // moment. Querying immediately made CI observe a transient 500 from a collector that ran
        // before its backing service was available. Poll until the endpoint answers, so a slow
        // start costs a few extra seconds instead of failing the whole unit-test job, while an
        // endpoint that never recovers still fails the test.
        //
        // pollDelay is set explicitly to zero: Awaitility otherwise defaults a fixed poll delay to
        // the poll interval, which would silently push the first request out by a second.
        // ignoreExceptionsInstanceOf(IOException.class) covers a transient connection failure from
        // the HTTP call itself (connect refused/reset, read timeout), not just an assertion
        // failure on its response, so a one-off socket error is retried under the same bound
        // instead of aborting the poll on the first occurrence. Scoped to IOException rather than
        // every Throwable so a genuine bug in assertMetricsExposed() still fails fast with its own
        // stack trace instead of being retried for the full budget and reported as a generic
        // timeout.
        Awaitility.await()
                .pollDelay(0, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .ignoreExceptionsInstanceOf(IOException.class)
                .untilAsserted(MetricsApiTest::assertMetricsExposed);
    }

    @AfterAll
    public static void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }

    /**
     * Asserts that the Prometheus endpoint exposes the metric families this test guards.
     *
     * <p>The full response body is attached to the status assertion because a collector failure is
     * translated into a 500 whose payload carries the originating stack trace. Without it a CI
     * failure only reports the status code and the real cause is unrecoverable from the logs. The
     * missing-metric assertions below log a truncated body instead: on that path the body is
     * unrelated metric family output, not a stack trace, and a full Prometheus exposition can run
     * tens of KB, which would otherwise flood CI logs across every retried poll.
     */
    private static void assertMetricsExposed() {
        Response response =
                given().config(
                                RestAssuredConfig.config()
                                        .httpClient(
                                                HttpClientConfig.httpClientConfig()
                                                        .setParam(
                                                                "http.connection.timeout",
                                                                REQUEST_TIMEOUT_MILLIS)
                                                        .setParam(
                                                                "http.socket.timeout",
                                                                REQUEST_TIMEOUT_MILLIS)))
                        .get(METRICS_URL);
        String body = response.getBody().asString();
        Assertions.assertEquals(
                200, response.getStatusCode(), "GET " + METRICS_URL + " failed, response: " + body);
        assertContains(body, "process_start_time_seconds");
        assertContains(body, "engine_state_store_local_owned_entries");
        assertContains(body, "engine_state_store_checkpoint_monitor_jobs");
    }

    private static void assertContains(String body, String expectedMetric) {
        Assertions.assertTrue(
                body.contains(expectedMetric),
                "Metric "
                        + expectedMetric
                        + " is missing from /metrics, response: "
                        + truncateForLogging(body));
    }

    private static String truncateForLogging(String body) {
        if (body.length() <= MAX_LOGGED_BODY_CHARS) {
            return body;
        }
        return body.substring(0, MAX_LOGGED_BODY_CHARS)
                + "...(truncated, "
                + body.length()
                + " chars total)";
    }
}
