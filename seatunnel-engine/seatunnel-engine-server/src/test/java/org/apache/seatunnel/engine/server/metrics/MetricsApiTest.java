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
     * Per-request connect/read timeout, well under {@link #READY_TIMEOUT_SECONDS}. Awaitility still
     * enforces its overall bound on the awaiting thread even while an attempt hangs in a socket
     * read, but without a per-request timeout one stalled attempt silently consumes the whole
     * budget: the poll never gets another try, the abandoned evaluation thread stays blocked past
     * cancellation, and the run ends in a bare timeout with no response body. Bounding each request
     * keeps every attempt short enough to be retried within the budget and keeps the eventual
     * failure a diagnostic-bearing assertion instead.
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
        instance = createHazelcastInstance();
    }

    private static HazelcastInstanceImpl createHazelcastInstance() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(8080);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        return SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    @Test
    public void metricsApiTest() {
        assertMetricsApi();

        instance.shutdown();
        instance = createHazelcastInstance();

        assertMetricsApi();
    }

    private static void assertMetricsApi() {
        // The HTTP listener accepts requests as soon as the member reaches STARTED, but the
        // registered collectors read coordinator-owned state that is still being wired up at that
        // moment. Querying immediately made CI observe a transient 500 from a collector that ran
        // before its backing service was available. Poll until the endpoint answers, so a slow
        // start costs a few extra seconds instead of failing the whole unit-test job, while an
        // endpoint that never recovers still fails the test. This poll is only the test-side
        // mitigation; the production-side startup window it tolerates is tracked in
        // https://github.com/apache/seatunnel/issues/11846.
        //
        // pollDelay is set explicitly to zero: Awaitility otherwise defaults a fixed poll delay to
        // the poll interval, which would silently push the first request out by a second.
        // Transport failures are converted to AssertionError inside assertMetricsExposed() rather
        // than suppressed with ignoreExceptions: untilAsserted retries AssertionError under the
        // same bound, and Awaitility records only AssertionError messages into its timeout
        // diagnostic, so this keeps the last connection error visible in the final report instead
        // of timing out with no cause at all.
        Awaitility.await()
                .pollDelay(0, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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
        Response response;
        try {
            response =
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
        } catch (Exception e) {
            // Rethrow transport-level failures as AssertionError: the poll retries them under the
            // same overall bound, and the last connection error stays visible in Awaitility's
            // timeout message instead of the run ending in a timeout with no diagnostic.
            throw new AssertionError("GET " + METRICS_URL + " was not reachable: " + e, e);
        }
        String body = response.getBody().asString();
        Assertions.assertEquals(
                200,
                response.getStatusCode(),
                () -> "GET " + METRICS_URL + " failed, response: " + body);
        assertContains(body, "process_start_time_seconds");
        assertContains(body, "engine_state_store_local_owned_entries");
        assertContains(body, "engine_state_store_checkpoint_monitor_jobs");
    }

    private static void assertContains(String body, String expectedMetric) {
        // Message suppliers keep both failure texts unbuilt on the passing path; the multi-KB
        // exposition body is only concatenated when an assertion actually fails.
        Assertions.assertTrue(
                body.contains(expectedMetric),
                () ->
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
