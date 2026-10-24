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

package org.apache.seatunnel.lineage.openlineage;

import org.apache.seatunnel.lineage.LineageBackend;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenLineageBackendTest {

    @Test
    void serializesFacetAndStatisticsAndSendsToken() throws Exception {
        AtomicReference<String> body = new AtomicReference<>();
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/lineage",
                exchange -> {
                    body.set(
                            new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8));
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    respond(exchange, 200);
                });
        server.start();
        try {
            Map<String, Object> environment =
                    Collections.singletonMap("OPENLINEAGE_AUTH_TOKEN", "test-token");
            Map<String, Object> options = new HashMap<>();
            options.put(LineageConfig.ENABLED, true);
            options.put(
                    LineageConfig.URL,
                    "http://localhost:" + server.getAddress().getPort() + "/lineage");
            LineageConfig config =
                    LineageConfig.resolve(options, Collections.emptyMap(), environment);
            UUID runId = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
            LineageEvent event =
                    LineageEvent.builder()
                            .runId(runId)
                            .eventTime(ZonedDateTime.of(2026, 9, 1, 1, 2, 3, 0, ZoneOffset.UTC))
                            .eventType(LineageEventType.COMPLETE)
                            .jobNamespace("seatunnel")
                            .jobName("lineage-test")
                            .producer("https://seatunnel.apache.org/3.0.0-SNAPSHOT")
                            .runFacet("seatunnel_properties")
                            .runProperties(Collections.singletonMap("engine", "zeta"))
                            .inputs(
                                    Collections.singletonList(
                                            LineageDataset.of("mysql://host:9030", "db.in")))
                            .outputs(
                                    Collections.singletonList(
                                            LineageDataset.of("mysql://host:9030", "db.out")
                                                    .withOutputStatistics(
                                                            new LineageOutputStatistics(
                                                                    12L, 345L, "committed"))))
                            .build();

            new OpenLineageBackend().emit(config, event);

            assertTrue(body.get().contains("\"runId\":\"" + runId + "\""));
            assertTrue(body.get().contains("\"rowCount\":12"));
            assertTrue(body.get().contains("\"size\":345"));
            assertTrue(body.get().contains("\"output_statistics_semantics\":\"committed\""));
            assertFalse(body.get().contains("\"additionalProperties\""));
            assertEquals("Bearer test-token", authorization.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void disabledBackendDoesNotRequireEndpoint() throws Exception {
        LineageConfig config = LineageConfig.defaults();
        new OpenLineageBackend().emit(config, minimalEvent());
    }

    @Test
    void exposesHttpTransportThroughPublicApi() {
        assertEquals("http", new OpenLineageBackend().getName());
    }

    @Test
    void isDiscoverableThroughLineageBackendServiceProvider() {
        assertTrue(
                StreamSupport.stream(ServiceLoader.load(LineageBackend.class).spliterator(), false)
                        .anyMatch(backend -> backend.getClass() == OpenLineageBackend.class));
    }

    @Test
    void failedSendDoesNotThrowOrLogSensitiveEndpoint() {
        HttpServer server = null;
        Logger logger = (Logger) LogManager.getLogger(OpenLineageBackend.class);
        CapturingAppender appender = new CapturingAppender();
        Level originalLevel = logger.getLevel();
        boolean originalAdditive = logger.isAdditive();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        logger.setAdditive(false);
        appender.start();
        try {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/lineage", exchange -> respond(exchange, 500));
            server.start();

            String userInfoSecret = "url-password";
            String querySecret = "query-secret";
            Map<String, Object> options = new HashMap<>();
            options.put(LineageConfig.ENABLED, true);
            options.put(
                    LineageConfig.URL,
                    "http://url-user:"
                            + userInfoSecret
                            + "@127.0.0.1:"
                            + server.getAddress().getPort()
                            + "/lineage?token="
                            + querySecret);
            options.put(LineageConfig.RETRY_TIMES, 0);
            LineageConfig config =
                    LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap());

            assertDoesNotThrow(() -> new OpenLineageBackend().emit(config, minimalEvent()));

            String logs =
                    appender.getEvents().stream()
                            .map(LogEvent::getMessage)
                            .map(message -> message.getFormattedMessage())
                            .collect(Collectors.joining("\n"));
            assertTrue(logs.contains("http://127.0.0.1:" + server.getAddress().getPort()));
            assertFalse(logs.contains("url-user"));
            assertFalse(logs.contains(userInfoSecret));
            assertFalse(logs.contains(querySecret));
        } catch (IOException e) {
            throw new AssertionError("Unable to start test HTTP server", e);
        } finally {
            if (server != null) {
                server.stop(0);
            }
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
            logger.setAdditive(originalAdditive);
            appender.stop();
        }
    }

    /**
     * A rejected request answers with a status code, and that code is the one thing an operator
     * needs to tell a bad credential from a receiver that is down. It reaches the log only if the
     * failure's own message is logged.
     */
    @Test
    void reportsWhyASendFailed() {
        HttpServer server = null;
        try {
            server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/lineage", exchange -> respond(exchange, 500));
            server.start();

            Map<String, Object> options = new HashMap<>();
            options.put(LineageConfig.ENABLED, true);
            options.put(
                    LineageConfig.URL,
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/lineage");
            options.put(LineageConfig.RETRY_TIMES, 0);
            LineageConfig config =
                    LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap());

            String logs =
                    capturedWarnings(() -> new OpenLineageBackend().emit(config, minimalEvent()));

            assertTrue(logs.contains("HTTP 500"), logs);
        } catch (IOException e) {
            throw new AssertionError("Unable to start test HTTP server", e);
        } finally {
            if (server != null) {
                server.stop(0);
            }
        }
    }

    /**
     * An endpoint that cannot be used fails the same way on every attempt, so retrying it only
     * multiplies the backoff sleep by the retry count on every event the job reports.
     */
    @Test
    void doesNotRetryAnEndpointThatCannotBeUsed() {
        Map<String, Object> options = new HashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.RETRY_TIMES, 5000);
        LineageConfig config =
                LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap());

        long startedAt = System.nanoTime();
        String logs = capturedWarnings(() -> new OpenLineageBackend().emit(config, minimalEvent()));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

        // The retried path sleeps up to 100ms per attempt, so 5000 retries cannot fit in here.
        assertTrue(
                elapsedMs < 5000,
                "a configuration error must not be retried, it took " + elapsedMs + "ms");
        assertTrue(logs.contains(LineageConfig.URL), logs);
    }

    /** Runs the body with the backend's warnings captured, and returns them joined. */
    private static String capturedWarnings(Runnable body) {
        Logger logger = (Logger) LogManager.getLogger(OpenLineageBackend.class);
        CapturingAppender appender = new CapturingAppender();
        Level originalLevel = logger.getLevel();
        boolean originalAdditive = logger.isAdditive();
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
        logger.setAdditive(false);
        appender.start();
        try {
            body.run();
            return appender.getEvents().stream()
                    .map(event -> event.getMessage().getFormattedMessage())
                    .collect(Collectors.joining("\n"));
        } finally {
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
            logger.setAdditive(originalAdditive);
            appender.stop();
        }
    }

    private static LineageEvent minimalEvent() {
        return LineageEvent.builder()
                .runId(UUID.randomUUID())
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(LineageEventType.START)
                .jobNamespace("seatunnel")
                .jobName("lineage-test")
                .producer("https://seatunnel.apache.org/3.0.0-SNAPSHOT")
                .inputs(Collections.emptyList())
                .outputs(Arrays.asList(LineageDataset.of("mysql://host:9030", "db.out")))
                .build();
    }

    private static void respond(HttpExchange exchange, int status) throws IOException {
        exchange.sendResponseHeaders(status, -1);
        exchange.close();
    }

    private static byte[] readAll(InputStream input) throws IOException {
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = input.read(buffer)) >= 0) {
            output.write(buffer, 0, length);
        }
        return output.toByteArray();
    }

    private static final class CapturingAppender extends AbstractAppender {
        private final List<LogEvent> events = new ArrayList<>();

        private CapturingAppender() {
            super("OpenLineageBackendTest", null, PatternLayout.createDefaultLayout());
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        private List<LogEvent> getEvents() {
            return events;
        }
    }
}
