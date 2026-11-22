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

package org.apache.seatunnel.connectors.seatunnel.sonarqube;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SonarQubeSourceReaderTest {
    private static final String KEY = "example:project";
    private static final Set<String> KEYS =
            SonarQubeSource.validateMetrics(List.of("ncloc", "new_coverage"));
    private static final String PATH = "/sonarqube/api/measures/component";
    private static final String QUERY =
            "component=example%3Aproject&metricKeys=ncloc%2Cnew_coverage";
    private static final String FIRST =
            "{\"metric\":\"ncloc\",\"value\":\"42\",\"bestValue\":false}";
    private static final String SECOND =
            "{\"metric\":\"new_coverage\",\"period\":{\"value\":\"84.2\"}}";

    @Test
    void preservesOverallAndPeriodMetricsThroughHttp() throws Exception {
        List<SeaTunnelRow> rows = execute(200, envelope(FIRST + "," + SECOND), false);
        assertEquals(2, rows.size());
        assertEquals(KEY, rows.get(0).getField(0));
        assertEquals("Example project", rows.get(0).getField(1));
        assertEquals("42", rows.get(0).getField(3));
        assertEquals(false, rows.get(0).getField(4));
        assertNull(rows.get(1).getField(3));
        assertTrue(((String) rows.get(1).getField(5)).contains("\"period\":{\"value\":\"84.2\"}"));
    }

    @Test
    void acceptsEmptyAndOmittedMeasures() throws IOException {
        assertTrue(parse(envelope("")).isEmpty());
        assertEquals(1, parse(envelope(FIRST)).size());
    }

    @Test
    void rejectsMalformedWrongProjectAndNonProjectResponses() {
        assertThrows(IOException.class, () -> parse(envelope(FIRST) + "{}"));
        assertThrows(IOException.class, () -> parse(envelope(FIRST).replace(KEY, "wrong:key")));
        assertThrows(IOException.class, () -> parse(envelope(FIRST).replace("TRK", "FIL")));
        assertThrows(IOException.class, () -> parse("{\"component\":{}}"));
    }

    @Test
    void rejectsUnrequestedDuplicateAndWronglyTypedMeasurements() {
        assertThrows(IOException.class, () -> parse(envelope(FIRST + "," + FIRST)));
        assertThrows(IOException.class, () -> parse(envelope(FIRST.replace("ncloc", "issues"))));
        assertThrows(IOException.class, () -> parse(envelope(FIRST.replace("\"42\"", "42"))));
    }

    @Test
    void failsHttpErrorsAndDoesNotFollowRedirects() throws Exception {
        assertTrue(execute(302, "{}", true).isEmpty());
        assertTrue(execute(401, "{}", true).isEmpty());
    }

    @Test
    void validatesContextsMetricsAndSecrets() {
        URI uri =
                SonarQubeSource.measuresUri(
                        "https://sonar.example/sonarqube/", KEY, KEYS, "READ_TOKEN");
        assertEquals(PATH, uri.getRawPath());
        assertEquals(QUERY, uri.getRawQuery());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SonarQubeSource.measuresUri(
                                "https://sonar.example/segment/../", KEY, KEYS, null));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SonarQubeSource.measuresUri(
                                "http://localhost/sonarqube", KEY, KEYS, "READ_TOKEN"));
        assertThrows(
                IllegalArgumentException.class,
                () -> SonarQubeSource.measuresUri("https://name@sonar.example", KEY, KEYS, null));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SonarQubeSource.measuresUri(
                                "https://sonar.example", "x&metricKeys=coverage", KEYS, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> SonarQubeSource.validateMetrics(List.of("ncloc", "ncloc")));
    }

    @Test
    void doesNotSilentlyFallBackToAnonymousIfConfiguredSecretIsMissing() {
        String missing = "__SONARQUBE_EMBEDDED_TEST_ABSENT_TOKEN_298561__";
        URI uri = SonarQubeSource.measuresUri("https://sonar.example", KEY, KEYS, missing);
        try (SonarQubeSourceReader reader =
                new SonarQubeSourceReader(uri, KEY, KEYS, missing, null)) {
            assertThrows(IOException.class, reader::open);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static List<SeaTunnelRow> execute(int status, String response, boolean expectError)
            throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicReference<String> rawQuery = new AtomicReference<>();
        AtomicBoolean redirected = new AtomicBoolean();
        server.createContext(
                PATH,
                exchange -> {
                    rawQuery.set(exchange.getRequestURI().getRawQuery());
                    exchange.getResponseHeaders().set("Location", "/elsewhere");
                    byte[] data = response.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, data.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(data);
                    }
                });
        server.createContext(
                "/elsewhere",
                exchange -> {
                    redirected.set(true);
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        server.start();
        AtomicBoolean finished = new AtomicBoolean();
        List<SeaTunnelRow> rows = new ArrayList<>();
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(null) {
                    @Override
                    public void signalNoMoreElement() {
                        finished.set(true);
                    }
                };
        try {
            URI uri =
                    SonarQubeSource.measuresUri(
                            "http://127.0.0.1:" + server.getAddress().getPort() + "/sonarqube",
                            KEY,
                            KEYS,
                            null);
            try (SonarQubeSourceReader reader =
                    new SonarQubeSourceReader(uri, KEY, KEYS, null, context)) {
                reader.open();
                Collector<SeaTunnelRow> collector =
                        new Collector<SeaTunnelRow>() {
                            @Override
                            public void collect(SeaTunnelRow record) {
                                rows.add(record);
                            }

                            @Override
                            public Object getCheckpointLock() {
                                return this;
                            }
                        };
                if (expectError) {
                    IOException error =
                            assertThrows(IOException.class, () -> reader.pollNext(collector));
                    assertTrue(error.getMessage().endsWith("HTTP " + status));
                } else {
                    reader.pollNext(collector);
                }
            }
            assertEquals(QUERY, rawQuery.get());
            assertFalse(redirected.get());
            assertEquals(!expectError, finished.get());
            return rows;
        } finally {
            server.stop(0);
        }
    }

    private static String envelope(String measures) {
        return "{\"component\":{\"key\":\""
                + KEY
                + "\",\"name\":\"Example project\",\"qualifier\":\"TRK\",\"measures\":["
                + measures
                + "]}}";
    }

    private static List<SeaTunnelRow> parse(String response) throws IOException {
        return SonarQubeSourceReader.parseMeasures(
                response.getBytes(StandardCharsets.UTF_8), KEY, KEYS);
    }
}
