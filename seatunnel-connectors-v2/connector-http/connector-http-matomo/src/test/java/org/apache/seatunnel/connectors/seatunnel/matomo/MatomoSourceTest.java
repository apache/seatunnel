/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.matomo;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MatomoSourceTest {
    private static final LocalDate FIRST = LocalDate.of(2026, 6, 1);
    private static final LocalDate SECOND = FIRST.plusDays(1);
    private static final String METRICS = "{\"nb_visits\":25,\"nb_actions\":40}";

    private static MatomoSourceReader reader(URI base, LocalDate end, String token) {
        return new MatomoSourceReader(base, 1, FIRST, end, token, 3000, null, "matomo");
    }

    @Test
    void parsesRangeInOrderAndIgnoresExplicitEmptyDays() throws IOException {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        List<SeaTunnelRow> rows =
                r.decode("{\"" + SECOND + "\":[] ,\"" + FIRST + "\":" + METRICS + "}");
        assertEquals(1, rows.size());
        assertArrayEquals(
                new Object[] {1, FIRST.toString(), 25L, 40L, null}, rows.get(0).getFields());
        assertEquals("matomo", rows.get(0).getTableId());
    }

    @Test
    void rejectsMissingDaysAndApplicationErrorsWithoutRepeatingBody() {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        assertThrows(IOException.class, () -> r.decode("{\"" + FIRST + "\":" + METRICS + "}"));
        IOException e =
                assertThrows(
                        IOException.class,
                        () ->
                                r.decode(
                                        "{\"result\":\"error\",\"message\":\"synthetic-sensitive\"}"));
        assertFalse(e.getMessage().contains("synthetic-sensitive"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":-1,\"nb_actions\":40}"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":9223372036854775808,\"nb_actions\":40}"));
    }

    @Test
    void usesAnonymousGetAndPrivatePostWithoutTokenInUri() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(METRICS));
            server.enqueue(new MockResponse().setBody(METRICS));
            server.start();
            MatomoSourceReader publicReader = reader(server.url("/index.php").uri(), FIRST, null);
            MatomoSourceReader formReader =
                    reader(server.url("/index.php").uri(), FIRST, "fake +&token");
            publicReader.open();
            formReader.open();
            try {
                assertEquals(1, publicReader.readRows().size());
                assertEquals(1, formReader.readRows().size());
                RecordedRequest get = server.takeRequest(1, TimeUnit.SECONDS);
                RecordedRequest post = server.takeRequest(1, TimeUnit.SECONDS);
                assertEquals("GET", get.getMethod());
                assertTrue(get.getPath().contains("method=VisitsSummary.get"));
                assertEquals("POST", post.getMethod());
                assertEquals("/index.php", post.getPath());
                assertTrue(post.getBody().readUtf8().contains("token_auth=fake+%2B%26token"));
            } finally {
                publicReader.close();
                formReader.close();
            }
        }
    }

    @Test
    void rejectsRedirectsAndHttpErrors() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(302).setHeader("Location", "/another"));
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            MatomoSourceReader r = reader(server.url("/index.php").uri(), FIRST, null);
            r.open();
            try {
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("302"));
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("429"));
                assertEquals(2, server.getRequestCount());
            } finally {
                r.close();
            }
        }
    }

    @Test
    void validatesHttpsForPrivateTokensAndDateBounds() {
        Map<String, Object> c = new HashMap<>();
        c.put("url", "http://127.0.0.1/index.php");
        c.put("site_id", 1);
        c.put("start_date", FIRST.toString());
        c.put("end_date", SECOND.toString());
        assertEquals("Matomo", new MatomoSource(ReadonlyConfig.fromMap(c)).getPluginName());
        c.put("token", "fake");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
        c.put("url", "https://example.org/index.php");
        c.put("end_date", "2029-12-31");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
    }
}
