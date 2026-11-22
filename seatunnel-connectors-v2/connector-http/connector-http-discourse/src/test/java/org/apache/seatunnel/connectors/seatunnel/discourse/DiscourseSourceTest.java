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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiscourseSourceTest {
    @Test
    void emitsCategoriesAndChildrenThenSignalsCompletionOnlyOnce() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    json(
                            "{\"category_list\":{\"categories\":[{\"id\":2,\"name\":\"General\",\"slug\":\"general\","
                                    + "\"topic_count\":3,\"post_count\":10,\"subcategory_list\":[{\"id\":4,\"name\":\"Help\","
                                    + "\"slug\":\"help\",\"parent_category_id\":2}]}]}}"));
            DiscourseSource source = source(server.url("/forum/").toString(), "CATEGORIES");
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader = reader(source, context)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                Collector<SeaTunnelRow> sink =
                        new Collector<SeaTunnelRow>() {
                            @Override
                            public void collect(SeaTunnelRow row) {
                                rows.add(row);
                            }

                            @Override
                            public Object getCheckpointLock() {
                                return this;
                            }
                        };
                reader.pollNext(sink);
                reader.pollNext(sink);
                assertEquals(2, rows.size());
                assertArrayEquals(
                        new Object[] {2L, "General", "general", null, 3L, 10L},
                        rows.get(0).getFields());
                assertArrayEquals(
                        new Object[] {4L, "Help", "help", 2L, null, null}, rows.get(1).getFields());
                assertEquals(1, context.signals);
                RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
                assertEquals("GET", request.getMethod());
                assertEquals(
                        "/forum/categories.json?include_subcategories=true", request.getPath());
                assertNull(request.getHeader("Api-Key"));
                assertNull(request.getHeader("Api-Username"));
                assertEquals(1, server.getRequestCount());
            }
        }
    }

    @Test
    void readsTypedLatestTopicsButNeverFollowsProviderContinuation() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    json(
                            "{\"topic_list\":{\"more_topics_url\":\"https://example.invalid/never\",\"topics\":["
                                    + "{\"id\":4294967296,\"title\":\"Topic\",\"slug\":\"topic\",\"category_id\":3,"
                                    + "\"created_at\":\"2026-01-01T00:00:00Z\",\"posts_count\":7,\"views\":13,\"closed\":false,\"archived\":true}]}}"));
            try (DiscourseSourceReader reader =
                    reader(
                            source(server.url("/").toString(), "LATEST_TOPICS", "topic_limit", 5),
                            new TestContext())) {
                assertArrayEquals(
                        new Object[] {
                            4294967296L,
                            "Topic",
                            "topic",
                            3L,
                            "2026-01-01T00:00:00Z",
                            null,
                            7L,
                            13L,
                            false,
                            true
                        },
                        reader.fetchRows().get(0).getFields());
                assertEquals(
                        "/latest.json?per_page=5",
                        server.takeRequest(1, TimeUnit.SECONDS).getPath());
                assertEquals(1, server.getRequestCount());
            }
        }
    }

    @Test
    void failsClosedOnRedirectAndNeverExposesErrorBody() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(302).setHeader("Location", "/unexpected"));
            server.enqueue(
                    new MockResponse().setResponseCode(429).setBody("private response content"));
            try (DiscourseSourceReader reader =
                    reader(source(server.url("/").toString(), "CATEGORIES"), new TestContext())) {
                assertEquals(
                        "Discourse CATEGORIES request returned HTTP 302",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(1, server.getRequestCount());
                assertEquals(
                        "Discourse CATEGORIES request returned HTTP 429",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(2, server.getRequestCount());
            }
        }
    }

    @Test
    void rejectsInvalidShapeOrRecordInsteadOfReturningPartialData() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(json("{\"category_list\":{}}"));
            server.enqueue(
                    json(
                            "{\"category_list\":{\"categories\":[{\"id\":1,\"name\":\"valid\",\"slug\":\"valid\"},{\"id\":\"x\"}]}}"));
            server.enqueue(json("{\"category_list\":{\"categories\":[]}}"));
            TestContext context = new TestContext();
            try (DiscourseSourceReader reader =
                    reader(source(server.url("/").toString(), "CATEGORIES"), context)) {
                List<SeaTunnelRow> rows = new ArrayList<>();
                Collector<SeaTunnelRow> out =
                        new Collector<SeaTunnelRow>() {
                            @Override
                            public void collect(SeaTunnelRow row) {
                                rows.add(row);
                            }

                            @Override
                            public Object getCheckpointLock() {
                                return this;
                            }
                        };
                assertTrue(
                        assertThrows(IOException.class, () -> reader.pollNext(out))
                                .getMessage()
                                .contains("categories array"));
                assertTrue(
                        assertThrows(IOException.class, () -> reader.pollNext(out))
                                .getMessage()
                                .contains("invalid type for field id"));
                assertTrue(rows.isEmpty());
                assertEquals(0, context.signals);
                reader.pollNext(out);
                assertEquals(1, context.signals);
            }
        }
    }

    @Test
    void rejectsUnsafeSiteAndAuthenticationSettings() {
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test/?key=secret", "CATEGORIES"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://user:secret@x.test", "CATEGORIES"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "token", "secret"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("http://x.test", "CATEGORIES", "token", "secret", "username", "bot"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "token", "a\r\nb", "username", "bot"));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "LATEST_TOPICS", "topic_limit", 101));
        assertThrows(
                IllegalArgumentException.class,
                () -> source("https://x.test", "CATEGORIES", "topic_limit", 4));
    }

    @Test
    void cancellationIsStickyAndPreventsNetworkAccess() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            DiscourseSource source = source(server.url("/").toString(), "CATEGORIES");
            try (DiscourseSourceReader reader =
                    (DiscourseSourceReader)
                            source.createReader(new SingleSplitReaderContext(new TestContext()))) {
                reader.cancel();
                reader.open();
                reader.cancel();
                assertEquals(
                        "Discourse source was cancelled",
                        assertThrows(IOException.class, reader::fetchRows).getMessage());
                assertEquals(0, server.getRequestCount());
            }
        }
    }

    private static final class TestContext implements SourceReader.Context {
        int signals;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            signals++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EventListener getEventListener() {
            throw new UnsupportedOperationException();
        }
    }

    private static DiscourseSourceReader reader(
            DiscourseSource source, SourceReader.Context context) {
        DiscourseSourceReader reader =
                (DiscourseSourceReader) source.createReader(new SingleSplitReaderContext(context));
        reader.open();
        return reader;
    }

    private static DiscourseSource source(String url, String resource, Object... options) {
        Map<String, Object> config = new HashMap<>();
        config.put("base_url", url);
        config.put("resource", resource);
        for (int i = 0; i < options.length; i += 2) {
            config.put((String) options[i], options[i + 1]);
        }
        return new DiscourseSource(ReadonlyConfig.fromMap(config));
    }

    private static MockResponse json(String body) {
        return new MockResponse().setHeader("Content-Type", "application/json").setBody(body);
    }
}
