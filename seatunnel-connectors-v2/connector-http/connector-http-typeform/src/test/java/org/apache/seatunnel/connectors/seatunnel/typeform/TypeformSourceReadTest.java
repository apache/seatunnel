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

package org.apache.seatunnel.connectors.seatunnel.typeform;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Reads Typeform-shaped pages through {@link TypeformSource} and the base HTTP reader, and checks
 * the cursor, the filters and how {@code answers} and {@code hidden} are emitted.
 */
public class TypeformSourceReadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final int ANSWERS = 5;
    private static final int HIDDEN = 6;

    // Answer shapes from Typeform's Retrieve Responses reference, plus escaping, unicode, a
    // decimal and an integer above 2^53.
    private static final String R1_ANSWERS =
            "[{\"field\":{\"id\":\"SMEUb7VJz92Q\",\"ref\":\"my_text\",\"type\":\"short_text\"},\"type\":\"text\",\"text\":\"Lian \\\"quoted\\\"\\nnew line, caf\\u00e9 \\u65e5\\u672c\"},"
                    + "{\"field\":{\"id\":\"RUqkXSeXBXSd\",\"ref\":\"my_yes_no\",\"type\":\"yes_no\"},\"type\":\"boolean\",\"boolean\":false},"
                    + "{\"field\":{\"id\":\"JwWggjAKtOkA\",\"ref\":\"my_email\",\"type\":\"email\"},\"type\":\"email\",\"email\":\"lian1078@other.com\"},"
                    + "{\"field\":{\"id\":\"Q7M2XAwY04dW\",\"ref\":\"my_number\",\"type\":\"number\"},\"type\":\"number\",\"number\":9007199254740993},"
                    + "{\"field\":{\"id\":\"dec1\",\"ref\":\"my_decimal\",\"type\":\"number\"},\"type\":\"number\",\"number\":3.14},"
                    + "{\"field\":{\"id\":\"KoJxDM3c6x8h\",\"ref\":\"my_date\",\"type\":\"date\"},\"type\":\"date\",\"date\":\"2012-03-20T00:00:00Z\"},"
                    + "{\"field\":{\"id\":\"PNe8ZKBK8C2Q\",\"ref\":\"my_multi\",\"type\":\"multiple_choice\"},\"type\":\"choices\",\"choices\":{\"labels\":[\"New York\",\"Tokyo\"]}},"
                    + "{\"field\":{\"id\":\"ceIXxpbP3t2q\",\"ref\":\"my_choice\",\"type\":\"multiple_choice\"},\"type\":\"choice\",\"choice\":{\"label\":\"Other\",\"other\":\"Kathmandu\"}},"
                    + "{\"field\":{\"id\":\"pn48RmPazVdM\",\"ref\":\"my_file\",\"type\":\"file_upload\"},\"type\":\"file_url\",\"file_url\":\"https://api.typeform.com/forms/lT9Z2j/responses/r1/fields/pn48/files/a.pdf\"},"
                    + "{\"field\":{\"id\":\"pay1\",\"ref\":\"my_payment\",\"type\":\"payment\"},\"type\":\"payment\",\"payment\":{\"amount\":\"4.20\",\"last4\":\"4242\",\"name\":\"Lian\",\"success\":true}}]";
    private static final String R1_HIDDEN =
            "{\"utm_source\":\"newsletter\",\"z_last\":\"z\",\"a_first\":\"a\",\"empty\":\"\"}";
    private static final String R5_ANSWERS =
            "[{\"field\":{\"id\":\"m1\",\"type\":\"matrix\"},\"type\":\"future_type\",\"future_type\":{\"rows\":[[1,2],[3,null]],\"meta\":{}}}]";
    private static final String R5_HIDDEN = "{\"nested_like\":\"{\\\"not\\\":\\\"parsed\\\"}\"}";

    /** A response item; {@code token} differs from {@code response_id} on purpose. */
    private static String item(String id, String answersJson, String hiddenJson) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"landing_id\":\"l-")
                .append(id)
                .append("\",\"token\":\"tok-")
                .append(id)
                .append("\",\"response_id\":\"")
                .append(id)
                .append("\",\"landed_at\":\"2026-09-14T22:33:59Z\",")
                .append("\"submitted_at\":\"2026-09-14T22:38:22Z\"");
        if (answersJson != null) {
            sb.append(",\"answers\":").append(answersJson);
        }
        if (hiddenJson != null) {
            sb.append(",\"hidden\":").append(hiddenJson);
        }
        return sb.append("}").toString();
    }

    private static String page(String... items) {
        return "{\"total_items\":5,\"page_count\":3,\"items\":[" + String.join(",", items) + "]}";
    }

    private static List<SeaTunnelRow> read(MockWebServer server, Map<String, Object> extra)
            throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("api_key", "tfp_test-token");
        m.put("form_id", "lT9Z2j");
        m.put("page_size", 2);
        m.put("base_url", server.url("/").toString());
        m.putAll(extra);
        TypeformSource source = new TypeformSource(ReadonlyConfig.fromMap(m));

        SourceReader.Context ctx = Mockito.mock(SourceReader.Context.class);
        TypeformSourceReader reader =
                (TypeformSourceReader) source.createReader(new SingleSplitReaderContext(ctx));
        reader.setMinRequestIntervalMillis(0);

        List<SeaTunnelRow> rows = new ArrayList<>();
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
        reader.open();
        try {
            reader.pollNext(collector);
        } finally {
            reader.close();
        }
        return rows;
    }

    private static Map<String, SeaTunnelRow> byResponseId(List<SeaTunnelRow> rows) {
        Map<String, SeaTunnelRow> map = new LinkedHashMap<>();
        for (SeaTunnelRow row : rows) {
            map.put((String) row.getField(0), row);
        }
        return map;
    }

    @Test
    public void testPagingWithBeforeTokenAndJsonStrings() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse()
                            .setBody(
                                    page(
                                            item("r1", R1_ANSWERS, R1_HIDDEN),
                                            item("r2", "[]", "{}"))));
            server.enqueue(
                    new MockResponse()
                            .setBody(page(item("r3", "null", "null"), item("r4", null, null))));
            server.enqueue(new MockResponse().setBody(page(item("r5", R5_ANSWERS, R5_HIDDEN))));
            server.enqueue(new MockResponse().setBody(page()));
            server.start();

            List<SeaTunnelRow> rows = read(server, new HashMap<>());

            // Paging: no cursor first, then the token (not the response_id) of the last item.
            Assertions.assertEquals(4, server.getRequestCount());
            RecordedRequest first = server.takeRequest(5, TimeUnit.SECONDS);
            Assertions.assertEquals("/forms/lT9Z2j/responses", first.getRequestUrl().encodedPath());
            Assertions.assertNull(first.getRequestUrl().queryParameter("before"));
            Assertions.assertEquals("2", first.getRequestUrl().queryParameter("page_size"));
            Assertions.assertEquals("Bearer tfp_test-token", first.getHeader("Authorization"));
            Assertions.assertFalse(first.getRequestUrl().toString().contains("tfp_test-token"));
            Assertions.assertEquals(
                    "tok-r2",
                    server.takeRequest(5, TimeUnit.SECONDS)
                            .getRequestUrl()
                            .queryParameter("before"));
            Assertions.assertEquals(
                    "tok-r4",
                    server.takeRequest(5, TimeUnit.SECONDS)
                            .getRequestUrl()
                            .queryParameter("before"));
            Assertions.assertEquals(
                    "tok-r5",
                    server.takeRequest(5, TimeUnit.SECONDS)
                            .getRequestUrl()
                            .queryParameter("before"));

            // Rows: five responses, the empty last page ends the read.
            Assertions.assertEquals(5, rows.size());
            Map<String, SeaTunnelRow> byId = byResponseId(rows);
            SeaTunnelRow r1 = byId.get("r1");
            Assertions.assertEquals("l-r1", r1.getField(1));
            Assertions.assertEquals("tok-r1", r1.getField(2));
            Assertions.assertEquals("2026-09-14T22:33:59Z", r1.getField(3));
            Assertions.assertEquals("2026-09-14T22:38:22Z", r1.getField(4));

            // answers and hidden are the source JSON, byte for byte: key order, escaping,
            // unicode, the decimal and the integer above 2^53 all survive.
            Assertions.assertEquals(
                    MAPPER.writeValueAsString(MAPPER.readTree(R1_ANSWERS)), r1.getField(ANSWERS));
            Assertions.assertEquals(R1_HIDDEN, r1.getField(HIDDEN));

            // Empty answers array becomes NULL (base reader behaviour); empty hidden stays "{}".
            Assertions.assertNull(byId.get("r2").getField(ANSWERS));
            Assertions.assertEquals("{}", byId.get("r2").getField(HIDDEN));
            // Explicit nulls and missing keys become NULL.
            Assertions.assertNull(byId.get("r3").getField(ANSWERS));
            Assertions.assertNull(byId.get("r3").getField(HIDDEN));
            Assertions.assertNull(byId.get("r4").getField(ANSWERS));
            Assertions.assertNull(byId.get("r4").getField(HIDDEN));
            // An unknown answer type passes through, and a JSON-looking string stays a string.
            Assertions.assertEquals(
                    MAPPER.writeValueAsString(MAPPER.readTree(R5_ANSWERS)),
                    byId.get("r5").getField(ANSWERS));
            Assertions.assertEquals(R5_HIDDEN, byId.get("r5").getField(HIDDEN));
        }
    }

    @Test
    public void testFiltersAreSentOnEveryPage() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(page(item("r1", "[]", "{}"))));
            server.enqueue(new MockResponse().setBody(page()));
            server.start();

            Map<String, Object> filters = new HashMap<>();
            filters.put("since", "2026-01-01T00:00:00");
            filters.put("until", "2026-12-31T23:59:59");
            filters.put("response_type", "partial,completed");
            List<SeaTunnelRow> rows = read(server, filters);

            Assertions.assertEquals(1, rows.size());
            Assertions.assertEquals(2, server.getRequestCount());
            for (int i = 0; i < 2; i++) {
                RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
                Assertions.assertEquals(
                        "2026-01-01T00:00:00", request.getRequestUrl().queryParameter("since"));
                Assertions.assertEquals(
                        "2026-12-31T23:59:59", request.getRequestUrl().queryParameter("until"));
                Assertions.assertEquals(
                        "partial,completed",
                        request.getRequestUrl().queryParameter("response_type"));
            }
        }
    }

    @Test
    public void testEmptyFormReadsNothing() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(page()));
            server.start();

            Assertions.assertTrue(read(server, new HashMap<>()).isEmpty());
            Assertions.assertEquals(1, server.getRequestCount());
        }
    }
}
