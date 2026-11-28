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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink.AmazonDataFirehoseSinkOptions.Format;

import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises bounded native conversion through real typed values and the production
 * writer/transport. The local HTTP endpoint is a protocol fixture; it does not establish live
 * service acceptance.
 */
class FirehoseRowSerializationSchemaTest {
    @Test
    void rejectsExpansionBeforeVisitingAllEntriesAndBeforeAnyServiceCall() throws Exception {
        SeaTunnelRowType type =
                rowType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
        char[] characters = new char[128];
        Arrays.fill(characters, 'a');
        String shared = new String(characters);
        try (Endpoint endpoint = new Endpoint()) {
            for (Format format : new Format[] {Format.JSON, Format.DELIMITED}) {
                TrackingMap map = new TrackingMap();
                for (int i = 0; i < 16; i++) {
                    map.values.put("k" + i, shared);
                }
                try (AmazonDataFirehoseSinkWriter writer = endpoint.writer(type, format, 512)) {
                    IOException failure =
                            assertThrows(
                                    IOException.class,
                                    () -> writer.write(new SeaTunnelRow(new Object[] {map})));
                    assertTrue(failure.getMessage().contains("max_record_bytes"));
                    // The old serializer consumes all 16 entries before its post-encoding byte
                    // check.
                    assertTrue(map.visited > 0 && map.visited < 16);
                }
                assertEquals(0, endpoint.requests.size());
                String[] array = new String[16];
                Arrays.fill(array, shared);
                try (AmazonDataFirehoseSinkWriter writer =
                        endpoint.writer(rowType(ArrayType.STRING_ARRAY_TYPE), format, 512)) {
                    assertThrows(
                            IOException.class,
                            () -> writer.write(new SeaTunnelRow(new Object[] {array})));
                }
                assertEquals(0, endpoint.requests.size());
            }
        }
    }

    @Test
    void sendsExactSmallNativeMapOutputsAndRetainsTheFinalLineFeed() throws Exception {
        SeaTunnelRowType type =
                rowType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
        Map<String, String> map = new LinkedHashMap<>();
        map.put("snow", "雪");
        map.put("quote", "\"\\\n");
        try (Endpoint endpoint = new Endpoint()) {
            for (Format format : new Format[] {Format.JSON, Format.DELIMITED}) {
                try (AmazonDataFirehoseSinkWriter writer = endpoint.writer(type, format, 512)) {
                    writer.write(new SeaTunnelRow(new Object[] {map}));
                }
            }
            assertEquals(2, endpoint.requests.size());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(endpoint.requests.get(0));
            JsonNode csv = mapper.readTree(endpoint.requests.get(1));
            assertEquals("{\"value\":{\"snow\":\"雪\",\"quote\":\"\\\"\\\\\\n\"}}\n", decode(json));
            assertEquals("snow\u0003雪\u0002quote\u0003\"\\\n\n", decode(csv));
        }
    }

    @Test
    void preservesDeepNarrowJsonAndSharedSchemaWithNullValuesWithoutRecursiveConverters()
            throws Exception {
        SeaTunnelDataType<?> narrow = BasicType.STRING_TYPE;
        Object value = "x";
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < 2048; i++) {
            narrow = rowType(narrow);
            value = new SeaTunnelRow(new Object[] {value});
            prefix.append("{\"value\":");
        }
        StringBuilder expected = new StringBuilder(prefix).append("\"x\"");
        for (int i = 0; i < 2048; i++) {
            expected.append('}');
        }
        assertEquals(
                expected.toString(),
                new String(
                        new FirehoseRowSerializationSchema(
                                        (SeaTunnelRowType) narrow, Format.JSON, "|", 65536)
                                .serialize((SeaTunnelRow) value),
                        StandardCharsets.UTF_8));

        SeaTunnelDataType<?> shared = BasicType.STRING_TYPE;
        for (int i = 0; i < 8; i++) {
            shared =
                    new SeaTunnelRowType(
                            new String[] {"left", "right"},
                            new SeaTunnelDataType<?>[] {shared, shared});
        }
        SeaTunnelRowType type = rowType(shared);
        assertEquals(
                "{\"value\":null}",
                new String(
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", 512)
                                .serialize(new SeaTunnelRow(new Object[] {null})),
                        StandardCharsets.UTF_8));
    }

    @Test
    void enforcesExactEscapedJsonBoundaryAndExistingDelimitedNestingBoundary() throws Exception {
        SeaTunnelRowType type = rowType(BasicType.STRING_TYPE);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"\"\\雪"});
        String expected = "{\"value\":\"\\\"\\\\雪\"}";
        int bytes = expected.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(
                expected,
                new String(
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", bytes + 1)
                                .serialize(row),
                        StandardCharsets.UTF_8));
        assertThrows(
                IOException.class,
                () ->
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", bytes)
                                .serialize(row));
        SeaTunnelDataType<?> nested = BasicType.STRING_TYPE;
        Object value = "x";
        for (int i = 0; i < 9; i++) {
            nested = rowType(nested);
            value = new SeaTunnelRow(new Object[] {value});
        }
        FirehoseRowSerializationSchema csv =
                new FirehoseRowSerializationSchema(
                        (SeaTunnelRowType) nested, Format.DELIMITED, "|", 512);
        SeaTunnelRow deep = (SeaTunnelRow) value;
        assertThrows(IOException.class, () -> csv.serialize(deep));
    }

    @Test
    void preservesNativeDecimalKeysCollisionsAndPlainNumericValues() throws Exception {
        Map<BigDecimal, String> keys = new LinkedHashMap<>();
        keys.put(new BigDecimal("1E+100"), "compact");
        SeaTunnelRowType type =
                rowType(new MapType<>(new DecimalType(38, 0), BasicType.STRING_TYPE));
        assertEquals(
                "{\"value\":{\"1E+100\":\"compact\"}}",
                new String(
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", 512)
                                .serialize(new SeaTunnelRow(new Object[] {keys})),
                        StandardCharsets.UTF_8));
        Map<Object, String> collisions = new LinkedHashMap<>();
        collisions.put(1, "first");
        collisions.put("1", "last");
        type = rowType(new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE));
        assertEquals(
                "{\"value\":{\"1\":\"last\"}}",
                new String(
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", 512)
                                .serialize(new SeaTunnelRow(new Object[] {collisions})),
                        StandardCharsets.UTF_8));
        type = rowType(new DecimalType(38, 0));
        assertEquals(
                "{\"value\":1000}",
                new String(
                        new FirehoseRowSerializationSchema(type, Format.JSON, "|", 512)
                                .serialize(new SeaTunnelRow(new Object[] {new BigDecimal("1E+3")})),
                        StandardCharsets.UTF_8));
    }

    private static String decode(JsonNode request) {
        return new String(
                Base64.getDecoder().decode(request.at("/Records/0/Data").asText()),
                StandardCharsets.UTF_8);
    }

    private static SeaTunnelRowType rowType(SeaTunnelDataType<?> type) {
        return new SeaTunnelRowType(new String[] {"value"}, new SeaTunnelDataType<?>[] {type});
    }

    /**
     * An ordinary ordered map whose iterator makes early bounded admission observable to the test.
     */
    private static final class TrackingMap extends AbstractMap<String, String> {
        private final Map<String, String> values = new LinkedHashMap<>();
        private int visited;

        @Override
        public Set<Entry<String, String>> entrySet() {
            return new AbstractSet<Entry<String, String>>() {
                @Override
                public int size() {
                    return values.size();
                }

                @Override
                public Iterator<Entry<String, String>> iterator() {
                    Iterator<Entry<String, String>> entries = values.entrySet().iterator();
                    return new Iterator<Entry<String, String>>() {
                        @Override
                        public boolean hasNext() {
                            return entries.hasNext();
                        }

                        @Override
                        public Entry<String, String> next() {
                            visited++;
                            return entries.next();
                        }
                    };
                }
            };
        }
    }

    /**
     * A real local HTTP endpoint records calls made through the production SDK and owned transport.
     */
    private static final class Endpoint implements AutoCloseable {
        private final List<String> requests = new CopyOnWriteArrayList<>();
        private final HttpServer server;

        private Endpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext(
                    "/",
                    exchange -> {
                        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                        try (InputStream input = exchange.getRequestBody()) {
                            byte[] buffer = new byte[1024];
                            int count;
                            while ((count = input.read(buffer)) != -1) {
                                bytes.write(buffer, 0, count);
                            }
                        }
                        requests.add(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
                        byte[] response =
                                "{\"FailedPutCount\":0,\"RequestResponses\":[{\"RecordId\":\"accepted\"}]}"
                                        .getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders()
                                .set("Content-Type", "application/x-amz-json-1.1");
                        exchange.sendResponseHeaders(200, response.length);
                        try (OutputStream output = exchange.getResponseBody()) {
                            output.write(response);
                        }
                        exchange.close();
                    });
            server.start();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        private AmazonDataFirehoseSinkWriter writer(
                SeaTunnelRowType type, Format format, int bytes) {
            FirehoseBatchClient client =
                    new FirehoseBatchClient(
                            "test-stream",
                            "us-east-1",
                            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create("fixture-key", "fixture-secret")),
                            5000,
                            0);
            return new AmazonDataFirehoseSinkWriter(
                    client,
                    format,
                    new FirehoseRowSerializationSchema(type, format, "|", bytes),
                    type.getTotalFields(),
                    1,
                    bytes);
        }
    }
}
