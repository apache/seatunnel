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

package org.apache.seatunnel.e2e.connector.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Checks persisted Source rows and Sink datagrams through a real Zeta job. */
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 8, unit = TimeUnit.MINUTES)
class CoapIT extends TestSuiteBase {
    private static final int SOURCE_PORT = 17080;
    private static final int SINK_PORT = 17081;
    private static final int TEXT = 0;
    private static final int JSON = 50;
    private static final int BINARY = 42;
    private static final String PATH = "/sensor/data";
    private static final String SOURCE = "/coap-source.conf";
    private static final String NATIVE_SOURCE = "/coap-source-native.conf";
    private static final String SINK = "/coap-sink.conf";
    private static final String NATIVE_SINK = "/coap-sink-native.conf";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Engine engine = new Engine();
    private final ExecutorService jobs = Executors.newCachedThreadPool();

    @BeforeAll
    void start() throws Exception {
        engine.startUp();
    }

    @AfterAll
    void stop() throws Exception {
        jobs.shutdownNow();
        engine.tearDown();
    }

    @Test
    void sourceAcceptsGetPostPutAndDeduplicatesAMidFromTheSamePeer() throws Exception {
        String rich = "{\"id\":9223372036854775808,\"name\":\"测🌐\",\"values\":[12.34000,null]}";
        String duplicate = message(0, 2, 11, 11, PATH, JSON, -1, -1, utf8(rich));
        try (Running source = source(SOURCE, "json-methods", "BATCH", "JSON", 3, 8192, 64, 16)) {
            assertEquals(
                    Arrays.asList(67, 67, 132, 133, 143, 128, 67, 67),
                    exchange(
                            duplicate,
                            duplicate,
                            message(0, 2, 12, 12, "/unknown", JSON, -1, -1, utf8("{}")),
                            message(0, 4, 13, 13, PATH, -1, -1, new byte[0]),
                            message(0, 2, 14, 14, PATH, TEXT, -1, -1, utf8("{}")),
                            message(0, 2, 15, 15, PATH, JSON, -1, -1, utf8("{")),
                            message(1, 3, 16, 16, PATH, JSON, -1, -1, utf8("null")),
                            message(0, 1, 17, 17, PATH, JSON, -1, -1, utf8("\"get\""))));
            success(source.result());
            assertRows("json-methods", rich, "null", "\"get\"");
        }
    }

    @Test
    void sourceKeepsEmptyAndUnicodeTextAndRejectsInvalidUtf8() throws Exception {
        try (Running source = source(SOURCE, "text", "BATCH", "TEXT", 2, 8192, 16, 16)) {
            assertEquals(
                    Arrays.asList(128, 67, 67),
                    exchange(
                            message(
                                    0,
                                    2,
                                    51,
                                    1,
                                    PATH,
                                    TEXT,
                                    -1,
                                    -1,
                                    new byte[] {(byte) 0xc3, 0x28}),
                            message(0, 2, 52, 2, PATH, -1, -1, -1, new byte[0]),
                            message(0, 2, 53, 3, PATH, TEXT, -1, -1, utf8("纸🌱\nlast"))));
            success(source.result());
            assertRows("text", "", "纸🌱\nlast");
        }
    }

    @Test
    void sourceReassemblesBlocksAndRejectsOutOfOrderAndOversizedBodies() throws Exception {
        byte[] raw = bytes(54);
        try (Running source = source(SOURCE, "binary-blocks", "BATCH", "BINARY", 2, 1024, 16, 16)) {
            assertEquals(
                    Arrays.asList(136, 141, 95, 67, 67),
                    exchange(
                            message(
                                    0,
                                    2,
                                    61,
                                    1,
                                    PATH,
                                    BINARY,
                                    33,
                                    -1,
                                    Arrays.copyOfRange(raw, 32, raw.length)),
                            message(0, 2, 62, 2, PATH, BINARY, 9, 2048, Arrays.copyOf(raw, 32)),
                            message(
                                    0,
                                    2,
                                    63,
                                    3,
                                    PATH,
                                    BINARY,
                                    9,
                                    raw.length,
                                    Arrays.copyOf(raw, 32)),
                            message(
                                    0,
                                    2,
                                    64,
                                    3,
                                    PATH,
                                    BINARY,
                                    17,
                                    -1,
                                    Arrays.copyOfRange(raw, 32, raw.length)),
                            message(0, 2, 65, 4, PATH, BINARY, -1, -1, new byte[0])));
            success(source.result());
            assertRows("binary-blocks", Base64.getEncoder().encodeToString(raw), "");
        }
    }

    @Test
    void sourceBoundsQueueAndRefusesNewPeersAtCapacity() throws Exception {
        try (Running source = source(SOURCE, "burst", "STREAMING", "TEXT", 0, 1024, 1, 1)) {
            Container.ExecResult burst = engine.peer("burst", Integer.toString(SOURCE_PORT), "450");
            success(burst);
            Set<String> admitted = new HashSet<>();
            int refused = 0;
            for (String response : lines(burst.getStdout())) {
                String[] parts = response.split(":");
                if ("67".equals(parts[1])) {
                    assertTrue(admitted.add("burst-" + parts[0]));
                } else {
                    assertEquals("163", parts[1]);
                    refused++;
                }
            }
            assertFalse(admitted.isEmpty());
            assertTrue(refused > 0, () -> burst.getStdout());
            assertEquals(
                    Collections.singletonList(-1),
                    exchange(message(0, 2, 71, 1, PATH, TEXT, -1, -1, utf8("new-peer"))));
            awaitCheckpoint(source.id);
            success(engine.savepointJob(source.id));
            source.result();
            assertEquals(admitted, new HashSet<>(payloads("burst")));
            assertEquals(admitted.size(), payloads("burst").size());
        }
    }

    @Test
    void sourceSavepointRebindsTheListenerWithoutTreatingAPartialTransferAsARow() throws Exception {
        byte[] partial = bytes(32);
        String name = "restore";
        String[] options = sourceOptions(name, "BATCH", "BINARY", 2, 1024, 16, 16);
        try (Running source = source(SOURCE, name, "BATCH", "BINARY", 2, 1024, 16, 16)) {
            assertEquals(
                    Arrays.asList(67, 95),
                    exchangeFrom(
                            17082,
                            message(0, 2, 81, 1, PATH, BINARY, -1, -1, new byte[] {11, 12}),
                            message(0, 2, 82, 2, PATH, BINARY, 9, 54, partial)));
            awaitCheckpoint(source.id);
            success(engine.savepointJob(source.id));
            source.result();
            engine.awaitListener(false);
            assertRows(name, "Cww=");

            Future<Container.ExecResult> restored =
                    jobs.submit(() -> engine.restoreJob(SOURCE, source.id, options));
            try {
                engine.awaitListener(true);
                assertEquals(
                        Arrays.asList(136, 67),
                        exchangeFrom(
                                17082,
                                message(0, 2, 83, 2, PATH, BINARY, 17, -1, new byte[] {31, 32}),
                                message(
                                        0,
                                        2,
                                        84,
                                        3,
                                        PATH,
                                        BINARY,
                                        -1,
                                        -1,
                                        new byte[] {(byte) 0xFF})));
                success(restored.get(60, TimeUnit.SECONDS));
                engine.awaitListener(false);
                assertRows(name, "Cww=", "/w==");
            } finally {
                if (!restored.isDone()) {
                    engine.cancelJob(source.id);
                    restored.cancel(true);
                }
            }
        }
    }

    @Test
    void sourceCancelReleasesThePortDuringAnIncompleteBlockwiseExchange() throws Exception {
        try (Running source = source(SOURCE, "cancel", "STREAMING", "BINARY", 0, 1024, 16, 16)) {
            assertEquals(
                    Collections.singletonList(95),
                    exchange(message(0, 2, 91, 1, PATH, BINARY, 9, 54, bytes(32))));
            success(engine.cancelJob(source.id));
            source.result();
            engine.awaitListener(false);
            assertRows("cancel");
        }
        try (Running source = source(SOURCE, "rebind", "BATCH", "BINARY", 1, 1024, 16, 16)) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(message(0, 2, 92, 1, PATH, BINARY, -1, -1, new byte[] {7})));
            success(source.result());
            assertRows("rebind", "Bw==");
        }
    }

    @Test
    void sourceDecodesCsvAvroAndProtobufToExactNativeRows() throws Exception {
        nativeSource("CSV", TEXT, utf8("\"x\",42"));
        // Avro zigzag length 1, string x, zigzag int 42.
        nativeSource("AVRO", BINARY, new byte[] {2, 'x', 84});
        // Protobuf Wire.label = "x" and Wire.counter = 42.
        nativeSource("PROTOBUF", BINARY, new byte[] {10, 1, 'x', 16, 42});
        nativeSource("JSON", JSON, utf8("{\"label\":\"x\",\"counter\":42}"));
    }

    private void nativeSource(String format, int media, byte[] encoded) throws Exception {
        String name = "native-" + format.toLowerCase(java.util.Locale.ROOT);
        try (Running source = source(NATIVE_SOURCE, name, "BATCH", format, 1, 1024, 16, 16)) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(message(0, 2, 101, 1, PATH, media, -1, -1, encoded)));
            success(source.result());
            List<JsonNode> rows = rowNodes(name);
            assertEquals(1, rows.size());
            assertEquals("x", rows.get(0).path("label").asText());
            assertEquals(42, rows.get(0).path("counter").intValue());
        }
    }

    @Test
    void sinkUsesTheExactMethodTypeResourceAndBody() throws Exception {
        assertCaptureOnce("post", "POST", "CON", "TEXT", "", 2, 0, TEXT, new byte[0]);
        assertCaptureOnce("put", "PUT", "NON", "TEXT", "温度🌡", 3, 1, TEXT, utf8("温度🌡"));
        assertCaptureOnce("get", "GET", "CON", "JSON", "", 1, 0, -1, new byte[0]);
        assertCaptureOnce("delete", "DELETE", "NON", "JSON", "", 4, 1, -1, new byte[0]);
        try (Peer peer = listen("binary", "ok")) {
            success(runSink("/coap-sink-binary.conf", "BINARY", "PUT", "CON", "", 5000, 4096));
            List<Capture> frames = peer.captures();
            assertEquals(1, frames.size());
            frames.get(0).assertSent(3, 0, BINARY, new byte[] {0, (byte) 255, (byte) 128, 65});
        }
    }

    @Test
    void sinkSendsEveryRowWithExactDecimalAndNullValuesBeforeJobCompletion() throws Exception {
        try (Peer peer = listen("json-values", "ok")) {
            success(runSink("/coap-sink-json-values.conf", "JSON", "POST", "CON", "", 6000, 4096));
            List<Capture> records = peer.captures();
            assertEquals(2, records.size());
            checkJson(records.get(0), "测🌐", "12345678901234.12345678");
            checkJson(records.get(1), "ending", "0.01000000");
        }
    }

    private static void checkJson(Capture capture, String label, String exact) throws Exception {
        assertEquals(2, capture.method);
        assertEquals(JSON, capture.format);
        String body = new String(capture.body, StandardCharsets.UTF_8);
        JsonNode row = MAPPER.readTree(body);
        assertEquals(label, row.path("label").asText());
        assertTrue(row.path("missing").isNull());
        assertTrue(body.contains(exact), body);
    }

    @Test
    void sinkSerializesNativeCsvAvroAndProtobuf() throws Exception {
        nativeSink("CSV", TEXT, utf8("x,42"));
        // SeaTunnel Avro scalar fields use [null, value] unions.
        nativeSink("AVRO", BINARY, new byte[] {2, 2, 'x', 2, 84});
        nativeSink("PROTOBUF", BINARY, new byte[] {10, 1, 'x', 16, 42});
    }

    private void nativeSink(String format, int media, byte[] expected) throws Exception {
        try (Peer peer = listen("native-sink-" + format, "ok")) {
            success(runSink(NATIVE_SINK, format, "POST", "CON", "", 10000, 4096));
            List<Capture> rows = peer.captures();
            assertEquals(1, rows.size());
            rows.get(0).assertSent(2, 0, media, expected);
        }
    }

    @Test
    void sinkRetainsMidTokenAndPayloadThroughConfirmableRetransmission() throws Exception {
        try (Peer peer = listen("same-exchange", "drop-first")) {
            success(runSink(SINK, "TEXT", "PUT", "CON", "once", 12000, 4096));
            List<Capture> frames = peer.captures();
            assertEquals(2, frames.size());
            assertEquals(frames.get(0).mid, frames.get(1).mid);
            assertEquals(frames.get(0).token, frames.get(1).token);
            for (Capture frame : frames) {
                frame.assertSent(3, 0, TEXT, utf8("once"));
            }
        }
    }

    @Test
    void sinkFailsReset4xx5xxAndSilentNonWithoutABusinessReplay() throws Exception {
        failureOnce("reset", "CON", "remote reset");
        failureOnce("4xx", "CON", "remote response 4.00");
        failureOnce("5xx", "NON", "remote response 5.03");
        failureOnce("silent", "NON", "delivery is unknown");
    }

    private void failureOnce(String mode, String type, String reason) throws Exception {
        try (Peer peer = listen(mode, mode)) {
            Container.ExecResult result =
                    runSink(SINK, "TEXT", "POST", type, "sensitive-body", 900, 4096);
            failure(result, reason);
            assertFalse(output(result).contains("peer-response-redaction-marker"));
            assertEquals(1, peer.captures().size());
        }
    }

    @Test
    void sinkSendsOneCompleteBlockwiseBodyAndRejectsOversizedRowsBeforeIO() throws Exception {
        String payload = "Z🌳".repeat(420);
        try (Peer peer = listen("sink-blocks", "ok")) {
            success(runSink(SINK, "TEXT", "PUT", "CON", payload, 15000, 4096));
            List<Capture> frames = peer.captures();
            List<Capture> assembled = new ArrayList<>();
            int parts = 0;
            for (Capture frame : frames) {
                assertTrue(frame.block >= 0);
                if ("assembled".equals(frame.kind)) {
                    assembled.add(frame);
                } else {
                    parts++;
                }
            }
            assertTrue(parts > 1);
            assertEquals(1, assembled.size());
            assembled.get(0).assertSent(3, 0, TEXT, utf8(payload));
        }
        try (Peer peer = listen("too-large", "ok")) {
            failure(runSink(SINK, "TEXT", "PUT", "CON", payload, 10000, 512), "max_payload_bytes");
            assertTrue(peer.captures().isEmpty());
        }
    }

    @Test
    void sinkCancellationDuringAnUnansweredNonRequestDoesNotStartAnotherExchange()
            throws Exception {
        try (Peer peer = listen("held-sink", "hold")) {
            String id = id();
            Future<Container.ExecResult> task =
                    jobs.submit(
                            () ->
                                    engine.executeJob(
                                            SINK,
                                            id,
                                            sinkOptions(
                                                    "TEXT", "PUT", "NON", "hold", 10000, 4096)));
            try (Running sink = new Running(id, task)) {
                Awaitility.await()
                        .atMost(45, TimeUnit.SECONDS)
                        .until(() -> peer.captures().size() == 1);
                success(engine.cancelJob(id));
                sink.result();
                assertEquals(1, peer.captures().size());
            }
        }
    }

    @Test
    void sourceSelectsStructuredXmlRowsAndRejectsDoctype() throws Exception {
        String name = "xml-structured";
        try (Running source =
                formatSource(
                        "/coap-source-formats.conf",
                        name,
                        "XML",
                        1,
                        "RAW_DATA",
                        "COMMON_LOG_FORMAT")) {
            String unsafe =
                    "<!DOCTYPE events [<!ENTITY unsafe SYSTEM 'file:///unreadable'>]>"
                            + "<events><item><label>&unsafe;</label></item></events>";
            String good =
                    "<e:events xmlns:e=\"urn:fixture\"><e:item id=\"1\"><e:label>北🌑</e:label>"
                            + "<e:value>x</e:value><e:value>y</e:value></e:item><e:item id=\"2\">"
                            + "<e:label>south</e:label></e:item></e:events>";
            assertEquals(
                    Arrays.asList(128, 67),
                    exchange(
                            message(0, 2, 200, 1, PATH, 41, -1, -1, utf8(unsafe)),
                            message(0, 2, 201, 2, PATH, 41, -1, -1, utf8(good))));
            success(source.result());
            List<JsonNode> rows = decodedPayloads(name);
            assertEquals(2, rows.size());
            JsonNode first = rows.get(0).path("e:item");
            assertEquals("1", first.path("@id").asText());
            assertEquals("北🌑", first.path("e:label").asText());
            assertEquals(MAPPER.readTree("[\"x\",\"y\"]"), first.path("e:value"));
            assertEquals("south", rows.get(1).path("e:item").path("e:label").asText());
        }
    }

    @Test
    void sourceParsesApacheAccessErrorAndJavaLogLayouts() throws Exception {
        String access = "192.0.2.1 - alice [28/Nov/2026:01:00:00 +0000] \"GET /t HTTP/1.1\" 200 9";
        assertLog("COMMON_LOG_FORMAT", access, "request", "/t");
        assertLog(
                "COMBINED_LOG_FORMAT",
                access + " \"https://example.org/\" \"fixture-agent\"",
                "agent",
                "fixture-agent");
        assertLog("APACHE_CUSTOM_LOG_FORMAT", access, "response", "200");
        assertLog(
                "APACHE_ERROR_LOG_FORMAT",
                "[Sat Nov 28 01:00:00] [core:error] [pid 14]"
                        + " [client 192.0.2.1:8765] failed item",
                "level",
                "error");
        assertLog(
                "LOG4J",
                "2026-11-28 01:00:00,600 INFO  Fixture - processed",
                "message",
                "processed");
    }

    @Test
    void sourceParsesRegexGrokCefAndLeefWithoutAdmittingInvalidRecords() throws Exception {
        assertLog("REGEX", "answer=42", "code", "42", "coap.log_pattern=([a-z]+)=([0-9]+)");
        assertLog("GROK", "answer=42", "key", "answer", "coap.log_pattern=%{WORD:key}=%{INT:code}");
        assertLog(
                "CEF",
                "CEF:0|Example|Sensor|1|evt|Test|8|src=192.0.2.1 msg=two words",
                "extensions/msg",
                "two words");
        assertLog(
                "LEEF",
                "LEEF:2.0|Example|Sensor|1|evt|0x09|src=192.0.2.1\tsev=5",
                "extensions/sev",
                "5");
    }

    private void assertLog(
            String mode, String line, String field, String expected, String... extras)
            throws Exception {
        String name = "log-" + mode.toLowerCase(java.util.Locale.ROOT);
        String config =
                extras.length == 0 ? "/coap-source-formats.conf" : "/coap-source-custom-log.conf";
        try (Running source = formatSource(config, name, "LOG", 1, "RAW_DATA", mode, extras)) {
            assertEquals(
                    Arrays.asList(128, 67),
                    exchange(
                            message(0, 2, 210, 1, PATH, TEXT, -1, -1, new byte[0]),
                            message(0, 2, 211, 2, PATH, TEXT, -1, -1, utf8(line))));
            success(source.result());
            List<JsonNode> rows = decodedPayloads(name);
            assertEquals(1, rows.size());
            JsonNode selected = rows.get(0);
            for (String part : field.split("/")) {
                selected = selected.path(part);
            }
            assertEquals(expected, selected.asText());
        }
    }

    @Test
    void sourceDecodesSyslogAndRawDatagramRecords() throws Exception {
        try (Running source =
                formatSource(
                        "/coap-source-formats.conf",
                        "syslog",
                        "DATAGRAM",
                        1,
                        "SYSLOG",
                        "COMMON_LOG_FORMAT")) {
            String wire =
                    "<165>1 2026-11-28T01:00:00Z node app 91 msg"
                            + " [meta@32473 tag=\"a\\]b\"] hello🌑\n<34>Nov 28 01:01:00 legacy app[24]: tail";
            assertEquals(
                    Arrays.asList(128, 67),
                    exchange(
                            message(0, 2, 220, 1, PATH, BINARY, -1, -1, utf8("<999>invalid")),
                            message(0, 2, 221, 2, PATH, BINARY, -1, -1, utf8(wire))));
            success(source.result());
            List<JsonNode> rows = decodedPayloads("syslog");
            assertEquals(2, rows.size());
            assertEquals(20, rows.get(0).path("facility").intValue());
            assertEquals(5, rows.get(0).path("severity").intValue());
            assertEquals(
                    "a]b",
                    rows.get(0).path("structured_data").path("meta@32473").path("tag").asText());
            assertEquals("hello🌑", rows.get(0).path("message").asText());
            assertEquals(0, rows.get(1).path("version").intValue());
            assertEquals("24", rows.get(1).path("proc_id").asText());
            assertEquals("tail", rows.get(1).path("message").asText());
        }
        try (Running source =
                formatSource(
                        "/coap-source-formats.conf",
                        "datagram-separated",
                        "DATAGRAM",
                        1,
                        "RAW_DATA",
                        "COMMON_LOG_FORMAT")) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(message(0, 2, 222, 1, PATH, BINARY, -1, -1, utf8("x||测|"))));
            success(source.result());
            assertRows("datagram-separated", "x", "", "测", "");
        }
        try (Running source =
                source(SOURCE, "datagram-raw", "BATCH", "DATAGRAM", 1, 1024, 16, 16)) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(
                            message(
                                    0,
                                    2,
                                    223,
                                    1,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    new byte[] {0, (byte) 255})));
            success(source.result());
            assertRows("datagram-raw", "AP8=");
        }
    }

    @Test
    void sourceDecodesNetflowV5AndV9TemplatesOptionsAndDataByExporter() throws Exception {
        String name = "netflow";
        try (Running source =
                formatSource(
                        "/coap-source-formats.conf",
                        name,
                        "DATAGRAM",
                        5,
                        "NETFLOW",
                        "COMMON_LOG_FORMAT")) {
            assertEquals(
                    Arrays.asList(128, 67, 128, 67, 67, 67, 67),
                    exchange(
                            message(
                                    0,
                                    2,
                                    230,
                                    1,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9Data(123)),
                            message(
                                    0,
                                    2,
                                    231,
                                    2,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9Templates()),
                            message(
                                    0,
                                    2,
                                    232,
                                    3,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9Data(124)),
                            message(0, 2, 233, 4, PATH, BINARY, -1, -1, CoapWireFixtures.flow5()),
                            message(
                                    0,
                                    2,
                                    234,
                                    5,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9Data(123)),
                            message(
                                    0,
                                    2,
                                    235,
                                    6,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9ShortTemplate()),
                            message(
                                    0,
                                    2,
                                    236,
                                    7,
                                    PATH,
                                    BINARY,
                                    -1,
                                    -1,
                                    CoapWireFixtures.flow9ShortData())));
            success(source.result());
            List<JsonNode> rows = decodedPayloads(name);
            rows.sort(java.util.Comparator.comparingInt(row -> row.path("template_id").asInt()));
            assertEquals(4, rows.size());
            assertEquals(5, rows.get(0).path("version").intValue());
            assertEquals("192.0.2.1", rows.get(0).path("src_addr").asText());
            assertEquals("4294967295", rows.get(0).path("packets").asText());
            assertEquals(443, rows.get(0).path("dst_port").intValue());
            assertEquals(256, rows.get(1).path("template_id").intValue());
            assertEquals("192.0.2.40", rows.get(1).path("fields").get(0).path("value").asText());
            assertEquals(
                    "18446744073709551615",
                    rows.get(1).path("fields").get(1).path("value").asText());
            assertTrue(rows.get(2).path("options").asBoolean());
            assertTrue(rows.get(2).path("fields").get(0).path("scope").asBoolean());
            assertEquals(19, rows.get(2).path("fields").get(0).path("value").intValue());
            assertEquals(10000, rows.get(2).path("fields").get(1).path("value").intValue());
            assertEquals(258, rows.get(3).path("template_id").intValue());
            assertEquals(1, rows.get(3).path("fields").size());
            assertEquals(6, rows.get(3).path("fields").get(0).path("value").intValue());
        }
    }

    @Test
    void sourceDecodesCollectdAndVerifiesSignedAndEncryptedMetricValues() throws Exception {
        byte[] plain = CoapWireFixtures.collectd();
        try (Running source =
                formatSource(
                        "/coap-source-formats.conf",
                        "collectd-plain",
                        "DATAGRAM",
                        1,
                        "COLLECTD",
                        "COMMON_LOG_FORMAT")) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(message(0, 2, 240, 1, PATH, BINARY, -1, -1, plain)));
            success(source.result());
            List<JsonNode> rows = decodedPayloads("collectd-plain");
            assertEquals(1, rows.size());
            checkCollectd(rows.get(0));
        }
        byte[] signed = CoapWireFixtures.signedCollectd(plain);
        byte[] encrypted = CoapWireFixtures.encryptedCollectd(plain);
        byte[] corrupt = encrypted.clone();
        corrupt[corrupt.length - 1] ^= 1;
        try (Running source =
                formatSource(
                        "/coap-source-collectd-auth.conf",
                        "collectd-auth",
                        "DATAGRAM",
                        2,
                        "COLLECTD",
                        "COMMON_LOG_FORMAT")) {
            assertEquals(
                    Arrays.asList(128, 128, 67, 67),
                    exchange(
                            message(0, 2, 241, 1, PATH, BINARY, -1, -1, plain),
                            message(0, 2, 242, 2, PATH, BINARY, -1, -1, corrupt),
                            message(0, 2, 243, 3, PATH, BINARY, -1, -1, signed),
                            message(0, 2, 244, 4, PATH, BINARY, -1, -1, encrypted)));
            success(source.result());
            List<JsonNode> rows = decodedPayloads("collectd-auth");
            assertEquals(2, rows.size());
            checkCollectd(rows.get(0));
            assertEquals(rows.get(0), rows.get(1));
        }
    }

    private static void checkCollectd(JsonNode value) {
        assertEquals("node-01", value.path("host").asText());
        assertEquals(1024000, value.path("time").longValue());
        assertEquals(1500, value.path("interval").longValue());
        JsonNode values = value.path("values");
        assertEquals("packets", values.get(0).path("name").asText());
        assertEquals("4294967295", values.get(0).path("value").asText());
        assertEquals(1.25, values.get(1).path("value").doubleValue());
        assertEquals(-5, values.get(2).path("value").intValue());
        assertEquals("18446744073709551615", values.get(3).path("value").asText());
    }

    @Test
    void sourceUnpacksTypedRecordEnvelopesAndPreservesHeadersAndValues() throws Exception {
        String first =
                "{\"header\":{\"sourceId\":\"origin-1\",\"values\":{\"team\":\"edge\"}},"
                        + "\"value\":{\"type\":\"MAP\",\"value\":{\"n\":{\"type\":\"LONG\",\"value\":\"9223372036854775807\"},"
                        + "\"data\":{\"type\":\"BYTE_ARRAY\",\"value\":\"AP8=\"},"
                        + "\"missing\":{\"type\":\"STRING\",\"value\":null}}}}";
        String second =
                "{\"header\":{\"sourceId\":\"origin-2\"},\"value\":{\"type\":\"LIST_MAP\",\"value\":["
                        + "{\"type\":\"STRING\",\"sqpath\":\"/'r/name'\",\"value\":\"北🌑\"},"
                        + "{\"type\":\"DECIMAL\",\"sqpath\":\"/precise\",\"value\":12345678901234.12345678}]}}";
        try (Running source =
                source(SOURCE, "typed-envelope", "BATCH", "RECORD_JSON", 1, 8192, 16, 16)) {
            assertEquals(
                    Arrays.asList(128, 67),
                    exchange(
                            message(0, 2, 250, 1, PATH, JSON, -1, -1, utf8("{\"value\":4}")),
                            message(
                                    0,
                                    2,
                                    251,
                                    2,
                                    PATH,
                                    JSON,
                                    -1,
                                    -1,
                                    utf8(first + "\n" + second + "\n"))));
            success(source.result());
            List<String> rows = payloads("typed-envelope");
            assertEquals(2, rows.size());
            JsonNode one = MAPPER.readTree(rows.get(0));
            assertEquals("edge", one.path("header").path("values").path("team").asText());
            assertEquals("9223372036854775807", one.path("value").path("n").asText());
            assertEquals("AP8=", one.path("value").path("data").asText());
            assertTrue(one.path("value").path("missing").isNull());
            JsonNode two = MAPPER.readTree(rows.get(1));
            assertEquals("北🌑", two.path("value").path("r/name").asText());
            assertTrue(rows.get(1).contains("12345678901234.12345678"));
        }
        try (Running source =
                source(
                        "/coap-source-typed-record.conf",
                        "typed-envelope-schema",
                        "BATCH",
                        "RECORD_JSON",
                        1,
                        8192,
                        16,
                        16)) {
            assertEquals(
                    Collections.singletonList(67),
                    exchange(message(0, 2, 252, 1, PATH, JSON, -1, -1, utf8(first))));
            success(source.result());
            List<JsonNode> rows = rowNodes("typed-envelope-schema");
            assertEquals(1, rows.size());
            assertEquals("9223372036854775807", rows.get(0).path("n").asText());
            assertEquals("AP8=", rows.get(0).path("data").asText());
            assertEquals("origin-1", rows.get(0).path("_header").path("sourceId").asText());
        }
    }

    @Test
    void sinkWritesSelfDescribingAvroAndTypedJsonEnvelopes() throws Exception {
        try (Peer peer = listen("avro-container", "ok")) {
            success(runSink("/coap-sink-container.conf", "AVRO", "POST", "CON", "", 10000, 8192));
            List<Capture> captured = peer.captures();
            assertEquals(1, captured.size());
            byte[] bytes = captured.get(0).body;
            assertArrayEquals(new byte[] {'O', 'b', 'j', 1}, Arrays.copyOf(bytes, 4));
            try (org.apache.avro.file.DataFileStream<org.apache.avro.generic.GenericRecord> file =
                    new org.apache.avro.file.DataFileStream<>(
                            new java.io.ByteArrayInputStream(bytes),
                            new org.apache.avro.generic.GenericDatumReader<>())) {
                assertEquals(2, file.getSchema().getFields().size());
                assertTrue(file.hasNext());
                org.apache.avro.generic.GenericRecord record = file.next();
                assertEquals("x", record.get("label").toString());
                assertEquals(42, record.get("counter"));
                assertFalse(file.hasNext());
            }
            assertEquals(BINARY, captured.get(0).format);
        }
        try (Peer peer = listen("typed-envelope", "ok")) {
            success(
                    runSink(
                            "/coap-sink-json-values.conf",
                            "RECORD_JSON",
                            "PUT",
                            "CON",
                            "",
                            10000,
                            8192));
            List<Capture> records = peer.captures();
            assertEquals(2, records.size());
            String payload = new String(records.get(0).body, StandardCharsets.UTF_8);
            assertTrue(payload.endsWith("\n"));
            JsonNode envelope = MAPPER.readTree(payload);
            assertEquals(3, records.get(0).method);
            assertEquals(JSON, records.get(0).format);
            assertEquals("CoAP", envelope.path("header").path("stagesPath").asText());
            assertFalse(envelope.path("header").path("sourceId").asText().isEmpty());
            assertEquals("MAP", envelope.path("value").path("type").asText());
            JsonNode value = envelope.path("value").path("value");
            assertEquals("STRING", value.path("label").path("type").asText());
            assertEquals("测🌐", value.path("label").path("value").asText());
            assertEquals("DECIMAL", value.path("exact").path("type").asText());
            assertTrue(payload.contains("12345678901234.12345678"));
            assertTrue(value.path("missing").path("value").isNull());
        }
    }

    private Running formatSource(
            String config,
            String name,
            String format,
            int count,
            String datagram,
            String logMode,
            String... extraVariables)
            throws Exception {
        List<String> variables =
                new ArrayList<>(
                        Arrays.asList(sourceOptions(name, "BATCH", format, count, 8192, 16, 16)));
        variables.add("coap.datagram_mode=" + datagram);
        variables.add("coap.log_mode=" + logMode);
        variables.add("coap.xml_path=/e:events/e:item");
        variables.addAll(Arrays.asList(extraVariables));
        return startSource(config, variables.toArray(new String[0]));
    }

    private List<JsonNode> decodedPayloads(String name) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (String body : payloads(name)) {
            rows.add(MAPPER.readTree(body));
        }
        return rows;
    }

    private void assertCaptureOnce(
            String name,
            String method,
            String type,
            String format,
            String text,
            int expectedMethod,
            int expectedType,
            int expectedFormat,
            byte[] body)
            throws Exception {
        try (Peer peer = listen(name, "ok")) {
            success(runSink(SINK, format, method, type, text, 5000, 4096));
            List<Capture> rows = peer.captures();
            assertEquals(1, rows.size());
            rows.get(0).assertSent(expectedMethod, expectedType, expectedFormat, body);
        }
    }

    private void awaitCheckpoint(String id) {
        long initial = engine.getCompletedCheckpointCount(id);
        Awaitility.await()
                .atMost(25, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(id) > initial);
    }

    private Running source(
            String config,
            String name,
            String jobMode,
            String format,
            int limit,
            int bytes,
            int queue,
            int peers)
            throws Exception {
        return startSource(
                config, sourceOptions(name, jobMode, format, limit, bytes, queue, peers));
    }

    private Running startSource(String config, String[] options) throws Exception {
        String id = id();
        Future<Container.ExecResult> task =
                jobs.submit(() -> engine.executeJob(config, id, options));
        Running running = new Running(id, task);
        try {
            engine.awaitListener(true);
            return running;
        } catch (RuntimeException e) {
            running.close();
            throw e;
        }
    }

    private Container.ExecResult runSink(
            String config,
            String format,
            String method,
            String type,
            String payload,
            int timeout,
            int bytes)
            throws Exception {
        return engine.executeJob(
                config, Arrays.asList(sinkOptions(format, method, type, payload, timeout, bytes)));
    }

    private static String[] sourceOptions(
            String name, String mode, String format, int limit, int bytes, int queue, int peers) {
        return new String[] {
            "coap.name=" + name,
            "coap.job_mode=" + mode,
            "coap.format=" + format,
            "coap.limit=" + limit,
            "coap.bytes=" + bytes,
            "coap.queue=" + queue,
            "coap.peers=" + peers
        };
    }

    private static String[] sinkOptions(
            String format, String method, String type, String payload, int timeout, int bytes) {
        return new String[] {
            "coap.format=" + format,
            "coap.method=" + method,
            "coap.type=" + type,
            "coap.payload=" + payload,
            "coap.timeout=" + timeout,
            "coap.bytes=" + bytes
        };
    }

    private List<Integer> exchange(String... messages) throws Exception {
        return exchangeFrom(0, messages);
    }

    private List<Integer> exchangeFrom(int from, String... messages) throws Exception {
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                from == 0 ? "send" : "send-from", Integer.toString(SOURCE_PORT)));
        if (from != 0) {
            command.add(Integer.toString(from));
        }
        command.addAll(Arrays.asList(messages));
        Container.ExecResult result = engine.peer(command.toArray(new String[0]));
        success(result);
        List<Integer> codes = new ArrayList<>();
        for (String response : lines(result.getStdout())) {
            codes.add("timeout".equals(response) ? -1 : Integer.parseInt(response));
        }
        return codes;
    }

    private static String message(
            int type,
            int code,
            int mid,
            int token,
            String path,
            int content,
            int block,
            int size,
            byte[] payload) {
        return String.join(
                ",",
                Integer.toString(type),
                Integer.toString(code),
                Integer.toString(mid),
                Integer.toString(token),
                path,
                Integer.toString(content),
                Integer.toString(block),
                Integer.toString(size),
                Base64.getEncoder().encodeToString(payload));
    }

    private void assertRows(String name, String... expected) throws Exception {
        List<String> actual = payloads(name);
        List<String> wanted = new ArrayList<>(Arrays.asList(expected));
        Collections.sort(actual);
        Collections.sort(wanted);
        assertEquals(wanted, actual);
    }

    private List<String> payloads(String name) throws Exception {
        List<String> payloads = new ArrayList<>();
        for (JsonNode row : rowNodes(name)) {
            assertTrue(row.path("payload").isTextual(), row.toString());
            payloads.add(row.path("payload").textValue());
        }
        return payloads;
    }

    private List<JsonNode> rowNodes(String name) throws Exception {
        String dir = "/tmp/seatunnel_mnt/coap-" + name;
        Container.ExecResult result =
                engine.shell(
                        "if [ -d "
                                + dir
                                + " ]; then find "
                                + dir
                                + " -maxdepth 1 -type f -name 'receipt_*' "
                                + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
        success(result);
        List<JsonNode> rows = new ArrayList<>();
        for (String line : lines(result.getStdout())) {
            rows.add(MAPPER.readTree(line));
        }
        return rows;
    }

    private Peer listen(String name, String behavior) throws Exception {
        String journal = "/tmp/coap-peer-" + name;
        Future<Container.ExecResult> process =
                jobs.submit(
                        () ->
                                engine.peer(
                                        "listen", Integer.toString(SINK_PORT), behavior, journal));
        Peer peer = new Peer(journal, process);
        try {
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .until(() -> engine.shell("test -f " + journal + ".ready").getExitCode() == 0);
            return peer;
        } catch (RuntimeException e) {
            peer.close();
            throw e;
        }
    }

    private static String id() {
        return String.valueOf(ThreadLocalRandom.current().nextLong(1_000_000_000L, 9_000_000_000L));
    }

    private static byte[] bytes(int length) {
        byte[] output = new byte[length];
        for (int i = 0; i < length; i++) {
            output[i] = (byte) (i ^ 0xa5);
        }
        return output;
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static List<String> lines(String text) {
        List<String> rows = new ArrayList<>();
        for (String line : text.split("\\r?\\n")) {
            if (!line.trim().isEmpty()) {
                rows.add(line);
            }
        }
        return rows;
    }

    private static String output(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void success(Container.ExecResult result) {
        assertEquals(0, result.getExitCode(), () -> output(result));
    }

    private static void failure(Container.ExecResult result, String reason) {
        assertNotEquals(0, result.getExitCode(), () -> output(result));
        assertTrue(output(result).contains(reason), () -> output(result));
    }

    /** Releases a job even when a protocol assertion fails. */
    private final class Running implements AutoCloseable {
        private final String id;
        private final Future<Container.ExecResult> task;

        private Running(String id, Future<Container.ExecResult> task) {
            this.id = id;
            this.task = task;
        }

        Container.ExecResult result() throws Exception {
            return task.get(45, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            if (!task.isDone()) {
                try {
                    engine.cancelJob(id);
                } finally {
                    task.cancel(true);
                }
            }
        }
    }

    /** Owns the independent listening peer and exposes wire receipts for assertions. */
    private final class Peer implements AutoCloseable {
        private final String journal;
        private final Future<Container.ExecResult> process;

        private Peer(String journal, Future<Container.ExecResult> process) {
            this.journal = journal;
            this.process = process;
        }

        private List<Capture> captures() throws Exception {
            Container.ExecResult read =
                    engine.shell("if [ -f " + journal + " ]; then cat " + journal + "; fi");
            success(read);
            List<Capture> captures = new ArrayList<>();
            for (String entry : lines(read.getStdout())) {
                captures.add(new Capture(entry));
            }
            return captures;
        }

        @Override
        public void close() throws Exception {
            success(engine.shell("touch " + journal + ".stop"));
            success(process.get(10, TimeUnit.SECONDS));
        }
    }

    /** One recorded packet or independently assembled body from the UDP peer. */
    private static final class Capture {
        private final String kind;
        private final int method;
        private final int type;
        private final int mid;
        private final String token;
        private final String path;
        private final int format;
        private final int block;
        private final byte[] body;

        private Capture(String journalLine) {
            String[] fields = journalLine.split("\\t", -1);
            kind = fields[0];
            method = Integer.parseInt(fields[1]);
            type = Integer.parseInt(fields[2]);
            mid = Integer.parseInt(fields[3]);
            token = fields[4];
            path = fields[5];
            format = Integer.parseInt(fields[6]);
            block = Integer.parseInt(fields[7]);
            body = Base64.getDecoder().decode(fields[8]);
        }

        private void assertSent(
                int expectedMethod, int expectedType, int expectedFormat, byte[] wanted) {
            assertEquals(expectedMethod, method);
            assertEquals(expectedType, type);
            assertEquals(PATH, path);
            assertEquals(expectedFormat, format);
            assertArrayEquals(wanted, body);
        }
    }

    /** Stages the independent peer next to the Zeta JVM and exposes listener state. */
    private static final class Engine extends SeaTunnelContainer {
        private static final String PEER_CLASS = CoapDatagramPeer.class.getName();
        private static final String PEER_ROOT = "/tmp/coap-test-peer";

        @Override
        protected void executeExtraCommands(GenericContainer<?> runtime) throws IOException {
            String packagePath =
                    "/" + CoapDatagramPeer.class.getPackage().getName().replace('.', '/') + "/";
            for (String file :
                    Arrays.asList("CoapDatagramPeer.class", "CoapDatagramPeer$Frame.class")) {
                try {
                    runtime.withCopyFileToContainer(
                            MountableFile.forHostPath(
                                    Paths.get(CoapDatagramPeer.class.getResource(file).toURI())),
                            PEER_ROOT + packagePath + file);
                } catch (URISyntaxException e) {
                    throw new IOException("cannot copy the independent UDP test peer", e);
                }
            }
        }

        private Container.ExecResult peer(String... args) throws Exception {
            List<String> command =
                    new ArrayList<>(Arrays.asList("java", "-cp", PEER_ROOT, PEER_CLASS));
            command.addAll(Arrays.asList(args));
            return server.execInContainer(command.toArray(new String[0]));
        }

        private Container.ExecResult shell(String command) throws Exception {
            return server.execInContainer("sh", "-c", command);
        }

        private void awaitListener(boolean bound) {
            String hex = String.format(java.util.Locale.ROOT, "%04X", SOURCE_PORT);
            Awaitility.await()
                    .atMost(45, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    (shell("grep -q ':" + hex + " ' /proc/net/udp").getExitCode()
                                                    == 0)
                                            == bound);
        }
    }
}
