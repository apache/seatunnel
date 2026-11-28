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

package org.apache.seatunnel.e2e.connector.udp;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the packaged UDP source in Zeta. A separate UDP sender feeds exact datagrams and assertions
 * read the LocalFile sink's committed JSON records.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 6, unit = TimeUnit.MINUTES)
class UdpSourceIT extends TestSuiteBase {
    private static final String CONFIG = "/udp_to_localfile.conf";
    private static final int FIRST = 23040;
    private static final int SECOND = 23041;
    private static final int SENDER_A = 23100;
    private static final int SENDER_B = 23101;
    private static final String SENDER_CLASS = UdpDatagramSender.class.getName();
    private static final String FIXTURE_CLASSES = "/tmp/udp-source-fixture";
    private static final String FIXTURE_CLASS = SENDER_CLASS.replace('.', '/') + ".class";
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ExecutorService jobs = Executors.newFixedThreadPool(2);
    private UdpZeta engine;

    @BeforeAll
    void startEngine() throws Exception {
        engine = new UdpZeta();
        engine.startUp();
    }

    @AfterAll
    void stopEngine() throws Exception {
        jobs.shutdownNow();
        if (engine != null) {
            engine.tearDown();
        }
    }

    @Test
    void preservesWholeDatagramsEmptyTextAndTwoIndependentPortOwners() throws Exception {
        Job job = start("TEXT", "", 16, 8, 4500, false, "BATCH", 2, FIRST, SECOND);
        awaitListener(job, FIRST);
        awaitListener(job, SECOND);
        assertTrue(jobLogs(job).contains("UDP listener ready on port " + FIRST + " (reader 0)"));
        assertTrue(jobLogs(job).contains("UDP listener ready on port " + SECOND + " (reader 1)"));
        send(
                SENDER_A,
                FIRST,
                utf8("雪\n🦊"),
                new byte[0],
                new byte[] {(byte) 0xC3, 0x28},
                new byte[17],
                utf8("after-drop"));
        send(SENDER_B, SECOND, utf8("other"));
        List<JsonNode> rows = finish(job);
        assertEquals(4, rows.size());
        Map<String, JsonNode> byPayload =
                rows.stream()
                        .collect(
                                Collectors.toMap(
                                        row -> row.path("payload").textValue(), row -> row));
        assertEquals(
                new HashSet<>(Arrays.asList("雪\n🦊", "", "after-drop", "other")),
                byPayload.keySet());
        assertMetadata(byPayload.get("雪\n🦊"), SENDER_A, FIRST, 0);
        assertMetadata(byPayload.get(""), SENDER_A, FIRST, 0);
        assertMetadata(byPayload.get("after-drop"), SENDER_A, FIRST, 0);
        assertMetadata(byPayload.get("other"), SENDER_B, SECOND, 0);
    }

    @Test
    void preservesBinaryBytesAndZeroLength() throws Exception {
        Job job = start("BINARY", "", 4, 8, 3500, false, "BATCH", 1, FIRST);
        awaitListener(job, FIRST);
        byte[] bytes = {0, (byte) 255, (byte) 128, 10};
        send(SENDER_A, FIRST, bytes, new byte[0]);
        List<JsonNode> rows = finish(job);
        assertEquals(2, rows.size());
        rows.sort(
                Comparator.comparingInt((JsonNode row) -> row.path("payload").asText().length())
                        .reversed());
        assertArrayEquals(bytes, rows.get(0).path("payload").binaryValue());
        assertArrayEquals(new byte[0], rows.get(1).path("payload").binaryValue());
        assertMetadata(rows.get(0), SENDER_A, FIRST, 0);
        assertMetadata(rows.get(1), SENDER_A, FIRST, 0);
    }

    @Test
    void separatesWithinDatagramsAndDropsEntireOverflowDuringBurst() throws Exception {
        Job job = start("TEXT", "|", 64, 2, 4500, false, "BATCH", 1, FIRST);
        awaitListener(job, FIRST);
        List<byte[]> payloads = new ArrayList<>();
        payloads.add(utf8("discard1|discard2|discard3"));
        payloads.add(utf8("|first||last|"));
        payloads.add(utf8("boundary"));
        payloads.add(utf8("|"));
        for (int i = 0; i < 25; i++) {
            payloads.add(utf8("burst-" + i));
        }
        send(SENDER_A, FIRST, payloads.toArray(new byte[0][]));
        List<JsonNode> rows = finish(job);
        List<String> expected = new ArrayList<>(Arrays.asList("first", "last", "boundary"));
        for (int i = 0; i < 25; i++) {
            expected.add("burst-" + i);
        }
        expected.sort(String::compareTo);
        assertEquals(
                expected,
                rows.stream()
                        .map(row -> row.path("payload").textValue())
                        .sorted()
                        .collect(Collectors.toList()));
        Map<String, JsonNode> byPayload =
                rows.stream()
                        .collect(
                                Collectors.toMap(
                                        row -> row.path("payload").textValue(), row -> row));
        assertMetadata(byPayload.get("first"), SENDER_A, FIRST, 0);
        assertMetadata(byPayload.get("last"), SENDER_A, FIRST, 1);
        assertMetadata(byPayload.get("boundary"), SENDER_A, FIRST, 0);
    }

    @Test
    void extractsSyslogPriorityHeadersStructuredDataAndLegacyTag() throws Exception {
        Job job = start("SYSLOG", "", 2048, 4, 3500, false, "BATCH", 1, FIRST);
        awaitListener(job, FIRST);
        String structured = "[example@32473 x=\"a\\]b\\\"c\"][second]";
        send(
                SENDER_A,
                FIRST,
                utf8(
                        "<165>1 2026-11-27T12:15:12.123456Z host app 42 ID47 "
                                + structured
                                + " \uFEFF雪"),
                utf8("<34>Oct  8 22:14:15 legacy app9[99]: exact legacy"),
                utf8("<165>1 2026-11-27T12:15:12Z host app 42 ID47 [broken x=\"unterminated"),
                utf8("<13>1 - - - - - -"));
        List<JsonNode> rows = finish(job);
        assertEquals(3, rows.size());
        Map<String, JsonNode> byMessage =
                rows.stream()
                        .collect(
                                Collectors.toMap(
                                        row -> row.path("message").asText(""), row -> row));
        JsonNode modern = byMessage.get("雪");
        assertEquals(
                JSON.readTree(
                        "{\"facility\":20,\"severity\":5,\"version\":1,"
                                + "\"event_time\":\"2026-11-27T12:15:12.123456Z\",\"hostname\":\"host\",\"app_name\":\"app\","
                                + "\"process_id\":\"42\",\"message_id\":\"ID47\",\"structured_data\":"
                                + JSON.writeValueAsString(structured)
                                + ",\"message\":\"雪\","
                                + "\"sender_host\":\"127.0.0.1\",\"sender_port\":23100,\"listen_port\":23040,\"record_index\":0}"),
                modern);
        JsonNode legacy = byMessage.get("exact legacy");
        assertEquals("Oct  8 22:14:15", legacy.path("event_time").asText());
        assertEquals("legacy", legacy.path("hostname").asText());
        assertEquals("app9", legacy.path("app_name").asText());
        assertEquals("99", legacy.path("process_id").asText());
        assertEquals("exact legacy", legacy.path("message").asText());
        assertTrue(legacy.path("version").isNull());
        assertTrue(byMessage.get("").path("message").isNull());
        assertTrue(byMessage.get("").path("structured_data").isNull());
    }

    @Test
    void decodesWireCollectdMetricsWithoutLosingUnsignedValues() throws Exception {
        Job job = start("COLLECTD", "", 2048, 8, 3500, false, "BATCH", 1, FIRST);
        awaitListener(job, FIRST);
        byte[] datagram = collectd();
        byte[] broken = Arrays.copyOf(datagram, datagram.length + 3);
        send(SENDER_A, FIRST, broken, datagram, new byte[] {2, 0, 0, 4});
        List<JsonNode> rows = finish(job);
        assertEquals(4, rows.size());
        rows.sort(Comparator.comparingInt(row -> row.path("value_index").asInt()));
        assertEquals("fixture-host", rows.get(0).path("hostname").asText());
        assertEquals("temperature", rows.get(0).path("plugin").asText());
        assertEquals("cpu", rows.get(0).path("metric_type").asText());
        assertEquals("eth0", rows.get(0).path("plugin_instance").asText());
        assertEquals("sample", rows.get(0).path("type_instance").asText());
        assertEquals(1700000000500L, rows.get(0).path("time_ms").asLong());
        assertEquals(2500, rows.get(0).path("interval_ms").asInt());
        assertEquals(
                Arrays.asList("COUNTER", "GAUGE", "DERIVE", "ABSOLUTE"),
                rows.stream()
                        .map(row -> row.path("value_type").asText())
                        .collect(Collectors.toList()));
        assertEquals(
                new BigInteger("18446744073709551615"),
                new BigInteger(rows.get(0).path("integer_value").asText()));
        assertEquals(12.25, rows.get(1).path("gauge_value").asDouble());
        assertEquals(-42, rows.get(2).path("integer_value").asInt());
        assertEquals(109, rows.get(3).path("integer_value").asInt());
        for (int index = 0; index < rows.size(); index++) {
            assertEquals(index, rows.get(index).path("value_index").asInt());
            assertMetadata(rows.get(index), SENDER_A, FIRST, index);
        }
    }

    @Test
    void decodesTwoNetflowRecordsAndDropsATruncatedOrWrongVersionPacket() throws Exception {
        Job job = start("NETFLOW_V5", "", 2048, 8, 3500, false, "BATCH", 1, FIRST);
        awaitListener(job, FIRST);
        byte[] packet = netflow();
        send(
                SENDER_A,
                FIRST,
                Arrays.copyOf(packet, packet.length - 1),
                packet,
                new byte[] {0, 9, 0, 1});
        List<JsonNode> rows = finish(job);
        assertEquals(2, rows.size());
        rows.sort(Comparator.comparingInt(row -> row.path("record_index").asInt()));
        assertEquals("192.0.2.1", rows.get(0).path("source_ip").asText());
        assertEquals("203.0.113.2", rows.get(0).path("destination_ip").asText());
        assertEquals("198.51.100.3", rows.get(0).path("next_hop").asText());
        assertEquals(4294967295L, rows.get(0).path("packets").asLong());
        assertEquals(3000, rows.get(0).path("octets").asLong());
        assertEquals(4294967294L, rows.get(0).path("flow_sequence").asLong());
        assertEquals(2, rows.get(0).path("sampling_mode").asInt());
        assertEquals(600, rows.get(0).path("sampling_interval").asInt());
        assertEquals(65530, rows.get(0).path("source_port").asInt());
        assertEquals(443, rows.get(0).path("destination_port").asInt());
        assertEquals(17, rows.get(1).path("protocol").asInt());
        assertMetadata(rows.get(0), SENDER_A, FIRST, 0);
        assertMetadata(rows.get(1), SENDER_A, FIRST, 1);
    }

    @Test
    void stopsAfterInactivityWithoutReceivingAnyDatagram() throws Exception {
        Job job = start("TEXT", "", 64, 2, 800, false, "BATCH", 1, FIRST);
        assertTrue(finish(job).isEmpty());
        assertEquals(0, engine.sender("probe", Integer.toString(FIRST)).getExitCode());
    }

    @Test
    void failsOnMalformedWhenConfiguredAndReleasesTheListener() throws Exception {
        Job job = start("TEXT", "", 64, 2, 0, true, "STREAMING", 1, FIRST);
        awaitListener(job, FIRST);
        send(SENDER_A, FIRST, new byte[] {(byte) 0xFE});
        Container.ExecResult result = job.result.get(40, TimeUnit.SECONDS);
        assertTrue(result.getExitCode() != 0 || "FAILED".equals(engine.getJobStatus(job.id)));
        assertTrue(
                (result.getStderr() + result.getStdout() + jobLogs(job)).contains("invalid UTF-8"));
        assertTrue(receipts(job.output).isEmpty());
        assertEquals(0, engine.sender("probe", Integer.toString(FIRST)).getExitCode());
    }

    @Test
    void rejectsASecondPortOwnerAndCancelsIdleListener() throws Exception {
        Job original = start("TEXT", "", 64, 4, 0, false, "STREAMING", 1, FIRST);
        try {
            awaitListener(original, FIRST);
            Job conflict = start("TEXT", "", 64, 4, 0, false, "STREAMING", 1, FIRST);
            Container.ExecResult rejected = conflict.result.get(45, TimeUnit.SECONDS);
            assertTrue(
                    rejected.getExitCode() != 0
                            || "FAILED".equals(engine.getJobStatus(conflict.id)));
            assertTrue(
                    (rejected.getStderr() + rejected.getStdout() + jobLogs(conflict))
                            .contains("could not bind configured port " + FIRST));
            assertEquals("RUNNING", engine.getJobStatus(original.id));
        } finally {
            cancel(original);
        }
        assertEquals(0, engine.sender("probe", Integer.toString(FIRST)).getExitCode());
    }

    @Test
    void restoresLivePortWithoutReopeningTerminalPortOrReplayingReceipts() throws Exception {
        Job original = start("TEXT", "", 256, 4, 14000, false, "STREAMING", 1, FIRST, SECOND);
        try {
            awaitListener(original, FIRST);
            awaitListener(original, SECOND);
            send(SENDER_A, FIRST, utf8("first-before-checkpoint"));
            Awaitility.await()
                    .pollDelay(8, TimeUnit.SECONDS)
                    .atMost(12, TimeUnit.SECONDS)
                    .until(() -> "RUNNING".equals(engine.getJobStatus(original.id)));
            send(SENDER_B, SECOND, utf8("second-before-checkpoint"));
            Awaitility.await()
                    .atMost(12, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    engine.sender("probe", Integer.toString(FIRST)).getExitCode()
                                            == 0);
            long completed = engine.getCompletedCheckpointCount(original.id);
            Awaitility.await()
                    .atMost(5, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(original.id) > completed);
        } finally {
            cancel(original);
        }

        List<String> before = texts(receipts(original.output));
        assertEquals(
                new HashSet<>(Arrays.asList("first-before-checkpoint", "second-before-checkpoint")),
                new HashSet<>(before));
        assertEquals(2, before.size());
        List<String> variables = new ArrayList<>(original.variables);
        variables.remove("idle.timeout=14000");
        variables.add("idle.timeout=5000");
        String newId = id();
        int offset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(
                        () ->
                                engine.restoreJobWithCheckpoint(
                                        CONFIG, original.id, newId, variables));
        Job restored = new Job(newId, original.output, offset, variables, result);
        awaitListener(restored, SECOND);
        assertFalse(jobLogs(restored).contains("UDP listener ready on port " + FIRST));
        assertEquals(0, engine.sender("probe", Integer.toString(FIRST)).getExitCode());
        send(SENDER_A, FIRST, utf8("must-not-be-received"));
        send(SENDER_B, SECOND, utf8("after-restore"));
        List<String> after = texts(finish(restored));
        assertEquals(3, after.size());
        assertTrue(
                after.containsAll(
                        Arrays.asList(
                                "first-before-checkpoint",
                                "second-before-checkpoint",
                                "after-restore")));
    }

    private Job start(
            String format,
            String separator,
            int maxDatagram,
            int maxRecords,
            int idle,
            boolean fail,
            String mode,
            int parallelism,
            int first,
            int... more) {
        String jobId = id();
        String ports =
                first
                        + Arrays.stream(more)
                                .mapToObj(port -> "," + port)
                                .collect(Collectors.joining());
        List<String> vars =
                new ArrayList<>(
                        Arrays.asList(
                                "mode=" + mode,
                                "parallelism=" + parallelism,
                                "listen_ports='[" + ports + "]'",
                                "wire.format=" + format,
                                "wire.separator='" + separator + "'",
                                "max.datagram=" + maxDatagram,
                                "max.records=" + maxRecords,
                                "idle.timeout=" + idle,
                                "malformed=" + (fail ? "FAIL" : "DROP"),
                                "output.path=" + jobId));
        int logOffset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(() -> engine.executeJob(CONFIG, jobId, vars.toArray(new String[0])));
        return new Job(jobId, jobId, logOffset, vars, result);
    }

    private void awaitListener(Job job, int port) {
        Awaitility.await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                assertTrue(
                                        jobLogs(job)
                                                .contains("UDP listener ready on port " + port)));
    }

    private void send(int sourcePort, int port, byte[]... datagrams) throws Exception {
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                "send", Integer.toString(sourcePort), Integer.toString(port)));
        for (byte[] bytes : datagrams) {
            command.add(Base64.getEncoder().encodeToString(bytes));
        }
        Container.ExecResult result = engine.sender(command.toArray(new String[0]));
        assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private List<JsonNode> finish(Job job) throws Exception {
        Container.ExecResult result = job.result.get(50, TimeUnit.SECONDS);
        assertEquals(0, result.getExitCode(), result.getStderr());
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals("FINISHED", engine.getJobStatus(job.id)));
        return receipts(job.output);
    }

    private void cancel(Job job) throws Exception {
        if (!job.result.isDone()) {
            engine.stopJob(job.id);
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals("CANCELED", engine.getJobStatus(job.id)));
        }
        job.result.get(40, TimeUnit.SECONDS);
        assertEquals("CANCELED", engine.getJobStatus(job.id));
    }

    private String jobLogs(Job job) {
        String logs = engine.getServerLogs();
        return logs.substring(Math.min(logs.length(), job.logOffset));
    }

    private List<JsonNode> receipts(String output) throws Exception {
        Container.ExecResult files =
                engine.command(
                        "sh",
                        "-c",
                        "if test -d /tmp/seatunnel_mnt/udp-"
                                + output
                                + "; then find /tmp/seatunnel_mnt/udp-"
                                + output
                                + " -maxdepth 1 -name 'receipt_*' -type f "
                                + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
        assertEquals(0, files.getExitCode(), files.getStderr());
        List<JsonNode> rows = new ArrayList<>();
        for (String line : files.getStdout().split("\\R")) {
            if (!line.trim().isEmpty()) {
                rows.add(JSON.readTree(line));
            }
        }
        return rows;
    }

    private static List<String> texts(List<JsonNode> rows) {
        return rows.stream()
                .map(row -> row.path("payload").textValue())
                .collect(Collectors.toList());
    }

    private static void assertMetadata(JsonNode row, int source, int listener, int record) {
        assertEquals("127.0.0.1", row.path("sender_host").asText());
        assertEquals(source, row.path("sender_port").asInt());
        assertEquals(listener, row.path("listen_port").asInt());
        assertEquals(record, row.path("record_index").asInt());
    }

    private static byte[] collectd() throws IOException {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        part(packet, 0, utf8("fixture-host\0"));
        part(packet, 2, utf8("temperature\0"));
        part(packet, 3, utf8("eth0\0"));
        part(packet, 4, utf8("cpu\0"));
        part(packet, 5, utf8("sample\0"));
        part(packet, 8, ByteBuffer.allocate(8).putLong((1700000000L << 30) + (1L << 29)).array());
        part(packet, 9, ByteBuffer.allocate(8).putLong((2L << 30) + (1L << 29)).array());
        ByteBuffer values = ByteBuffer.allocate(38);
        values.putShort((short) 4).put(new byte[] {0, 1, 2, 3});
        values.putLong(-1L);
        values.order(ByteOrder.LITTLE_ENDIAN).putDouble(12.25).order(ByteOrder.BIG_ENDIAN);
        values.putLong(-42L).putLong(109L);
        part(packet, 6, values.array());
        return packet.toByteArray();
    }

    private static void part(ByteArrayOutputStream packet, int type, byte[] value)
            throws IOException {
        packet.write(
                ByteBuffer.allocate(4)
                        .putShort((short) type)
                        .putShort((short) (4 + value.length))
                        .array());
        packet.write(value);
    }

    private static byte[] netflow() {
        ByteBuffer packet = ByteBuffer.allocate(120);
        packet.putShort((short) 5)
                .putShort((short) 2)
                .putInt(5500)
                .putInt(1700000000)
                .putInt(125000000)
                .putInt(-2)
                .put((byte) 1)
                .put((byte) 9)
                .putShort((short) 0x8258);
        netflowRecord(packet, 6);
        netflowRecord(packet, 17);
        return packet.array();
    }

    private static void netflowRecord(ByteBuffer packet, int protocol) {
        packet.putInt(0xc0000201)
                .putInt(0xcb007102)
                .putInt(0xc6336403)
                .putShort((short) 2)
                .putShort((short) 3)
                .putInt(-1)
                .putInt(3000)
                .putInt(100)
                .putInt(250)
                .putShort((short) 65530)
                .putShort((short) 443)
                .put((byte) 0)
                .put((byte) 0x12)
                .put((byte) protocol)
                .put((byte) 16)
                .putShort((short) 64512)
                .putShort((short) 64513)
                .put((byte) 24)
                .put((byte) 32)
                .putShort((short) 0);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String id() {
        return Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
    }

    /**
     * Keeps one submitted job identity, output directory and log observation window together.
     *
     * <p>Concurrent bind-conflict jobs must not accidentally inspect another submission as their
     * result.
     */
    private static final class Job {
        final String id;
        final String output;
        final int logOffset;
        final List<String> variables;
        final Future<Container.ExecResult> result;

        Job(
                String id,
                String output,
                int logOffset,
                List<String> variables,
                Future<Container.ExecResult> result) {
            this.id = id;
            this.output = output;
            this.logOffset = logOffset;
            this.variables = variables;
            this.result = result;
        }
    }

    /**
     * Makes the independent sender available inside Zeta's network namespace without port mapping.
     */
    private static final class UdpZeta extends SeaTunnelContainer {
        @Override
        protected void executeExtraCommands(GenericContainer<?> container) throws IOException {
            Path classes;
            try {
                classes =
                        Paths.get(
                                UdpDatagramSender.class
                                        .getProtectionDomain()
                                        .getCodeSource()
                                        .getLocation()
                                        .toURI());
            } catch (Exception e) {
                throw new IOException("Could not find UDP fixture classes", e);
            }
            Path fixtureClass = classes.resolve(FIXTURE_CLASS);
            if (!Files.isRegularFile(fixtureClass)) {
                throw new IOException("UDP fixture class is missing");
            }
            container.withCopyFileToContainer(
                    MountableFile.forHostPath(fixtureClass), FIXTURE_CLASSES + "/" + FIXTURE_CLASS);
        }

        Container.ExecResult sender(String... args) throws Exception {
            List<String> command =
                    new ArrayList<>(Arrays.asList("java", "-cp", FIXTURE_CLASSES, SENDER_CLASS));
            command.addAll(Arrays.asList(args));
            return command(command.toArray(new String[0]));
        }

        Container.ExecResult command(String... command) throws IOException, InterruptedException {
            return server.execInContainer(command);
        }
    }
}
