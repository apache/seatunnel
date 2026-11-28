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

package org.apache.seatunnel.e2e.connector.amazonkinesis;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.Driver;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the packaged source and sink through Zeta using an independent HTTPS CBOR/SigV4
 * fixture. Captured wire data, restore/cancel outcomes and DEBUG privacy checks do not certify live
 * AWS behavior.
 */
class AmazonKinesisIT extends TestSuiteBase {
    private static final String HOST = "kinesis.us-east-1.amazonaws.com";
    private static final String ARN = "arn:aws:kinesis:us-east-1:111122223333:stream/in";
    private static final String FIRST = "90071992547409931234567890";
    private static final String SECOND = "90071992547409931234567891";
    private static final String A = "shardId-000000000000";
    private static final String B = "shardId-000000000001";
    private static final String CHILD = "shardId-000000000002";
    private static final String WHEN = "2023-11-14T22:13:20.123Z";
    private static final String BATCH = "/source_batch.conf";
    private static final String STREAM = "/source_stream.conf";
    private static final String SINK = "/sink.conf";
    private static final String ORDERED = "/sink_ordered.conf";
    private static final String BOTH = "/stream_to_kinesis.conf";
    private static final String CERT_PASSWORD = "fixture-store";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final AtomicLong IDS = new AtomicLong(System.currentTimeMillis());

    @TempDir private static Path temp;

    private final String access = UUID.randomUUID().toString();
    private final String secret = UUID.randomUUID().toString();
    private final String session = UUID.randomUUID().toString();
    private GenericContainer<?> provider;
    private PostgreSQLContainer<?> postgres;
    private SeaTunnelContainer engine;

    @BeforeAll
    void start() throws Exception {
        prepareTls();
        Path logConfiguration = temp.resolve("log4j2.properties");
        Files.write(
                logConfiguration,
                ("appender.console.type=Console\n"
                                + "appender.console.name=STDOUT\nappender.console.layout.type=PatternLayout\n"
                                + "appender.console.layout.pattern=%p %c - %m%n\n"
                                + "rootLogger.level=info\nrootLogger.appenderRef.console.ref=STDOUT\n"
                                + "logger.aws.name=software.amazon.awssdk\nlogger.aws.level=debug\n")
                        .getBytes(StandardCharsets.UTF_8));
        provider =
                new GenericContainer<>(DockerImageName.parse("python:3.11-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withExposedPorts(8080)
                        .withEnv("FIXTURE_ACCESS", access)
                        .withEnv("FIXTURE_SECRET", secret)
                        .withEnv("FIXTURE_TOKEN", session)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("kinesis_fixture.py"),
                                "/fixture.py")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("cert.pem")), "/cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("key.pem")), "/key.pem")
                        .withCommand("python", "/fixture.py")
                        .waitingFor(Wait.forHttp("/ready").forPort(8080));
        provider.start();
        postgres =
                new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                        .withDatabaseName("test")
                        .withUsername("test")
                        .withPassword("test")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("kinesis-postgres");
        postgres.start();
        try (Connection sql = sql();
                Statement statement = sql.createStatement()) {
            statement.execute(
                    "CREATE TABLE kinesis_rows (stream_arn TEXT, shard_id TEXT, sequence_number TEXT, "
                            + "partition_key TEXT, arrival_timestamp TEXT, data BYTEA)");
        }
        Path jdbc = DependencyJar.of(Driver.class).path();
        engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/kinesis-fixture.jks "
                                + "-Djavax.net.ssl.trustStorePassword="
                                + CERT_PASSWORD
                                + " -Dlog4j2.configurationFile=/tmp/kinesis-fixture-log4j2.properties";
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> runtime) {
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(temp.resolve("trust.jks")),
                                "/tmp/kinesis-fixture.jks");
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(logConfiguration),
                                "/tmp/kinesis-fixture-log4j2.properties");
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(jdbc),
                                "/tmp/seatunnel/plugins/Jdbc/lib/postgresql.jar");
                    }
                };
        engine.startUp();
    }

    @AfterAll
    void stop() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            try {
                if (provider != null) {
                    provider.stop();
                }
            } finally {
                if (postgres != null) {
                    postgres.stop();
                }
            }
        }
    }

    @Test
    void consumesExactBinaryRowsAcrossExpiredListingIteratorsAndMergedShards() throws Exception {
        select("read");
        int baseline = engine.getServerLogs().length();
        Container.ExecResult job =
                engine.executeJob(
                        BATCH, args("TRIM_HORIZON", 10000, 30000, "text", "payload", 3, 100, 100));
        assertSuccess(job, baseline);
        assertEquals(
                Arrays.asList(
                        row(A, FIRST, "雪", WHEN, "AP8="), row(A, SECOND, "next", WHEN, b64("tail")),
                        row(B, "42", null, null, ""), row(CHILD, "9", "key", WHEN, b64("merged"))),
                rows());
        List<JsonNode> all = signedCalls();
        List<JsonNode> discovery = filter(all, "ListShards");
        assertEquals(4, discovery.size());
        assertEquals(Arrays.asList(null, "expired", null, "last"), values(discovery, "NextToken"));
        List<JsonNode> opens = filter(all, "GetShardIterator");
        assertEquals(
                2,
                opens.stream()
                        .filter(
                                n ->
                                        A.equals(n.path("body").path("ShardId").asText())
                                                && "TRIM_HORIZON"
                                                        .equals(
                                                                n.path("body")
                                                                        .path("ShardIteratorType")
                                                                        .asText()))
                        .count());
        assertEquals(
                Collections.singletonList(FIRST),
                opens.stream()
                        .filter(
                                n ->
                                        "AFTER_SEQUENCE_NUMBER"
                                                .equals(
                                                        n.path("body")
                                                                .path("ShardIteratorType")
                                                                .asText()))
                        .map(n -> n.path("body").path("StartingSequenceNumber").asText())
                        .collect(Collectors.toList()));
        assertAfterBothParents(all);
    }

    @Test
    void keepsTheExplicitTimestampWhenRefreshingAnIteratorBeforeAnyRecord() throws Exception {
        select("read");
        int baseline = engine.getServerLogs().length();
        assertSuccess(
                engine.executeJob(
                        BATCH, args("AT_TIMESTAMP", 10000, 30000, "text", "payload", 3, 100, 100)),
                baseline);
        assertEquals(4, rows().size());
        List<JsonNode> reads =
                filter(signedCalls(), "GetShardIterator").stream()
                        .filter(
                                n ->
                                        A.equals(n.path("body").path("ShardId").asText())
                                                && "AT_TIMESTAMP"
                                                        .equals(
                                                                n.path("body")
                                                                        .path("ShardIteratorType")
                                                                        .asText()))
                        .collect(Collectors.toList());
        assertEquals(2, reads.size());
        for (JsonNode read : reads) {
            assertEquals(1700000000123L, read.path("body").path("Timestamp").asLong());
        }
    }

    @Test
    void discoversReshardedChildrenOnlyAfterBothParentsComplete() throws Exception {
        select("dynamic");
        String job = jobId();
        List<String> args = args("TRIM_HORIZON", 10000, 30000, "text", "payload", 3, 100, 100);
        CompletableFuture<Container.ExecResult> active = submit(STREAM, job, args, false);
        try {
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(4, rows().size()));
            List<JsonNode> calls = signedCalls();
            assertTrue(filter(calls, "ListShards").size() > 1);
            assertAfterBothParents(calls);
            assertTrue(
                    engine.getCompletedCheckpointCount(job) > 0,
                    "Merged child requires a durable parent checkpoint");
            cancel(job, active);
        } finally {
            cleanup(job);
        }
    }

    @Test
    void unblocksBothSplitChildrenAfterTheirParentAndAbortsAHeldReadOnCancel() throws Exception {
        select("split");
        String job = jobId();
        List<String> args = args("TRIM_HORIZON", 10000, 30000, "text", "payload", 3, 100, 100);
        CompletableFuture<Container.ExecResult> active = submit(STREAM, job, args, false);
        try {
            await().atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(4, rows().size()));
            List<JsonNode> calls = signedCalls();
            int terminal = find(calls, "GetRecords", "ShardIterator", "terminal-a");
            assertTrue(
                    terminal >= 0
                            && find(calls, "GetShardIterator", "ShardId", B) > terminal
                            && find(calls, "GetShardIterator", "ShardId", CHILD) > terminal);
            cancel(job, active);
        } finally {
            cleanup(job);
        }

        select("held-deadline");
        String heldJob = jobId();
        CompletableFuture<Container.ExecResult> held =
                submit(
                        STREAM,
                        heldJob,
                        args("TRIM_HORIZON", 60000, 30000, "text", "payload", 3, 100, 100),
                        false);
        try {
            await().atMost(50, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    filter(calls(), "GetRecords").stream()
                                            .anyMatch(n -> n.path("held").asBoolean()));
            cancel(heldJob, held);
            await().atMost(10, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    filter(calls(), "GetRecords").stream()
                                            .anyMatch(
                                                    n ->
                                                            n.has("closed_after")
                                                                    && !n.path("released")
                                                                            .asBoolean()));
            assertEquals(1, filter(calls(), "GetRecords").size());
            assertTrue(rows().isEmpty());
        } finally {
            cleanup(heldJob);
        }
    }

    @Test
    void savepointAndRestoreUseCommittedSequenceWhileTheRequestIsHeld() throws Exception {
        heldRecovery("held-source", STREAM, false);
    }

    @Test
    void flushesTheProducerAtCheckpointAndRestoresTheRemainingStreamRecords() throws Exception {
        heldRecovery("checkpoint-sink", BOTH, true);
    }

    @Test
    void rejectsIncompleteDiscoveryAndStopsBoundedReadsWithNoProgress() throws Exception {
        List<String> args = args("TRIM_HORIZON", 3000, 12000, "text", "payload", 3, 100, 100);
        for (String scenario : Arrays.asList("bad-list", "token-loop", "source-denied")) {
            select(scenario);
            int baseline = engine.getServerLogs().length();
            Container.ExecResult result = engine.executeJob(BATCH, args);
            assertFailure(
                    result,
                    baseline,
                    scenario.equals("source-denied")
                            ? "access denied"
                            : scenario.equals("bad-list")
                                    ? "incomplete shard discovery"
                                    : "repeated shard discovery token");
            assertEquals(
                    scenario.equals("source-denied") ? 1 : 0,
                    filter(signedCalls(), "GetRecords").size());
            assertTrue(rows().isEmpty());
        }
        select("idle");
        int baseline = engine.getServerLogs().length();
        assertFailure(
                engine.executeJob(
                        BATCH, args("TRIM_HORIZON", 3000, 1500, "text", "payload", 3, 100, 100)),
                baseline,
                "batch catch-up deadline exceeded");

        select("held-deadline");
        baseline = engine.getServerLogs().length();
        assertFailure(
                engine.executeJob(
                        BATCH, args("TRIM_HORIZON", 1000, 15000, "text", "payload", 3, 100, 100)),
                baseline,
                "transport or response failure");
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<JsonNode> attempts = filter(calls(), "GetRecords");
                            assertEquals(3, attempts.size());
                            for (JsonNode attempt : attempts) {
                                assertTrue(
                                        attempt.has("closed_after")
                                                && attempt.path("closed_after").asDouble() < 4.0);
                            }
                        });
    }

    @Test
    void retriesOnlyExplicitFailedSlotsAndEnforcesUtf8BatchBoundaries() throws Exception {
        select("partial");
        int baseline = engine.getServerLogs().length();
        assertSuccess(
                engine.executeJob(
                        SINK, args("TRIM_HORIZON", 10000, 30000, "text", "payload", 3, 100, 100)),
                baseline);
        List<String> expected =
                Arrays.asList("甲:" + b64("one"), "B:" + b64("two"), "C:" + b64("three"));
        List<JsonNode> attempts = filter(signedCalls(), "PutRecords");
        assertEquals(3, attempts.size());
        assertEquals(expected, payloads(attempts.get(0).path("body").path("Records")));
        assertEquals(
                expected.subList(1, 3), payloads(attempts.get(1).path("body").path("Records")));
        assertEquals(
                expected.subList(2, 3), payloads(attempts.get(2).path("body").path("Records")));
        assertEquals(expected, payloads(accepted()));

        select("bounded-sink");
        baseline = engine.getServerLogs().length();
        assertSuccess(
                engine.executeJob(
                        SINK, args("TRIM_HORIZON", 10000, 30000, "text", "payload", 10, 10, 6)),
                baseline);
        attempts = filter(signedCalls(), "PutRecords");
        assertEquals(2, attempts.size());
        assertEquals(
                expected.subList(0, 2), payloads(attempts.get(0).path("body").path("Records")));
        assertEquals(
                expected.subList(2, 3), payloads(attempts.get(1).path("body").path("Records")));
        assertEquals(expected, payloads(accepted()));

        select("bounded-sink");
        baseline = engine.getServerLogs().length();
        assertFailure(
                engine.executeJob(
                        SINK, args("TRIM_HORIZON", 10000, 30000, "text", "payload", 10, 10, 5)),
                baseline,
                "invalid payload, key or size");
        assertEquals(0, filter(signedCalls(), "PutRecords").size());
    }

    @Test
    void ordersSameKeyRecordsAndSerializesNativeJsonPayloads() throws Exception {
        select("ordered");
        int baseline = engine.getServerLogs().length();
        assertSuccess(
                engine.executeJob(
                        ORDERED,
                        args("TRIM_HORIZON", 10000, 30000, "text", "payload", 100, 100, 100)),
                baseline);
        List<JsonNode> attempts = signedCalls();
        assertEquals(3, filter(attempts, "PutRecord").size());
        assertEquals(0, filter(attempts, "PutRecords").size());
        assertEquals(Arrays.asList(null, null, "1"), values(attempts, "SequenceNumberForOrdering"));
        assertEquals(
                Arrays.asList("甲:" + b64("one"), "B:" + b64("two"), "甲:" + b64("three")),
                payloads(accepted()));

        select("json");
        baseline = engine.getServerLogs().length();
        assertSuccess(
                engine.executeJob(
                        SINK, args("TRIM_HORIZON", 10000, 30000, "json", "", 3, 1024, 1024)),
                baseline);
        List<JsonNode> actual = new ArrayList<>();
        for (JsonNode record : accepted()) {
            actual.add(
                    JSON.readTree(
                            Base64.getDecoder()
                                    .decode(record.path("Data").path("bytes").asText())));
        }
        assertEquals(
                Arrays.asList(
                        JSON.readTree("{\"key\":\"甲\",\"payload\":\"one\"}"),
                        JSON.readTree("{\"key\":\"B\",\"payload\":\"two\"}"),
                        JSON.readTree("{\"key\":\"C\",\"payload\":\"three\"}")),
                actual);
    }

    @Test
    void ambiguousPermanentAndHeldWritesNeverResendTheWholeBatch() throws Exception {
        for (String scenario :
                Arrays.asList("ambiguous", "bad-count", "failure-count", "permanent", "held-put")) {
            select(scenario);
            int baseline = engine.getServerLogs().length();
            Container.ExecResult result =
                    engine.executeJob(
                            SINK,
                            args("TRIM_HORIZON", 1500, 15000, "text", "payload", 3, 100, 100));
            assertFailure(
                    result,
                    baseline,
                    scenario.equals("bad-count")
                            ? "cardinality differs"
                            : scenario.equals("failure-count")
                                    ? "failure count differs"
                                    : scenario.equals("permanent")
                                            ? "permanent record failure"
                                            : "unambiguous acknowledgment");
            List<JsonNode> writes = filter(signedCalls(), "PutRecords");
            assertEquals(1, writes.size());
            if (scenario.equals("held-put")) {
                await().atMost(10, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> {
                                    JsonNode held = filter(calls(), "PutRecords").get(0);
                                    assertTrue(
                                            held.has("closed_after")
                                                    && held.path("closed_after").asDouble() < 5.0);
                                });
            }
        }
    }

    @Test
    void boundsNativeArrayPayloadsAndPreservesSmallTypedOutput() throws Exception {
        select("bounded-sink");
        List<String> small =
                new ArrayList<>(args("TRIM_HORIZON", 10000, 30000, "json", "", 3, 1024, 512));
        small.add("test_nested=雪");
        int baseline = engine.getServerLogs().length();
        assertSuccess(engine.executeJob("/sink_nested.conf", small), baseline);
        List<JsonNode> writes = filter(signedCalls(), "PutRecords");
        assertEquals(1, writes.size());
        JsonNode records = accepted();
        assertEquals(1, records.size());
        byte[] encoded =
                Base64.getDecoder().decode(records.get(0).path("Data").path("bytes").asText());
        assertEquals(
                JSON.readTree(
                        "{\"key\":\"k\",\"payload\":["
                                + String.join(",", Collections.nCopies(16, "\"雪\""))
                                + "]}"),
                JSON.readTree(encoded));

        select("bounded-sink");
        char[] content = new char[128];
        Arrays.fill(content, 'a');
        List<String> oversized =
                new ArrayList<>(args("TRIM_HORIZON", 10000, 30000, "json", "", 3, 1024, 512));
        oversized.add("test_nested=" + new String(content));
        baseline = engine.getServerLogs().length();
        assertFailure(
                engine.executeJob("/sink_nested.conf", oversized),
                baseline,
                "invalid payload, key or size");
        assertEquals(0, filter(signedCalls(), "PutRecords").size());
        assertEquals(0, accepted().size());
    }

    private void prepareTls() throws Exception {
        Path file = temp.resolve("fixture.p12");
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "2",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                CERT_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(temp.resolve("keytool.log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Kinesis fixture certificate generation timed out");
        }
        assertEquals(0, keytool.exitValue());
        KeyStore privateStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(file)) {
            privateStore.load(in, CERT_PASSWORD.toCharArray());
        }
        Certificate cert = privateStore.getCertificate("fixture");
        pem("CERTIFICATE", cert.getEncoded(), temp.resolve("cert.pem"));
        pem(
                "PRIVATE KEY",
                privateStore.getKey("fixture", CERT_PASSWORD.toCharArray()).getEncoded(),
                temp.resolve("key.pem"));
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", cert);
        try (OutputStream out = Files.newOutputStream(temp.resolve("trust.jks"))) {
            trust.store(out, CERT_PASSWORD.toCharArray());
        }
    }

    private static void pem(String type, byte[] payload, Path target) throws Exception {
        String content =
                "-----BEGIN "
                        + type
                        + "-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(payload)
                        + "\n-----END "
                        + type
                        + "-----\n";
        Files.write(target, content.getBytes(StandardCharsets.US_ASCII));
    }

    private void heldRecovery(String scenario, String conf, boolean binarySink) throws Exception {
        select(scenario);
        String job = jobId();
        List<String> args = args("LATEST", 60000, 60000, "binary", "data", 100, 1024, 1024);
        CompletableFuture<Container.ExecResult> running = submit(conf, job, args, false);
        try {
            await().atMost(55, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertTrue(
                                        filter(calls(), "GetRecords").stream()
                                                .anyMatch(n -> n.path("held").asBoolean()));
                                assertEquals(1, binarySink ? accepted().size() : rows().size());
                            });
            long baseline = engine.getCompletedCheckpointCount(job);
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(job) > baseline);
            assertEquals(0, engine.savepointJob(job).getExitCode());
            assertEquals(0, running.get(40, TimeUnit.SECONDS).getExitCode());
            assertFalse(
                    filter(calls(), "GetRecords").stream()
                            .anyMatch(n -> n.path("released").asBoolean()));
            await().atMost(15, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    filter(calls(), "GetRecords").stream()
                                            .anyMatch(
                                                    n ->
                                                            n.path("held").asBoolean()
                                                                    && n.has("closed_after")));
            control("/release");
            CompletableFuture<Container.ExecResult> restored = submit(conf, job, args, true);
            await().atMost(55, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                if (binarySink) {
                                    assertEquals(
                                            Arrays.asList(
                                                    "k:" + b64("first"), "k:" + b64("second")),
                                            payloads(accepted()));
                                } else {
                                    assertEquals(
                                            Arrays.asList(
                                                    row(A, FIRST, "k", WHEN, b64("first")),
                                                    row(A, SECOND, "k", WHEN, b64("second"))),
                                            rows());
                                }
                            });
            List<JsonNode> opens = filter(signedCalls(), "GetShardIterator");
            assertEquals(
                    Arrays.asList("AT_TIMESTAMP", "AFTER_SEQUENCE_NUMBER"),
                    values(opens, "ShardIteratorType"));
            assertTrue(opens.get(0).path("body").path("Timestamp").asLong() > 1700000000000L);
            assertEquals(FIRST, opens.get(1).path("body").path("StartingSequenceNumber").asText());
            cancel(job, restored);
        } finally {
            cleanup(job);
        }
    }

    private void assertAfterBothParents(List<JsonNode> calls) {
        int child = find(calls, "GetShardIterator", "ShardId", CHILD);
        int aDone = find(calls, "GetRecords", "ShardIterator", "terminal-a");
        int bDone = find(calls, "GetRecords", "ShardIterator", B);
        assertTrue(aDone >= 0 && bDone >= 0 && child > aDone && child > bDone);
    }

    private static int find(List<JsonNode> calls, String op, String name, String expected) {
        for (int i = 0; i < calls.size(); i++) {
            JsonNode call = calls.get(i);
            if (op.equals(call.path("op").asText())
                    && expected.equals(call.path("body").path(name).asText())) {
                return i;
            }
        }
        return -1;
    }

    private void assertSuccess(Container.ExecResult result, int baseline) {
        assertEquals(0, result.getExitCode(), "Engine exit code");
        assertPrivate(result, baseline);
    }

    private void assertFailure(Container.ExecResult result, int baseline, String failure) {
        assertNotEquals(0, result.getExitCode());
        String logs = assertPrivate(result, baseline);
        assertTrue(logs.contains(failure), "Expected sanitized diagnostic: " + failure);
    }

    private String assertPrivate(Container.ExecResult result, int baseline) {
        String server = engine.getServerLogs();
        String relevant =
                result.getStdout()
                        + result.getStderr()
                        + server.substring(Math.min(baseline, server.length()));
        assertFalse(
                relevant.contains(access)
                        || relevant.contains(secret)
                        || relevant.contains(session)
                        || relevant.contains("sensitive-provider-message")
                        || relevant.contains("AWS4 Canonical Request"),
                "Credentials or provider content leaked into engine logs");
        return relevant;
    }

    private List<String> args(
            String position,
            int timeout,
            int batchTimeout,
            String format,
            String data,
            int batchSize,
            int batchBytes,
            int recordBytes) {
        return Arrays.asList(
                "test_aws_access=" + access,
                "test_aws_secret=" + secret,
                "test_aws_session=" + session,
                "test_start_position=" + position,
                "test_timestamp=" + ("AT_TIMESTAMP".equals(position) ? WHEN : ""),
                "test_api_timeout=" + timeout,
                "test_batch_timeout=" + batchTimeout,
                "test_format=" + format,
                "test_data_field=" + data,
                "test_batch_size=" + batchSize,
                "test_batch_bytes=" + batchBytes,
                "test_record_bytes=" + recordBytes,
                "test_key_field=" + ("binary".equals(format) ? "partition_key" : "key"));
    }

    private static String jobId() {
        return Long.toString(IDS.incrementAndGet());
    }

    private CompletableFuture<Container.ExecResult> submit(
            String conf, String jobId, List<String> args, boolean restore) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return restore
                                ? engine.restoreJob(conf, jobId, args.toArray(new String[0]))
                                : engine.executeJob(conf, jobId, args.toArray(new String[0]));
                    } catch (Exception ex) {
                        throw new IllegalStateException("Engine invocation failed", ex);
                    }
                });
    }

    private void cancel(String job, CompletableFuture<Container.ExecResult> active)
            throws Exception {
        assertEquals(0, engine.cancelJob(job).getExitCode());
        await().atMost(35, TimeUnit.SECONDS)
                .until(() -> "CANCELED".equals(engine.getJobStatus(job)));
        active.get(45, TimeUnit.SECONDS);
    }

    private void cleanup(String job) throws Exception {
        control("/release");
        if ("RUNNING".equals(engine.getJobStatus(job))) {
            engine.cancelJob(job);
        }
    }

    private void select(String scenario) throws Exception {
        control("/select?value=" + scenario);
        try (Connection sql = sql();
                Statement query = sql.createStatement()) {
            query.execute("TRUNCATE kinesis_rows");
        }
    }

    private List<JsonNode> calls() throws Exception {
        List<JsonNode> calls = new ArrayList<>();
        control("/capture").path("calls").forEach(calls::add);
        return calls;
    }

    private List<JsonNode> signedCalls() throws Exception {
        List<JsonNode> calls = calls();
        for (JsonNode call : calls) {
            assertTrue(call.path("valid").asBoolean(), "Independent fixture rejected SigV4");
            assertEquals("application/x-amz-cbor-1.1", call.path("media").asText());
        }
        return calls;
    }

    private static List<JsonNode> filter(List<JsonNode> calls, String op) {
        return calls.stream()
                .filter(n -> op.equals(n.path("op").asText()))
                .collect(Collectors.toList());
    }

    private static List<String> values(List<JsonNode> calls, String field) {
        return calls.stream()
                .map(n -> n.path("body").path(field).textValue())
                .collect(Collectors.toList());
    }

    private JsonNode accepted() throws Exception {
        return control("/capture").path("accepted");
    }

    private static List<String> payloads(JsonNode records) {
        List<String> payloads = new ArrayList<>();
        records.forEach(
                record ->
                        payloads.add(
                                record.path("PartitionKey").asText()
                                        + ":"
                                        + record.path("Data").path("bytes").asText()));
        return payloads;
    }

    private JsonNode control(String path) throws Exception {
        URL url = new URL("http", provider.getHost(), provider.getMappedPort(8080), path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        try (InputStream response = connection.getInputStream()) {
            return JSON.readTree(response);
        } finally {
            connection.disconnect();
        }
    }

    private Connection sql() throws Exception {
        return DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    private List<List<String>> rows() throws Exception {
        List<List<String>> result = new ArrayList<>();
        try (Connection sql = sql();
                Statement query = sql.createStatement();
                ResultSet rows =
                        query.executeQuery(
                                "SELECT stream_arn,shard_id,sequence_number,partition_key,arrival_timestamp,data FROM kinesis_rows "
                                        + "ORDER BY shard_id,sequence_number")) {
            while (rows.next()) {
                result.add(
                        Arrays.asList(
                                rows.getString(1),
                                rows.getString(2),
                                rows.getString(3),
                                rows.getString(4),
                                rows.getString(5),
                                Base64.getEncoder().encodeToString(rows.getBytes(6))));
            }
        }
        return result;
    }

    private static List<String> row(
            String shard, String sequence, String key, String arrival, String data) {
        return Arrays.asList(ARN, shard, sequence, key, arrival, data);
    }

    private static String b64(String text) {
        return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
}
