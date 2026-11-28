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

package org.apache.seatunnel.e2e.connector.opcua;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.AUX_NS;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.CLIENT_URI;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.CONTROL;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.GUID;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.HOST;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.NS;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.PASSWORD;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.SERVER_URI;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.SILENT;
import static org.apache.seatunnel.e2e.connector.opcua.OpcUaFixture.UA;

@Execution(ExecutionMode.SAME_THREAD)
class OpcUaSourceIT extends TestSuiteBase implements TestResource {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String WORKER_KEYS = "/tmp/seatunnel-opcua-client.p12";
    private static final String WORKER_FILE = "/tmp/seatunnel-opcua-nodes.json";
    private static final String ROOT = "nsu=" + NS + ";s=tree";
    private static final String SAMPLE = "nsu=" + NS + ";s=sample";
    private static final String ENDPOINT = "opc.tcp://" + HOST + ":" + UA + "/fixture";
    private static final String SILENT_ENDPOINT = "opc.tcp://" + HOST + ":" + SILENT + "/fixture";
    private static final String CHECKPOINT_ENV = "STREAMING\n  checkpoint.interval = 200";

    private GenericContainer<?> fixture;
    private Path keys;
    private String fingerprint;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        keys = Files.createTempDirectory("seatunnel-opcua-e2e-");
        Path client = certificate("client", CLIENT_URI, "client");
        Path server = certificate("server", SERVER_URI, HOST);
        fingerprint =
                hex(
                        MessageDigest.getInstance("SHA-256")
                                .digest(
                                        OpcUaFixture.load(server)
                                                .getCertificate("test")
                                                .getEncoded()));
        Path testClasses =
                Path.of(
                        OpcUaFixture.class
                                .getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .toURI());
        fixture =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST, "opcua-alias")
                        .withExposedPorts(CONTROL)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(testClasses), "/opcua/classes")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(client), "/opcua/client.p12")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(server), "/opcua/server.p12")
                        .withCommand(
                                "java",
                                "--add-modules",
                                "jdk.httpserver",
                                "-cp",
                                "/opcua/classes:/opcua/lib/*",
                                OpcUaFixture.class.getName(),
                                "/opcua/server.p12",
                                "/opcua/client.p12")
                        .waitingFor(Wait.forLogMessage(".*OPC UA fixture ready.*\\n", 1));
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        Set<String> libraries = new HashSet<>();
        for (String item : classpath.split(Pattern.quote(File.pathSeparator))) {
            Path path = Path.of(item);
            String name = path.getFileName().toString();
            if (!name.endsWith(".jar") || !isFixtureLibrary(name) || !libraries.add(name)) {
                continue;
            }
            fixture.withCopyFileToContainer(MountableFile.forHostPath(path), "/opcua/lib/" + name);
        }
        Assertions.assertTrue(
                libraries.contains("sdk-server-0.6.16.jar"), "Milo SDK server absent");
        fixture.start();
    }

    private static boolean isFixtureLibrary(String name) {
        return name.startsWith("sdk-server-")
                || name.startsWith("sdk-core-")
                || name.startsWith("stack-server-")
                || name.startsWith("stack-core-")
                || name.startsWith("bsd-core-")
                || name.startsWith("bsd-generator-")
                || name.startsWith("netty-")
                || name.startsWith("guava-")
                || name.startsWith("failureaccess-")
                || name.startsWith("annotations-")
                || name.startsWith("bcprov-")
                || name.startsWith("bcpkix-")
                || name.startsWith("bcutil-")
                || name.startsWith("slf4j-api-")
                || name.startsWith("jaxb-")
                || name.startsWith("jakarta.xml.bind-")
                || name.startsWith("jakarta.activation-")
                || name.startsWith("istack-")
                || name.startsWith("txw2-")
                || name.startsWith("stax-ex-")
                || name.startsWith("FastInfoset-");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (fixture != null) {
                fixture.close();
            }
        } finally {
            if (keys != null) {
                Files.deleteIfExists(keys.resolve("server.p12"));
                Files.deleteIfExists(keys.resolve("client.p12"));
                Files.deleteIfExists(keys.resolve("nodes.json"));
                Files.deleteIfExists(keys);
            }
        }
    }

    @TestTemplate
    void pollsAllNodeIdFormsAndRetainsExactValues(TestContainer engine) throws Exception {
        ObjectNode options = unsecured().put("max_records", 6).put("read_batch_size", 2);
        options.putObject("nodes")
                .put("unsigned", "nsu=" + NS + ";i=42")
                .put("words", "nsu=" + NS + ";g=" + GUID)
                .put("opaque", "nsu=" + NS + ";b=AQID")
                .put("bad", "nsu=" + NS + ";s=bad")
                .put("main", SAMPLE)
                .put("other", "nsu=" + AUX_NS + ";s=sample");
        List<String[]> values = run(engine, options);
        Assertions.assertEquals(6, values.size());
        String[] wide = alias(values, "unsigned");
        Assertions.assertEquals("UInt64", wide[7]);
        Assertions.assertEquals("18446744073709551615", wide[8]);
        Assertions.assertEquals("GOOD", wide[9]);
        Assertions.assertEquals("0", wide[10]);
        Assertions.assertEquals("1969-12-31T23:59:59.999999900Z", wide[11]);
        Assertions.assertEquals("73", wide[12]);
        Assertions.assertEquals("2024-01-01T00:00:00.123456700Z", wide[13]);
        Assertions.assertEquals("29", wide[14]);
        Assertions.assertEquals("String[]", alias(values, "words")[7]);
        Assertions.assertTrue(JSON.readTree(alias(values, "words")[8]).get(1).isNull());
        Assertions.assertEquals("水", JSON.readTree(alias(values, "words")[8]).get(0).asText());
        Assertions.assertEquals("café", JSON.readTree(alias(values, "words")[8]).get(2).asText());
        Assertions.assertEquals("AP8C", JSON.readTree(alias(values, "opaque")[8]).asText());
        Assertions.assertEquals("BAD", alias(values, "bad")[9]);
        Assertions.assertEquals("null", alias(values, "bad")[8]);
        Assertions.assertEquals("77", alias(values, "other")[8]);
        Assertions.assertEquals(SAMPLE, alias(values, "main")[1]);

        options.put("max_array_length", 2);
        fail(engine, options, "element or depth limit");
        options.put("max_array_length", 65536).put("max_string_bytes", 2);
        fail(engine, options, "exceeds max_string_bytes");
    }

    @TestTemplate
    void readsWorkerFileAndRejectsAmbiguousSelection(TestContainer engine) throws Exception {
        Path input = keys.resolve("nodes.json");
        Files.writeString(input, JSON.createObjectNode().put("file_node", SAMPLE).toString());
        engine.copyAbsolutePathToContainer(input.toString(), WORKER_FILE);
        ObjectNode options =
                unsecured()
                        .put("node_fetch_mode", "FILE")
                        .put("node_file", WORKER_FILE)
                        .put("max_records", 1);
        Assertions.assertEquals(SAMPLE, alias(run(engine, options), "file_node")[1]);

        Files.writeString(
                input, "{\"duplicate\":\"" + SAMPLE + "\",\"duplicate\":\"" + SAMPLE + "\"}");
        engine.copyAbsolutePathToContainer(input.toString(), WORKER_FILE);
        fail(engine, options, "node_file");
        ObjectNode duplicate = unsecured().put("max_records", 1);
        duplicate.putObject("nodes").put("a", SAMPLE).put("b", "ns=2;s=sample");
        fail(engine, duplicate, "duplicate NodeId");
    }

    @TestTemplate
    void browsesPagedCyclicGraphAndEnforcesBounds(TestContainer engine) throws Exception {
        ObjectNode options =
                unsecured()
                        .put("read_mode", "BROWSE_NODES")
                        .put("browse_root", ROOT)
                        .put("browse_page_size", 1);
        List<String[]> records = run(engine, options);
        Set<String> identities = new HashSet<>();
        records.forEach(
                row -> Assertions.assertTrue(identities.add(row[1]), "duplicate browse node"));
        Assertions.assertFalse(identities.contains(ROOT), "cycle must not re-emit the root");
        Assertions.assertTrue(identities.contains(SAMPLE));
        Assertions.assertTrue(identities.contains("nsu=" + NS + ";b=AQID"));
        String[] bytes =
                records.stream().filter(row -> row[4].equals("bytes")).findFirst().orElseThrow();
        Assertions.assertEquals("NODE", bytes[0]);
        Assertions.assertEquals("nsu=" + NS + ";s=nested", bytes[3]);
        Assertions.assertEquals("Variable", bytes[5]);
        Assertions.assertEquals("2", bytes[6]);
        fail(engine, options.deepCopy().put("max_depth", 1), "max_depth");
        fail(engine, options.deepCopy().put("max_nodes", 2), "max_nodes");
        fail(
                engine,
                options.deepCopy().put("browse_root", "nsu=" + NS + ";s=long-identity"),
                "server NodeId exceeds its textual length");

        ObjectNode empty = options.deepCopy().put("browse_root", "nsu=" + NS + ";s=empty");
        Assertions.assertTrue(run(engine, empty).isEmpty());
        empty.put("read_mode", "POLLING").put("node_fetch_mode", "BROWSE").put("max_records", 1);
        fail(engine, empty, "did not contain any variable");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void refreshesBrowseSelectionWhilePolling(TestContainer engine) throws Exception {
        ObjectNode options =
                unsecured()
                        .put("node_fetch_mode", "BROWSE")
                        .put("browse_root", ROOT)
                        .put("interval_ms", 600)
                        .put("refresh_interval_ms", 300)
                        .put("max_records", 100);
        String before = engine.getServerLogs();
        try (RunningJob job = start(engine, options, "STREAMING")) {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    rows(fresh(engine, before)).stream()
                                            .anyMatch(r -> SAMPLE.equals(r[1])));
            control("/extend");
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    rows(fresh(engine, before)).stream()
                                            .anyMatch(
                                                    r ->
                                                            "later".equals(r[2])
                                                                    && "99".equals(r[8])));
            job.cancel();
        }
    }

    @TestTemplate
    void selectsTheRequestedSecurityModeAndChecksCertificateIdentity(TestContainer engine)
            throws Exception {
        engine.copyAbsolutePathToContainer(keys.resolve("client.p12").toString(), WORKER_KEYS);
        ObjectNode options =
                single(
                        JSON.createObjectNode()
                                .put("endpoint", ENDPOINT)
                                .put("client_key_store", WORKER_KEYS)
                                .put("client_key_alias", "test")
                                .put("application_uri", CLIENT_URI)
                                .put("server_application_uri", SERVER_URI)
                                .put("server_certificate_sha256", fingerprint)
                                .put("password", PASSWORD));
        Assertions.assertEquals(1, run(engine, options).size());
        Assertions.assertEquals(
                1,
                run(
                                engine,
                                options.deepCopy()
                                        .put("endpoint", ENDPOINT.replace("/fixture", "/secure")))
                        .size());
        Assertions.assertEquals(
                1, run(engine, options.deepCopy().put("message_security_mode", "Sign")).size());
        fail(
                engine,
                options.deepCopy().put("server_certificate_sha256", "00".repeat(32)),
                "certificate trust");
        fail(
                engine,
                options.deepCopy().put("server_application_uri", "urn:other"),
                "application URI");
        fail(
                engine,
                options.deepCopy().put("endpoint", ENDPOINT.replace(HOST, "opcua-alias")),
                "hostname");
        fail(
                engine,
                options.deepCopy().put("application_uri", "urn:wrong-client"),
                "PKCS12 client identity");
        String badPassword = "private-incorrect-fixture-password";
        String logs =
                fail(
                        engine,
                        options.deepCopy().put("password", badPassword),
                        "PKCS12 client identity");
        Assertions.assertFalse(logs.contains(badPassword));

        Path untrusted = certificate("untrusted-" + UUID.randomUUID(), CLIENT_URI, "untrusted");
        String untrustedPath = "/tmp/seatunnel-opcua-untrusted.p12";
        try {
            engine.copyAbsolutePathToContainer(untrusted.toString(), untrustedPath);
            X509Certificate leaf =
                    (X509Certificate) OpcUaFixture.load(untrusted).getCertificate("test");
            fail(
                    engine,
                    options.deepCopy().put("client_key_store", untrustedPath),
                    "OpcUa Source:");
            Assertions.assertTrue(
                    control("/rejected")
                            .lines()
                            .anyMatch(leaf.getSerialNumber().toString(16)::equals),
                    "the OPC UA server must reject the untrusted client certificate");
        } finally {
            Files.deleteIfExists(untrusted);
        }
        fail(engine, single(unsecured().put("allow_insecure", false)), "requires allow_insecure");
        fail(
                engine,
                options.deepCopy().put("security_policy", "Basic256"),
                "requires allow_legacy_security");
        fail(
                engine,
                options.deepCopy()
                        .put("security_policy", "Basic256")
                        .put("allow_legacy_security", true),
                "matching anonymous endpoint");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void emitsNativeSubscriptionsAndSignalsReconnectWithoutAffectingAnotherJob(TestContainer engine)
            throws Exception {
        control("/sample/107");
        ObjectNode options =
                single(unsecured())
                        .put("max_records", 0)
                        .put("read_mode", "SUBSCRIBE")
                        .put("interval_ms", 80);
        String log = engine.getServerLogs();
        try (RunningJob first = start(engine, options, "STREAMING");
                RunningJob second = start(engine, options, "STREAMING")) {
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> counts()[3] >= 2);
            awaitValue(engine, log, "107");
            control("/sample/133");
            awaitValue(engine, log, "133");
            first.cancel();
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> counts()[3] == 1);
            control("/disconnect");
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    rows(fresh(engine, log)).stream()
                                            .anyMatch(
                                                    r ->
                                                            "GAP".equals(r[0])
                                                                    && Set.of(
                                                                                    "SESSION_INTERRUPTED",
                                                                                    "SUBSCRIPTION_TRANSFER_FAILED",
                                                                                    "NOTIFICATION_DATA_LOST")
                                                                            .contains(r[15])));
            control("/sample/199");
            awaitValue(engine, log, "199");
            Assertions.assertEquals("RUNNING", engine.getJobStatus(second.id));
            second.cancel();
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void failsLoudlyWhenNativePublishOverrunsTheClientQueue(TestContainer engine) throws Exception {
        ObjectNode options =
                unsecured()
                        .put("read_mode", "SUBSCRIBE")
                        .put("node_fetch_mode", "BROWSE")
                        .put("browse_root", "nsu=" + NS + ";s=burst")
                        .put("queue_capacity", 1)
                        .put("interval_ms", 250)
                        .put("max_records", 32);
        String before = engine.getServerLogs();
        try (RunningJob job = start(engine, options, "STREAMING")) {
            Container.ExecResult result = job.finished.get(90, TimeUnit.SECONDS);
            String logs = result.getStderr() + result.getStdout() + fresh(engine, before);
            Assertions.assertNotEquals(0, result.getExitCode(), logs);
            Assertions.assertTrue(logs.contains("subscription notification queue overflow"), logs);
            Assertions.assertTrue(rows(fresh(engine, before)).size() < 32);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void clearsUnresponsiveDiscoveryOnDeadlineAndOnCancel(TestContainer engine) throws Exception {
        ObjectNode options = single(unsecured().put("endpoint", SILENT_ENDPOINT));
        int before = counts()[1];
        fail(engine, options.deepCopy().put("request_timeout_ms", 1400), "deadline");
        Awaitility.await().atMost(8, TimeUnit.SECONDS).until(() -> counts()[2] == 0);
        Assertions.assertEquals(before + 1, counts()[1]);

        try (RunningJob job =
                start(engine, options.deepCopy().put("request_timeout_ms", 60000), "STREAMING")) {
            Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> counts()[2] == 1);
            job.cancel();
            Awaitility.await().atMost(8, TimeUnit.SECONDS).until(() -> counts()[2] == 0);
            Assertions.assertEquals(before + 2, counts()[1]);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresBoundedProgressFromSavepointThroughTheRealEngine(TestContainer engine)
            throws Exception {
        ObjectNode options = single(unsecured()).put("max_records", 16).put("interval_ms", 750);
        String before = engine.getServerLogs();
        try (RunningJob job = start(engine, options, CHECKPOINT_ENV)) {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () ->
                                    rows(fresh(engine, before)).size() >= 2
                                            && engine.getCompletedCheckpointCount(job.id) > 0);
            succeed(engine.savepointJob(job.id));
            status(engine, job.id, "SAVEPOINT_DONE");
            succeed(job.finished.get(45, TimeUnit.SECONDS));
            int emitted = rows(fresh(engine, before)).size();
            Assertions.assertTrue(emitted >= 2 && emitted < 16, "expected partial output");

            String resumeLog = engine.getServerLogs();
            Container.ExecResult resumed =
                    engine.restoreJobWithCheckpoint(resource(job.file), job.id, jobId());
            succeed(resumed);
            List<String[]> after = output(engine, resumeLog, resumed);
            Assertions.assertEquals(16 - emitted, after.size());
            after.forEach(row -> Assertions.assertEquals(SAMPLE, row[1]));
        }
    }

    private static String[] alias(List<String[]> records, String alias) {
        return records.stream().filter(row -> alias.equals(row[2])).findFirst().orElseThrow();
    }

    private static ObjectNode unsecured() {
        return JSON.createObjectNode()
                .put("endpoint", ENDPOINT)
                .put("security_policy", "None")
                .put("allow_insecure", true);
    }

    private static ObjectNode single(ObjectNode options) {
        options.put("max_records", 1).putObject("nodes").put("sample", SAMPLE);
        return options;
    }

    private static Path config(ObjectNode options, String mode) throws IOException {
        Path resources = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(resources);
        Path file = Files.createTempFile(resources, "opcua-e2e-", ".conf");
        return Files.writeString(
                file,
                "env {\n  parallelism = 1\n  job.retry.times = 0\n  job.mode = "
                        + mode
                        + "\n}\nsource {\n  OpcUa "
                        + options
                        + "\n}\nsink {\n  Console {}\n}\n");
    }

    private static List<String[]> run(TestContainer engine, ObjectNode options) throws Exception {
        Path file = config(options, "BATCH");
        String before = engine.getServerLogs();
        try {
            Container.ExecResult result = engine.executeJob(resource(file));
            succeed(result);
            return output(engine, before, result);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static String fail(TestContainer engine, ObjectNode options, String reason)
            throws Exception {
        Path file = config(options, "BATCH");
        String before = engine.getServerLogs();
        try {
            Container.ExecResult result = engine.executeJob(resource(file));
            String logs =
                    result.getStdout() + "\n" + result.getStderr() + "\n" + fresh(engine, before);
            Assertions.assertNotEquals(0, result.getExitCode(), logs);
            Assertions.assertTrue(logs.contains(reason), () -> "Expected " + reason + "\n" + logs);
            Assertions.assertFalse(logs.contains(PASSWORD));
            Assertions.assertTrue(
                    output(engine, before, result).isEmpty(), "failed job emitted partial data");
            return logs;
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static List<String[]> output(
            TestContainer engine, String before, Container.ExecResult result) {
        List<String[]> server = rows(fresh(engine, before));
        return server.isEmpty() ? rows(result.getStdout()) : server;
    }

    private static List<String[]> rows(String log) {
        List<String[]> result = new ArrayList<>();
        for (String line : log.split("\\R")) {
            int kind = line.indexOf("SeaTunnelRow#kind=");
            int value = kind < 0 ? -1 : line.indexOf(" : ", kind);
            if (value >= 0) {
                String[] row = line.substring(value + 3).split(", ", -1);
                if (Set.of("NODE", "GAP", "VALUE").contains(row[0])) {
                    Assertions.assertEquals(16, row.length, line);
                    result.add(row);
                }
            }
        }
        return result;
    }

    private static void awaitValue(TestContainer engine, String log, String expected) {
        Awaitility.await()
                .atMost(25, TimeUnit.SECONDS)
                .until(
                        () ->
                                rows(fresh(engine, log)).stream()
                                        .anyMatch(r -> expected.equals(r[8])));
    }

    private Path certificate(String name, String uri, String dns) throws Exception {
        Path file = keys.resolve(name + ".p12");
        Process keytool =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "test",
                                "-dname",
                                "CN=" + dns,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "3",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                PASSWORD,
                                "-keypass",
                                PASSWORD,
                                "-ext",
                                "SAN=DNS:" + dns + ",URI:" + uri,
                                "-ext",
                                "KU=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment,keyCertSign",
                                "-ext",
                                "EKU=clientAuth,serverAuth")
                        .redirectErrorStream(true)
                        .start();
        try {
            Assertions.assertTrue(
                    keytool.waitFor(20, TimeUnit.SECONDS), "test certificate timeout");
            Assertions.assertEquals(
                    0,
                    keytool.exitValue(),
                    new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            keytool.destroyForcibly();
        }
        return file;
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder();
        for (byte b : bytes) {
            value.append(String.format(Locale.ROOT, "%02x", b & 0xff));
        }
        return value.toString();
    }

    private int[] counts() throws IOException {
        return Arrays.stream(control("/counts").split(",")).mapToInt(Integer::parseInt).toArray();
    }

    private String control(String path) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL("http", fixture.getHost(), fixture.getMappedPort(CONTROL), path)
                                .openConnection();
        connection.setConnectTimeout(4000);
        connection.setReadTimeout(4000);
        try (InputStream stream = connection.getInputStream()) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } finally {
            connection.disconnect();
        }
    }

    private static String fresh(TestContainer engine, String baseline) {
        String log = engine.getServerLogs();
        return log.startsWith(baseline) ? log.substring(baseline.length()) : "";
    }

    private static void succeed(Container.ExecResult result) {
        Assertions.assertEquals(
                0, result.getExitCode(), result.getStdout() + "\n" + result.getStderr());
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static String resource(Path config) {
        return "/" + config.getFileName();
    }

    private static void status(TestContainer engine, String id, String expected) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> expected.equals(engine.getJobStatus(id)));
    }

    private static RunningJob start(TestContainer engine, ObjectNode options, String mode)
            throws IOException {
        return new RunningJob(engine, config(options, mode));
    }

    private static final class RunningJob implements AutoCloseable {
        private final TestContainer engine;
        private final Path file;
        private final String id = jobId();
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final Future<Container.ExecResult> finished;

        private RunningJob(TestContainer engine, Path file) {
            this.engine = engine;
            this.file = file;
            finished = executor.submit(() -> engine.executeJob(resource(file), id));
        }

        private void cancel() throws Exception {
            succeed(engine.cancelJob(id));
            status(engine, id, "CANCELED");
            finished.get(40, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            try {
                if ("RUNNING".equals(engine.getJobStatus(id))) {
                    engine.cancelJob(id);
                }
            } finally {
                executor.shutdownNow();
                Files.deleteIfExists(file);
            }
        }
    }
}
