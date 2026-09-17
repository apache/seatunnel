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

package org.apache.seatunnel.e2e.connector.syslog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkOptions;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises engine factory discovery and an independent RFC 5424/5425 receiver. */
@Slf4j
public class SyslogSinkIT extends TestSuiteBase implements TestResource {
    // syslog-ng 4.12.0, pinned by its multi-platform image digest.
    private static final DockerImageName IMAGE =
            DockerImageName.parse(
                    "balabit/syslog-ng@sha256:f564a6906b03f0dc7fa9edf7925956abf3935a474c86d8a1384367889ce6e090");
    @TempDir static Path directory;
    private GenericContainer<?> receiver;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        Config secretConfig =
                ConfigFactory.parseString(
                        "sink { Syslog { "
                                + SyslogSinkOptions.KEY_STORE_PASSWORD.key()
                                + " = \"masking-test-value\" } }");
        assertFalse(
                ConfigBuilder.configDesensitization(
                                secretConfig.root().unwrapped(),
                                ConfigShadeUtils.getLogDesensitizationOptions(secretConfig))
                        .toString()
                        .contains("masking-test-value"));
        createCertificate();
        receiver =
                new GenericContainer<>(IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("syslog-receiver")
                        .withExposedPorts(6514)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(directory.resolve("key.pem")),
                                "/tmp/receiver-key.pem")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(directory.resolve("cert.pem")),
                                "/tmp/receiver-cert.pem")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("docker/syslog-ng.conf"),
                                "/etc/syslog-ng/syslog-ng.conf")
                        .withCreateContainerCmdModifier(
                                command -> command.withEntrypoint("/usr/sbin/syslog-ng"))
                        .withCommand("-F", "--no-caps", "-f", "/etc/syslog-ng/syslog-ng.conf")
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))
                        .withLogConsumer(new Slf4jLogConsumer(log));
        receiver.start();
    }

    @Override
    @AfterAll
    public void tearDown() {
        try {
            if (receiver != null) {
                receiver.close();
            }
        } finally {
            NETWORK.close();
        }
    }

    @TestTemplate
    public void sendsRfc5424OverTls(TestContainer container) throws Exception {
        Container.ExecResult truncate =
                receiver.execInContainer("sh", "-c", ": > /tmp/received.json");
        assertEquals(0, truncate.getExitCode(), truncate.getStderr());
        // The harness shares this mount between the Flink job manager and task managers.
        container.copyAbsolutePathToContainer(
                directory.resolve("cert.pem").toString(), "/tmp/seatunnel_mnt/syslog-ca.pem");
        Container.ExecResult result = container.executeJob("/fake_to_syslog.conf");
        assertEquals(0, result.getExitCode(), result.getStderr());
        List<JsonNode> rows =
                Awaitility.await()
                        .atMost(Duration.ofSeconds(30))
                        .until(this::records, records -> records.size() >= 2);
        assertEquals(2, rows.size(), "Unexpected extra receiver records");
        JsonNode first =
                rows.stream()
                        .filter(row -> "ID47".equals(row.path("MSGID").asText()))
                        .findFirst()
                        .orElse(null);
        JsonNode last =
                rows.stream()
                        .filter(row -> "ID48".equals(row.path("MSGID").asText()))
                        .findFirst()
                        .orElse(null);
        assertNotNull(first);
        assertNotNull(last);
        assertEquals("origin", first.path("HOST").asText());
        assertEquals("seatunnel", first.path("PROGRAM").asText());
        assertEquals("123", first.path("PID").asText());
        assertEquals("hello \u4e16\u754c\nsecond line", first.path("MESSAGE").asText());
        assertEquals("last message", last.path("MESSAGE").asText());
        // format-json may render dotted names as nested objects; find the parameter recursively.
        assertTrue(first.toString().contains("quote\\\"slash\\\\bracket]"), first.toString());
    }

    private List<JsonNode> records() throws Exception {
        Container.ExecResult result = receiver.execInContainer("cat", "/tmp/received.json");
        assertEquals(0, result.getExitCode(), result.getStderr());
        try (MappingIterator<JsonNode> iterator =
                new ObjectMapper().readerFor(JsonNode.class).readValues(result.getStdout())) {
            return iterator.readAll();
        }
    }

    private static void createCertificate() throws Exception {
        Path keyStore = directory.resolve("receiver.jks");
        List<String> command = new ArrayList<>();
        Collections.addAll(
                command,
                Paths.get(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair",
                "-alias",
                "receiver",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-sigalg",
                "SHA256withRSA",
                "-dname",
                "CN=syslog-receiver",
                "-ext",
                "SAN=dns:syslog-receiver",
                "-ext",
                "EKU=serverAuth",
                "-startdate",
                "2020/01/01 00:00:00",
                "-validity",
                "36500",
                "-storetype",
                "JKS",
                "-keystore",
                keyStore.toString(),
                "-storepass",
                "test-password",
                "-keypass",
                "test-password",
                "-noprompt");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "keytool timed out");
            assertEquals(0, process.exitValue(), "keytool fixture generation failed");
        } finally {
            process.destroyForcibly();
        }
        KeyStore store = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(keyStore)) {
            store.load(input, "test-password".toCharArray());
        }
        writePem(
                directory.resolve("key.pem"),
                "PRIVATE KEY",
                store.getKey("receiver", "test-password".toCharArray()).getEncoded());
        writePem(
                directory.resolve("cert.pem"),
                "CERTIFICATE",
                store.getCertificate("receiver").getEncoded());
    }

    private static void writePem(Path path, String label, byte[] der) throws Exception {
        String encoded = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        Files.write(
                path,
                ("-----BEGIN " + label + "-----\n" + encoded + "\n-----END " + label + "-----\n")
                        .getBytes(StandardCharsets.US_ASCII));
    }
}
