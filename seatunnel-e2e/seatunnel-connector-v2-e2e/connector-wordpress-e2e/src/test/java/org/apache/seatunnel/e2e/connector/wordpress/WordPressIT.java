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

package org.apache.seatunnel.e2e.connector.wordpress;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

/**
 * Exercises the installed Source through an isolated HTTPS fixture in Zeta, checking exact rows and
 * checkpoint/cancellation progress while a later response is held by the fixture.
 */
class WordPressIT extends TestSuiteBase {
    private static final String HOST = "public-api.wordpress.com";
    private static final String STORE_PASSWORD = "fixture-only";
    private static final String TRUSTSTORE = "/tmp/wordpress-fixture.p12";

    @Test
    void packagedWordPressSourceReadsBothPagesInZeta(@TempDir Path tmp) throws Exception {
        Path serverPem = tmp.resolve("wordpress-fixture.pem");
        Path truststore = tmp.resolve("wordpress-trust.p12");
        createFixtureCertificate(tmp, serverPem, truststore);

        try (GenericContainer<?> fixture = fixture(serverPem)) {
            fixture.start();
            SeaTunnelContainer engine = engine(truststore);

            try {
                engine.startUp();
                Container.ExecResult result = engine.executeJob("/wordpress_to_console.conf");
                Assertions.assertEquals(
                        0, result.getExitCode(), result.getStdout() + result.getStderr());
                List<String> requests =
                        fixture.getLogs()
                                .lines()
                                .filter(
                                        line ->
                                                line.startsWith(
                                                        "verified WordPress fixture offset="))
                                .collect(Collectors.toList());
                Assertions.assertEquals(
                        Arrays.asList(
                                "verified WordPress fixture offset=0",
                                "verified WordPress fixture offset=1"),
                        requests);

                String logs = engine.getServerLogs();
                Assertions.assertTrue(
                        logs.contains(
                                "output rowType: id<BIGINT>, slug<STRING>, title<STRING>, url<STRING>, published_at<TIMESTAMP>, modified_at<TIMESTAMP>"),
                        logs);
                String marker = "SeaTunnelRow#tableId=posts SeaTunnelRow#kind=INSERT : ";
                List<String> rows =
                        logs.lines()
                                .filter(line -> line.contains(marker))
                                .map(line -> line.substring(line.indexOf(marker) + marker.length()))
                                .collect(Collectors.toList());
                Assertions.assertEquals(
                        Arrays.asList(
                                "7, earliest, First &amp; exact, https://fixture.wordpress.com/earliest/, 2005-09-01T00:00, 2005-09-02T06:34:56",
                                "8, latest, Second, https://fixture.wordpress.com/latest/, 2005-09-30T23:59:59, 2005-10-01T00:00:01"),
                        rows);
            } finally {
                engine.tearDown();
            }
        }
    }

    @Test
    void zetaCancellationClosesBlockedWordPressRequestBeforeItsDeadline(@TempDir Path tmp)
            throws Exception {
        Path serverPem = tmp.resolve("wordpress-fixture.pem");
        Path truststore = tmp.resolve("wordpress-trust.p12");
        createFixtureCertificate(tmp, serverPem, truststore);

        try (GenericContainer<?> fixture = fixture(serverPem).withEnv("WORDPRESS_HOLD_PAGE", "1")) {
            fixture.start();
            SeaTunnelContainer engine = engine(truststore);
            try {
                engine.startUp();
                String jobId = String.valueOf(JobIdGenerator.newJobId());
                CompletableFuture<Container.ExecResult> job =
                        CompletableFuture.supplyAsync(
                                () -> {
                                    try {
                                        return engine.executeJob(
                                                "/wordpress_to_console_checkpoint.conf", jobId);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new CompletionException(e);
                                    } catch (IOException e) {
                                        throw new CompletionException(e);
                                    }
                                });

                await().atMost(60, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        fixture.getLogs()
                                                .contains("WordPress fixture holds offset=1"));
                Assertions.assertFalse(
                        job.isDone(), "The blocked source job unexpectedly completed");
                // Observe a newly completed checkpoint after the second HTTP page is held.
                // This requires the real source barrier to pass while that request is in flight.
                long completedBefore = completedCheckpoints(engine.getServerLogs(), jobId);
                await().atMost(10, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        completedCheckpoints(engine.getServerLogs(), jobId)
                                                > completedBefore);
                Assertions.assertFalse(
                        job.isDone(), "The held source finished before checkpoint verification");
                Assertions.assertFalse(
                        fixture.getLogs().contains("WordPress fixture saw offset=1 disconnect"),
                        "The held request disconnected before cancellation");
                CompletableFuture<Void> stop =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        engine.stopJob(jobId);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new CompletionException(e);
                                    } catch (IOException e) {
                                        throw new CompletionException(e);
                                    }
                                });
                // A client deadline or socket timeout first fires after 30 seconds.
                await().atMost(15, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        fixture.getLogs()
                                                .contains(
                                                        "WordPress fixture saw offset=1 disconnect"));
                stop.get(30, TimeUnit.SECONDS);
                job.get(30, TimeUnit.SECONDS);
                Assertions.assertEquals("CANCELED", engine.getJobStatus(jobId));
            } finally {
                engine.tearDown();
            }
        }
    }

    private static long completedCheckpoints(String logs, String jobId) {
        return logs.lines()
                .filter(
                        line ->
                                line.contains(
                                        "pending checkpoint notify finished, job id: "
                                                + jobId
                                                + ","))
                .count();
    }

    private static GenericContainer<?> fixture(Path serverPem) {
        // Keep DNS and trust overrides inside the isolated test network/JVM.
        return new GenericContainer<>(DockerImageName.parse("python:3.12.12-alpine3.22"))
                .withNetwork(NETWORK)
                .withNetworkAliases(HOST)
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("wordpress_fixture.py"),
                        "/tmp/wordpress_fixture.py")
                .withCopyFileToContainer(
                        MountableFile.forHostPath(serverPem), "/tmp/wordpress-fixture.pem")
                .withCommand("python", "-u", "/tmp/wordpress_fixture.py")
                .waitingFor(Wait.forLogMessage(".*WordPress HTTPS fixture ready.*", 1));
    }

    private static SeaTunnelContainer engine(Path truststore) {
        return new SeaTunnelContainer() {
            @Override
            protected String getJavaToolOptions() {
                return "-Djavax.net.ssl.trustStore="
                        + TRUSTSTORE
                        + " -Djavax.net.ssl.trustStorePassword="
                        + STORE_PASSWORD;
            }

            @Override
            protected void executeExtraCommands(GenericContainer<?> runtime) {
                runtime.withCopyFileToContainer(MountableFile.forHostPath(truststore), TRUSTSTORE);
            }
        };
    }

    private static void createFixtureCertificate(Path tmp, Path pem, Path truststore)
            throws Exception {
        Path keystore = tmp.resolve("generated.p12");
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        Process generate =
                new ProcessBuilder(
                                keytool.toString(),
                                "-genkeypair",
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
                                "SAN=DNS:" + HOST,
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                keystore.toString(),
                                "-storepass",
                                STORE_PASSWORD)
                        .redirectErrorStream(true)
                        .redirectOutput(tmp.resolve("keytool.log").toFile())
                        .start();
        if (!generate.waitFor(30, TimeUnit.SECONDS)) {
            generate.destroyForcibly();
            Assertions.fail("Timed out generating the WordPress fixture certificate");
        }
        Assertions.assertEquals(
                0, generate.exitValue(), Files.readString(tmp.resolve("keytool.log")));

        char[] password = STORE_PASSWORD.toCharArray();
        KeyStore key = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keystore)) {
            key.load(in, password);
        }
        Certificate cert = key.getCertificate("fixture");
        Files.write(
                pem,
                (pemBlock("CERTIFICATE", cert.getEncoded())
                                + pemBlock(
                                        "PRIVATE KEY",
                                        key.getKey("fixture", password).getEncoded()))
                        .getBytes(StandardCharsets.US_ASCII));

        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", cert);
        try (OutputStream out = Files.newOutputStream(truststore)) {
            trust.store(out, password);
        }
    }

    private static String pemBlock(String label, byte[] bytes) {
        return "-----BEGIN "
                + label
                + "-----\n"
                + Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(bytes)
                + "\n-----END "
                + label
                + "-----\n";
    }
}
