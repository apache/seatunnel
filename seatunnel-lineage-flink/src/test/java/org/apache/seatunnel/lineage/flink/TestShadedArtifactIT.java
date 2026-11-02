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

package org.apache.seatunnel.lineage.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sends one event from a JVM that has only the shaded artifact and slf4j-api on its class path, the
 * way a JobManager runs the status hook.
 *
 * <p>This is not a duplicate of the unit tests. Both defects it guards are invisible to them and
 * silent in production, because the backend contains every failure in a warning and lets the job
 * succeed without its lineage:
 *
 * <ul>
 *   <li>Relocating Jackson and the OpenLineage model in separate passes, or relocating only one of
 *       them, breaks the model's {@code @JsonAnyGetter} and nests every facet property under a
 *       literal {@code additionalProperties} key that the receiver stores verbatim.
 *   <li>Apache HttpClient calls commons-logging directly. Dropping the bridge from the artifact
 *       leaves {@code HttpClients.createDefault()} throwing NoClassDefFoundError on a JobManager,
 *       whose lib/ supplies no commons-logging of its own.
 * </ul>
 *
 * <p>Run with {@code ./mvnw -pl seatunnel-lineage-flink -DskipUT -DskipIT=false verify}.
 */
class TestShadedArtifactIT {

    @Test
    void refusesToRedirectAnEnvironmentTokenToTheJobReceiver() throws Exception {
        BlockingQueue<String> bodies = new ArrayBlockingQueue<>(4);
        AtomicReference<String> authorization = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
                    new CapturingHandler(bodies).handle(exchange);
                });
        server.start();
        try {
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            Map<String, String> environment = new HashMap<>();
            environment.put("OPENLINEAGE_AUTH_TOKEN", "fixture-token");
            environment.put("OPENLINEAGE_URL", base + "/trusted");
            String rejected = run(base + "/job", environment, 1);
            Assertions.assertNull(bodies.poll(), "a rejected receiver must not receive a request");
            Assertions.assertFalse(rejected.contains("fixture-token"));
            run(base + "/trusted", environment, 0);
            Assertions.assertNotNull(bodies.poll(30, TimeUnit.SECONDS));
            Assertions.assertEquals("Bearer fixture-token", authorization.get());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void emitsAValidEventWithNothingButTheShadedArtifact() throws Exception {
        BlockingQueue<String> bodies = new ArrayBlockingQueue<>(4);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v1/lineage", new CapturingHandler(bodies));
        server.start();
        String body;
        try {
            String endpoint =
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1/lineage";
            run(endpoint);
            body = bodies.poll(30, TimeUnit.SECONDS);
        } finally {
            server.stop(0);
        }

        Assertions.assertNotNull(body, "the shaded artifact sent no request");
        Assertions.assertFalse(
                body.contains("\"additionalProperties\""),
                "relocation broke @JsonAnyGetter, so facets are nested: " + body);
        Assertions.assertTrue(body.contains("\"engine\":\"flink\""), body);
        Assertions.assertTrue(body.contains("\"flink_job_id\":\"shaded-artifact-it\""), body);
        Assertions.assertTrue(body.contains("\"rowCount\":7"), body);
        Assertions.assertTrue(body.contains("\"name\":\"dw.orders\""), body);
    }

    /** Runs the driver in a JVM that sees only what a JobManager's lib/ would supply. */
    private static void run(String endpoint) throws Exception {
        run(endpoint, Collections.emptyMap(), 0);
    }

    private static String run(String endpoint, Map<String, String> environment, int expectedExit)
            throws Exception {
        String classPath =
                new File("target/test-classes").getAbsolutePath()
                        + File.pathSeparator
                        + shadedArtifact().getAbsolutePath()
                        + File.pathSeparator
                        + jarOf(LoggerFactory.class).getAbsolutePath();
        ProcessBuilder builder =
                new ProcessBuilder(
                        new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
                        "-cp",
                        classPath,
                        ShadedArtifactDriver.class.getName(),
                        endpoint);
        builder.redirectErrorStream(true);
        builder.environment().keySet().removeIf(key -> key.startsWith("OPENLINEAGE_"));
        builder.environment().putAll(environment);
        return runProcess(builder, 60, TimeUnit.SECONDS, expectedExit);
    }

    private static String runProcess(
            ProcessBuilder builder, long timeout, TimeUnit unit, int expectedExit)
            throws Exception {
        Path outputFile = Files.createTempFile("lineage-driver-", ".log");
        Process process = null;
        try {
            builder.redirectErrorStream(true).redirectOutput(outputFile.toFile());
            process = builder.start();
            boolean finished = process.waitFor(timeout, unit);
            if (!finished) {
                process.destroyForcibly();
                Assertions.assertTrue(
                        process.waitFor(5, TimeUnit.SECONDS),
                        "the driver JVM could not be stopped");
            }
            String output = new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8);
            Assertions.assertTrue(finished, "the driver JVM did not exit: " + output);
            Assertions.assertEquals(expectedExit, process.exitValue(), output);
            return output;
        } finally {
            try {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                    Assertions.assertTrue(
                            process.waitFor(5, TimeUnit.SECONDS),
                            "the driver JVM could not be stopped during cleanup");
                }
            } finally {
                Files.deleteIfExists(outputFile);
            }
        }
    }

    @Test
    void terminatesAStuckDriver() {
        ProcessBuilder builder =
                new ProcessBuilder(
                        new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
                        "-cp",
                        new File("target/test-classes").getAbsolutePath(),
                        ShadedArtifactDriver.class.getName(),
                        "hang");
        AssertionError error =
                Assertions.assertThrows(
                        AssertionError.class, () -> runProcess(builder, 2, TimeUnit.SECONDS, 0));
        Assertions.assertTrue(error.getMessage().contains("did not exit"));
    }

    @Test
    void boundsTheWaitWhenTheDriverCannotBeStopped() throws Exception {
        ProcessBuilder builder = Mockito.mock(ProcessBuilder.class);
        Process process = Mockito.mock(Process.class);
        Mockito.when(builder.redirectErrorStream(true)).thenReturn(builder);
        Mockito.when(builder.redirectOutput(Mockito.any(File.class))).thenReturn(builder);
        Mockito.when(builder.start()).thenReturn(process);
        Mockito.when(process.waitFor(Mockito.anyLong(), Mockito.any(TimeUnit.class)))
                .thenReturn(false);
        Mockito.when(process.isAlive()).thenReturn(true);
        Mockito.when(process.destroyForcibly()).thenReturn(process);

        AssertionError error =
                Assertions.assertThrows(
                        AssertionError.class,
                        () -> runProcess(builder, 1, TimeUnit.MILLISECONDS, 0));
        Assertions.assertTrue(error.getMessage().contains("could not be stopped"));
        Mockito.verify(process, Mockito.never()).waitFor();
        Mockito.verify(process, Mockito.atLeastOnce()).waitFor(5, TimeUnit.SECONDS);
    }

    /** Locates the deployable artifact this module attaches during the package phase. */
    private static File shadedArtifact() {
        File[] candidates =
                new File("target")
                        .listFiles(
                                (directory, name) ->
                                        name.startsWith("seatunnel-lineage-flink")
                                                && name.endsWith("-shaded.jar"));
        Assertions.assertNotNull(candidates, "target/ does not exist; run the package phase first");
        Assertions.assertEquals(
                1,
                candidates.length,
                "expected exactly one shaded artifact in target/, found "
                        + java.util.Arrays.toString(candidates));
        return candidates[0];
    }

    private static File jarOf(Class<?> type) throws Exception {
        return new File(type.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    private static String read(InputStream stream) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read = stream.read(buffer); read > 0; read = stream.read(buffer)) {
            bytes.write(buffer, 0, read);
        }
        return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    }

    private static final class CapturingHandler implements HttpHandler {
        private final BlockingQueue<String> bodies;

        private CapturingHandler(BlockingQueue<String> bodies) {
            this.bodies = bodies;
        }

        @Override
        public void handle(HttpExchange exchange) throws java.io.IOException {
            try {
                bodies.offer(read(exchange.getRequestBody()));
            } catch (Exception error) {
                throw new java.io.IOException(error);
            }
            exchange.sendResponseHeaders(201, -1);
            exchange.close();
        }
    }
}
