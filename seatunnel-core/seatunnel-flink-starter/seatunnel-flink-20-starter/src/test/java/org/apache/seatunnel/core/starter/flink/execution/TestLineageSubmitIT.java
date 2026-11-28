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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

/**
 * Submits a lineage-enabled job through {@code execute(StreamGraph)} with the shaded hook jar
 * installed under a JobManager {@code lib/} directory, and checks that a stub receiver sees START
 * and a terminal event.
 */
class TestLineageSubmitIT {

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void executeStreamGraphReportsStartAndTerminalEvent() throws Exception {
        File shaded = shadedArtifact();
        Path flinkHome = Files.createTempDirectory("seatunnel-lineage-flink-home");
        Path lib = Files.createDirectory(flinkHome.resolve("lib"));
        Path installed = lib.resolve(shaded.getName());
        Files.copy(shaded.toPath(), installed);
        try (JarFile jarFile = new JarFile(installed.toFile(), false)) {
            Assertions.assertNotNull(
                    jarFile.getJarEntry(
                            "org/apache/seatunnel/lineage/flink/LineageJobStatusHook.class"),
                    "the jar installed in lib/ must contain the status hook");
        }

        BlockingQueue<String> bodies = new ArrayBlockingQueue<>(8);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/lineage",
                exchange -> {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    try (InputStream input = exchange.getRequestBody()) {
                        byte[] chunk = new byte[4096];
                        int read;
                        while ((read = input.read(chunk)) >= 0) {
                            buffer.write(chunk, 0, read);
                        }
                    }
                    bodies.add(buffer.toString(StandardCharsets.UTF_8));
                    exchange.sendResponseHeaders(204, -1);
                    exchange.close();
                });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/lineage";
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.createLocalEnvironment(1);
            org.apache.flink.configuration.Configuration overrides =
                    new org.apache.flink.configuration.Configuration();
            overrides.set(DeploymentOptions.ATTACHED, true);
            environment.configure(overrides);
            environment.fromElements(1).print();

            Map<String, Object> options = new LinkedHashMap<>();
            options.put(LineageConfig.ENABLED, true);
            options.put(LineageConfig.URL, endpoint);
            options.put(LineageConfig.TIMEOUT_MS, 2000);
            options.put(LineageConfig.RETRY_TIMES, 0);
            options.put(LineageConfig.HEARTBEAT_MIN_INTERVAL_MS, 0);
            LineageConfig config =
                    FlinkLineageSupport.resolveConfig(
                            options,
                            Collections.<String, Object>emptyMap(),
                            Collections.<String, Object>emptyMap());
            StreamGraph streamGraph =
                    FlinkLineageSupport.register(
                            environment,
                            config,
                            new JobContext(),
                            Collections.<LineageDataset>emptyList(),
                            Collections.singletonList(
                                    LineageDataset.of("mysql://db", "warehouse.orders")),
                            "lineage-submit-it");
            Assertions.assertNotNull(streamGraph, "registration must return the graph to execute");
            Assertions.assertFalse(
                    streamGraph.getJobStatusHooks().isEmpty(),
                    "execute(StreamGraph) must carry the status hook");

            JobExecutionResult result = environment.execute(streamGraph);
            Assertions.assertNotNull(result);
            Assertions.assertTrue(
                    result.isJobExecutionResult(),
                    "the local submission must finish attached so the hook can observe FINISHED");

            String first = bodies.poll(30, TimeUnit.SECONDS);
            String second = bodies.poll(60, TimeUnit.SECONDS);
            Assertions.assertNotNull(first, "the receiver got no lineage event");
            Assertions.assertNotNull(
                    second, "the receiver got no terminal lineage event: " + first);
            String delivered = first + "\n" + second;
            Assertions.assertTrue(delivered.contains("\"START\""), delivered);
            Assertions.assertTrue(
                    delivered.contains("\"COMPLETE\"")
                            || delivered.contains("\"FAIL\"")
                            || delivered.contains("\"ABORT\""),
                    delivered);
            Assertions.assertTrue(delivered.contains("\"COMPLETE\""), delivered);
        } finally {
            server.stop(0);
            deleteRecursively(flinkHome);
        }
    }

    private static File shadedArtifact() {
        String classpath = System.getProperty("java.class.path");
        for (String entry : classpath.split(File.pathSeparator)) {
            if (!entry.contains("seatunnel-lineage-flink")) {
                continue;
            }
            File targetDir = new File(entry).getParentFile();
            File match = matchShaded(targetDir);
            if (match != null) {
                return match;
            }
        }
        File match =
                matchShaded(
                        new File("../../../../seatunnel-lineage-flink/target").getAbsoluteFile());
        Assertions.assertNotNull(
                match, "seatunnel-lineage-flink shaded jar was not built; run package first");
        return match;
    }

    private static File matchShaded(File directory) {
        File[] candidates =
                directory.listFiles(
                        (dir, name) ->
                                name.startsWith("seatunnel-lineage-flink")
                                        && name.endsWith("-shaded.jar"));
        if (candidates == null) {
            return null;
        }
        for (File candidate : candidates) {
            if (candidate.isFile() && candidate.length() > 0) {
                return candidate;
            }
        }
        return null;
    }

    private static void deleteRecursively(Path root) throws java.io.IOException {
        if (!Files.exists(root)) {
            return;
        }
        java.util.List<Path> paths = new java.util.ArrayList<>();
        Files.walk(root).forEach(paths::add);
        for (int index = paths.size() - 1; index >= 0; index--) {
            Files.deleteIfExists(paths.get(index));
        }
    }
}
