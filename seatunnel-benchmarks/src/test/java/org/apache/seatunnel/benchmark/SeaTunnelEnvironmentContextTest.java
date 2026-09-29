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

package org.apache.seatunnel.benchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeaTunnelEnvironmentContextTest {

    @TempDir Path resultDirectory;

    @Test
    void shouldExecuteAllPipelinesOnEmbeddedZeta() throws Exception {
        String property = SeaTunnelEnvironmentContext.RESULT_DIRECTORY_PROPERTY;
        String previousResultDirectory = System.getProperty(property);
        System.setProperty(property, resultDirectory.toString());
        try {
            executeDefaultEnvironmentPipelines();
            executeObservabilityEnvironmentPipeline();
            executeTraceEnvironmentPipeline();
            executeObservabilityTraceEnvironmentPipeline();
        } finally {
            if (previousResultDirectory == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previousResultDirectory);
            }
        }
    }

    private void executeDefaultEnvironmentPipelines() throws Exception {
        SeaTunnelEnvironmentContext context = new SeaTunnelEnvironmentContext();
        try {
            context.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(2_000L, 0L, 1, 32, 4);

            context.setUpIteration(iterationParams(IterationType.WARMUP));
            context.execute(BenchmarkPipeline.SOURCE_SINK, options);
            assertEquals(0L, resultFileCount(resultDirectory));

            context.setUpIteration(iterationParams(IterationType.MEASUREMENT));

            Map<BenchmarkPipeline, BenchmarkRunResult> results =
                    new EnumMap<>(BenchmarkPipeline.class);
            for (BenchmarkPipeline pipeline :
                    new BenchmarkPipeline[] {
                        BenchmarkPipeline.SOURCE_SINK, BenchmarkPipeline.SOURCE_TRANSFORM_SINK
                    }) {
                results.put(pipeline, context.execute(pipeline, options));
            }

            BenchmarkRunResult direct = results.get(BenchmarkPipeline.SOURCE_SINK);
            assertEquals(2_000L, direct.getProcessedRows());
            assertEquals(0L, direct.getChecksum());
            assertCompleteTransformResult(
                    results.get(BenchmarkPipeline.SOURCE_TRANSFORM_SINK), options.getTotalRows());

            String baselineConfig =
                    context.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK, options, "baseline");
            assertFalse(baselineConfig.contains("observability"));
            assertFalse(baselineConfig.contains("stain_trace"));
            assertEquals(2L, resultFileCount(resultDirectory));
        } finally {
            context.tearDown();
        }
    }

    private static void executeObservabilityEnvironmentPipeline() throws Exception {
        SeaTunnelObservabilityEnvironmentContext context =
                new SeaTunnelObservabilityEnvironmentContext();
        try {
            context.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(2_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    context.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY, options),
                    options.getTotalRows());

            String observabilityConfig =
                    context.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY,
                            options,
                            "observability");
            assertTrue(observabilityConfig.contains("observability"));
            assertTrue(
                    observabilityConfig.contains("async_boundaries = [\"benchmark_transform\"]"));
            assertFalse(observabilityConfig.contains("stain_trace"));
        } finally {
            context.tearDown();
        }
    }

    private static void executeTraceEnvironmentPipeline() throws Exception {
        SeaTunnelTraceEnvironmentContext traceContext = new SeaTunnelTraceEnvironmentContext();
        try {
            traceContext.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(20_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    traceContext.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK_TRACE, options),
                    options.getTotalRows());

            assertTrue(
                    traceContext
                            .createSeaTunnelConfig("trace-test")
                            .getEngineConfig()
                            .isStainTraceEnabled());
            String traceConfig =
                    traceContext.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_TRACE, options, "trace");
            assertFalse(traceConfig.contains("observability"));
            assertTrue(traceConfig.contains("stain_trace"));
            assertTrue(traceConfig.contains("sample_interval = 10000"));
            assertTraceStages(waitForTraceJson(traceContext));
        } finally {
            traceContext.tearDown();
        }
    }

    private static void executeObservabilityTraceEnvironmentPipeline() throws Exception {
        SeaTunnelObservabilityTraceEnvironmentContext context =
                new SeaTunnelObservabilityTraceEnvironmentContext();
        try {
            context.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(20_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    context.execute(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY_TRACE, options),
                    options.getTotalRows());

            String allEnabledConfig =
                    context.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_OBSERVABILITY_TRACE,
                            options,
                            "all-enabled");
            assertTrue(allEnabledConfig.contains("observability"));
            assertTrue(allEnabledConfig.contains("stain_trace"));
            assertTraceStages(waitForTraceJson(context));
        } finally {
            context.tearDown();
        }
    }

    private static String waitForTraceJson(SeaTunnelEnvironmentContext context) throws Exception {
        Path traceDirectory = context.getMiniClusterHome().resolve("traces");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (Files.isDirectory(traceDirectory)) {
                try (java.util.stream.Stream<Path> files = Files.walk(traceDirectory)) {
                    Optional<Path> traceFile =
                            files.filter(path -> path.toString().endsWith(".jsonl"))
                                    .filter(
                                            path -> {
                                                try {
                                                    return Files.size(path) > 0L;
                                                } catch (java.io.IOException ignored) {
                                                    return false;
                                                }
                                            })
                                    .findFirst();
                    if (traceFile.isPresent()) {
                        String traceJson =
                                new String(
                                        Files.readAllBytes(traceFile.get()),
                                        StandardCharsets.UTF_8);
                        if (traceJson.contains("SINK_WRITE_DONE")) {
                            return traceJson;
                        }
                    }
                }
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("StainTrace JSONL was not created under " + traceDirectory);
    }

    private static void assertTraceStages(String traceJson) {
        assertTrue(traceJson.contains("SOURCE_EMIT"), traceJson);
        assertTrue(traceJson.contains("QUEUE_IN"), traceJson);
        assertTrue(traceJson.contains("QUEUE_OUT"), traceJson);
        assertTrue(traceJson.contains("TRANSFORM_IN"), traceJson);
        assertTrue(traceJson.contains("TRANSFORM_OUT"), traceJson);
        assertTrue(traceJson.contains("SINK_WRITE_DONE"), traceJson);
    }

    private static void assertCompleteTransformResult(
            BenchmarkRunResult result, long expectedRows) {
        assertEquals(expectedRows, result.getProcessedRows());
        assertNotEquals(0L, result.getChecksum());
    }

    private static long resultFileCount(Path resultDirectory) throws Exception {
        try (java.util.stream.Stream<Path> files = Files.list(resultDirectory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(".json")).count();
        }
    }

    private static IterationParams iterationParams(IterationType type) {
        return new IterationParams(type, 1, TimeValue.seconds(1L), 1);
    }
}
