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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Map;

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
            executeBackpressureEnvironmentPipeline();
            executeTraceEnvironmentPipeline();
            executeBackpressureTraceEnvironmentPipeline();
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
            assertCompleteTransformResult(results.get(BenchmarkPipeline.SOURCE_TRANSFORM_SINK));

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

    private static void executeBackpressureEnvironmentPipeline() throws Exception {
        SeaTunnelBackpressureEnvironmentContext context =
                new SeaTunnelBackpressureEnvironmentContext();
        try {
            context.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(2_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    context.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK_BACKPRESSURE, options));

            String backpressureConfig =
                    context.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_BACKPRESSURE,
                            options,
                            "backpressure");
            assertTrue(backpressureConfig.contains("observability"));
            assertTrue(backpressureConfig.contains("async_boundaries = [\"benchmark_transform\"]"));
            assertFalse(backpressureConfig.contains("stain_trace"));
        } finally {
            context.tearDown();
        }
    }

    private static void executeTraceEnvironmentPipeline() throws Exception {
        SeaTunnelTraceEnvironmentContext traceContext = new SeaTunnelTraceEnvironmentContext();
        try {
            traceContext.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(2_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    traceContext.execute(BenchmarkPipeline.SOURCE_TRANSFORM_SINK_TRACE, options));

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
            assertTrue(traceConfig.contains("sample_interval = 100000"));
        } finally {
            traceContext.tearDown();
        }
    }

    private static void executeBackpressureTraceEnvironmentPipeline() throws Exception {
        SeaTunnelBackpressureTraceEnvironmentContext context =
                new SeaTunnelBackpressureTraceEnvironmentContext();
        try {
            context.setUp();
            PipelineBenchmarkOptions options = new PipelineBenchmarkOptions(2_000L, 0L, 1, 32, 4);

            assertCompleteTransformResult(
                    context.execute(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_BACKPRESSURE_TRACE, options));

            String allEnabledConfig =
                    context.createJobConfig(
                            BenchmarkPipeline.SOURCE_TRANSFORM_SINK_BACKPRESSURE_TRACE,
                            options,
                            "all-enabled");
            assertTrue(allEnabledConfig.contains("observability"));
            assertTrue(allEnabledConfig.contains("stain_trace"));
        } finally {
            context.tearDown();
        }
    }

    private static void assertCompleteTransformResult(BenchmarkRunResult result) {
        assertEquals(2_000L, result.getProcessedRows());
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
