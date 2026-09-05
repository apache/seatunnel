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
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointingTimeBenchmarkTest {

    @Test
    void shouldUseCheckpointRecordSizesAndSeconds() {
        assertEquals(1L, SeaTunnelCheckpointEnvironmentContext.DEBLOATING_RECORD_SIZE.getBytes());
        assertEquals(
                1_024L, SeaTunnelCheckpointEnvironmentContext.UNALIGNED_RECORD_SIZE.getBytes());
        assertEquals(
                TimeUnit.SECONDS,
                CheckpointingTimeBenchmark.class.getAnnotation(OutputTimeUnit.class).value());
        assertEquals(
                Mode.AverageTime,
                CheckpointingTimeBenchmark.class.getAnnotation(BenchmarkMode.class).value()[0]);
        assertEquals(1, CheckpointingTimeBenchmark.class.getAnnotation(Threads.class).value());
    }

    @Test
    void shouldRejectInvalidMemorySize() {
        assertThrows(
                IllegalArgumentException.class,
                () -> SeaTunnelCheckpointEnvironmentContext.MemorySize.parse("1tb"));
    }

    @Test
    void shouldRenderCheckpointJobConfig() {
        CheckpointingTimeBenchmarkPipeline pipeline = new CheckpointingTimeBenchmarkPipeline();
        pipeline.recordSize = "1kb";
        Path resultPath = Paths.get("target", "checkpoint-results").toAbsolutePath();

        String jobConfig = pipeline.createCheckpointJobConfig(resultPath);

        assertTrue(jobConfig.contains("payload_size = 1024"));
        assertTrue(jobConfig.contains("rate_per_second = 10000"));
        assertTrue(jobConfig.contains("parallelism = 4"));
        assertTrue(jobConfig.contains("result_path = \"" + resultPath + "\""));
        assertFalse(jobConfig.contains("{{"));
    }
}
