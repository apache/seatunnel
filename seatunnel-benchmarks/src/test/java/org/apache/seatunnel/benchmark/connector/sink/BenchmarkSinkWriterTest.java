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

package org.apache.seatunnel.benchmark.connector.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.benchmark.BenchmarkRunResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkSinkWriterTest {

    @TempDir Path resultDirectory;

    @Test
    void shouldMergeParallelWritersIntoOneResult() throws Exception {
        BenchmarkSinkWriter first = writer(0);
        BenchmarkSinkWriter second = writer(1);
        long scheduledAt = System.currentTimeMillis() - 5L;

        first.write(row(0L, scheduledAt, 10L));
        first.write(row(2L, scheduledAt, 20L));
        second.write(row(1L, scheduledAt, 30L));
        second.write(row(3L, scheduledAt, 40L));

        first.close();
        assertFalse(Files.exists(resultDirectory.resolve("test-run.json")));
        second.close();

        String result =
                new String(
                        Files.readAllBytes(resultDirectory.resolve("test-run.json")),
                        StandardCharsets.UTF_8);
        assertTrue(result.contains("\"processed_rows\": 4"));
        assertTrue(result.contains("\"parallelism\": 2"));
        assertTrue(result.contains("\"payload_size\": 256"));
        assertTrue(result.contains("\"transform_operations\": 64"));
        assertTrue(result.contains("\"checksum\": 100"));
        assertTrue(result.contains("\"sustainable\": true"));

        BenchmarkRunResult parsed =
                BenchmarkRunResult.read(resultDirectory.resolve("test-run.json"));
        assertTrue(parsed.isSustainable());
        assertTrue(parsed.getThroughputRowsPerSecond() > 0D);
    }

    private BenchmarkSinkWriter writer(int subtaskIndex) {
        return new BenchmarkSinkWriter(
                new TestSinkContext(subtaskIndex),
                resultDirectory.toString(),
                "test-run",
                4L,
                1_000L,
                256,
                64,
                1_000,
                100L,
                2D);
    }

    private static SeaTunnelRow row(long sequence, long scheduledAt, long checksum) {
        return new SeaTunnelRow(new Object[] {sequence, scheduledAt, "payload", checksum});
    }

    private static final class TestSinkContext implements SinkWriter.Context {
        private static final long serialVersionUID = 1L;
        private final int subtaskIndex;

        private TestSinkContext(int subtaskIndex) {
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        public int getIndexOfSubtask() {
            return subtaskIndex;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 2;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
