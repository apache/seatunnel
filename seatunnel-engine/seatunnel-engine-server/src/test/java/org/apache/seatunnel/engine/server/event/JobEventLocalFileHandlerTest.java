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

package org.apache.seatunnel.engine.server.event;

import org.apache.seatunnel.api.event.StainTraceEvent;
import org.apache.seatunnel.engine.server.trace.StainTracePayload;
import org.apache.seatunnel.engine.server.trace.StainTraceStage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.ringbuffer.Ringbuffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Verifies local stain trace file writing, rotation, and shutdown flush behavior. */
class JobEventLocalFileHandlerTest {

    @TempDir private Path tempDir;

    private HazelcastInstance hazelcast;
    private Ringbuffer ringbuffer;

    @BeforeEach
    void setUp() {
        String ringBufferName = "stain-trace-local-file-test-" + UUID.randomUUID();
        Config config = new Config();
        config.setRingbufferConfigs(
                java.util.Collections.singletonMap(
                        ringBufferName,
                        new RingbufferConfig(ringBufferName)
                                .setCapacity(1024)
                                .setBackupCount(0)
                                .setAsyncBackupCount(1)
                                .setTimeToLiveSeconds(0)
                                .setRingbufferStoreConfig(
                                        new RingbufferStoreConfig().setEnabled(false))));
        hazelcast = Hazelcast.newHazelcastInstance(config);
        ringbuffer = hazelcast.getRingbuffer(ringBufferName);
    }

    @AfterEach
    void tearDown() {
        if (hazelcast != null) {
            hazelcast.shutdown();
        }
    }

    @Test
    void testCloseFlushesPendingTraceWhenWriterNotInitialized() throws Exception {
        JobEventLocalFileHandler handler =
                new JobEventLocalFileHandler(
                        tempDir.toString(),
                        Duration.ofHours(1),
                        ringbuffer,
                        10000,
                        10 * 1024 * 1024L);

        String jobId = "job-close-flush";
        handler.handle(newTraceEvent(jobId, 1001L));

        handler.close();

        List<Path> jsonlFiles = findJsonlFiles(tempDir);
        Assertions.assertEquals(1, jsonlFiles.size());

        List<String> lines = Files.readAllLines(jsonlFiles.get(0), StandardCharsets.UTF_8);
        Assertions.assertEquals(1, lines.size());
        Assertions.assertTrue(lines.get(0).contains("\"seatunnel.job_id\""));
        Assertions.assertTrue(lines.get(0).contains("\"stringValue\":\"" + jobId + "\""));
    }

    private static StainTraceEvent newTraceEvent(String jobId, long traceId) {
        long now = System.currentTimeMillis();
        byte[] payload = StainTracePayload.init(traceId, now);
        payload =
                StainTracePayload.append(payload, StainTraceStage.SOURCE_EMIT, 1L, now, 32)
                        .getPayload();
        payload =
                StainTracePayload.append(payload, StainTraceStage.SINK_WRITE_DONE, 2L, now + 1, 32)
                        .getPayload();

        StainTraceEvent event = new StainTraceEvent(traceId, payload, 77L, "tbl");
        event.setJobId(jobId);
        event.setCreatedTime(now);
        return event;
    }

    private static List<Path> findJsonlFiles(Path base) throws IOException {
        try (Stream<Path> stream = Files.walk(base)) {
            return stream.filter(p -> p.toString().endsWith(".jsonl")).collect(Collectors.toList());
        }
    }
}
