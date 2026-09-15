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

package org.apache.seatunnel.lineage;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link LineageRuntime#emit} is the single seam every engine call site relies on to never throw.
 * These assertions cover the case a backend-resolution failure exercises, since a bad {@code
 * openlineage_transport} value or an incomplete deployment must degrade to a logged warning rather
 * than propagate into the job that lineage reporting is only supposed to observe.
 */
class LineageRuntimeTest {

    @Test
    void doesNothingWhenDisabled() {
        LineageConfig config =
                LineageConfig.resolve(
                        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());

        assertDoesNotThrow(() -> LineageRuntime.emit(config, event()));
    }

    @Test
    void logsAWarningInsteadOfThrowingWhenTheTransportCannotBeResolved() {
        Map<String, Object> job = new HashMap<>();
        job.put(LineageConfig.ENABLED, true);
        job.put(LineageConfig.TRANSPORT, "no-such-transport");
        LineageConfig config =
                LineageConfig.resolve(job, Collections.emptyMap(), Collections.emptyMap());

        Logger logger = Logger.getLogger(LineageRuntime.class.getName());
        RecordingHandler handler = new RecordingHandler();
        logger.addHandler(handler);
        try {
            assertDoesNotThrow(() -> LineageRuntime.emit(config, event()));
        } finally {
            logger.removeHandler(handler);
        }

        assertEquals(1, handler.records.size());
        assertEquals(Level.WARNING, handler.records.get(0).getLevel());
        assertTrue(handler.records.get(0).getThrown() instanceof IllegalArgumentException);
    }

    private static LineageEvent event() {
        return LineageEvent.builder()
                .runId(UUID.nameUUIDFromBytes("lineage-runtime-test".getBytes()))
                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                .eventType(LineageEventType.START)
                .jobNamespace("seatunnel")
                .jobName("job")
                .producer("https://seatunnel.apache.org/")
                .build();
    }

    private static final class RecordingHandler extends Handler {
        private final List<LogRecord> records = new ArrayList<>();

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
    }
}
