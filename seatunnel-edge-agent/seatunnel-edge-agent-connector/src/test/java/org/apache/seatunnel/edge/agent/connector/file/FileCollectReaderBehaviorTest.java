/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.connector.file;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectConfig;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class FileCollectReaderBehaviorTest {

    private static final long CLOSE_INACTIVE_MS = 500L;
    private static final long GLOB_SCAN_INTERVAL_MS = 20L;

    @TempDir Path tempDir;

    @Test
    void rediscoversFileAfterInactiveCursorClosed() throws Exception {
        Path logFile = tempDir.resolve("tail.log");
        Files.write(logFile, new byte[0]);

        Map<String, Object> map = baseConfigMap(logFile);
        map.put(FileCollectOptions.CLOSE_INACTIVE_MS.key(), CLOSE_INACTIVE_MS);
        map.put(FileCollectOptions.GLOB_SCAN_INTERVAL_MS.key(), GLOB_SCAN_INTERVAL_MS);
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), false);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            Assertions.assertTrue(reader.poll(10).isEmpty());

            Files.write(
                    logFile, "first\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            Assertions.assertEquals(1, reader.poll(10).size());

            long idleDeadlineMs = System.currentTimeMillis() + CLOSE_INACTIVE_MS + 30L;
            Awaitility.await()
                    .atMost(3, TimeUnit.SECONDS)
                    .pollInterval(GLOB_SCAN_INTERVAL_MS, TimeUnit.MILLISECONDS)
                    .until(
                            () -> {
                                reader.poll(10);
                                return System.currentTimeMillis() >= idleDeadlineMs;
                            });

            // Ensure glob scan rediscovers the file before new data is appended
            Awaitility.await()
                    .atMost(3, TimeUnit.SECONDS)
                    .pollInterval(5, TimeUnit.MILLISECONDS)
                    .until(
                            () -> {
                                reader.poll(10);
                                return System.currentTimeMillis()
                                        >= idleDeadlineMs + GLOB_SCAN_INTERVAL_MS;
                            });
            Assertions.assertTrue(reader.poll(10).isEmpty());

            Files.write(
                    logFile,
                    "second\n".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            await().atMost(3, TimeUnit.SECONDS)
                    .pollInterval(10, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<EdgeEvent> events = reader.poll(10);
                                Assertions.assertEquals(1, events.size());
                                String payload =
                                        new String(
                                                events.get(0).getPayload(), StandardCharsets.UTF_8);
                                Assertions.assertTrue(payload.contains("second"));
                                Assertions.assertFalse(payload.contains("first"));
                            });
        } finally {
            reader.close();
        }
    }

    @Test
    void doesNotEmitPartialLineUntilNewline() throws Exception {
        Path logFile = tempDir.resolve("partial.log");
        Files.write(logFile, "partial".getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = baseConfigMap(logFile);
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), true);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            Assertions.assertTrue(reader.poll(10).isEmpty());

            Files.write(
                    logFile, " line\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);

            List<EdgeEvent> events = reader.poll(10);
            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload.contains("partial line"));
        } finally {
            reader.close();
        }
    }

    @Test
    void readsFileWithConfiguredEncoding() throws Exception {
        Path logFile = tempDir.resolve("gbk.log");
        Charset gbk = Charset.forName("GBK");
        String text = "中文日志\n";
        Files.write(logFile, text.getBytes(gbk));

        Map<String, Object> map = baseConfigMap(logFile);
        map.put(FileCollectOptions.ENCODING.key(), "GBK");
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), true);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            List<EdgeEvent> events = reader.poll(10);
            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload.contains("中文日志"));
        } finally {
            reader.close();
        }
    }

    private static Map<String, Object> baseConfigMap(Path logFile) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList(logFile.toString()));
        return map;
    }
}
