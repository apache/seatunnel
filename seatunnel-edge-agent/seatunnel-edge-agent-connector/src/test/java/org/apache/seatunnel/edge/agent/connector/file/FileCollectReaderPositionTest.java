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

package org.apache.seatunnel.edge.agent.connector.file;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectConfig;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileCollectReaderPositionTest {

    @TempDir Path tempDir;

    @Test
    void openRestoresOffsetFromStore() throws Exception {
        Path logFile = tempDir.resolve("a.log");
        byte[] content = "line1\nline2\n".getBytes(StandardCharsets.UTF_8);
        Files.write(logFile, content);

        String absolutePath = logFile.toAbsolutePath().toString();
        long offsetAfterLine1 = "line1\n".getBytes(StandardCharsets.UTF_8).length;

        Map<String, EdgeSourcePosition> positions = new HashMap<>();
        positions.put(
                absolutePath,
                EdgeSourcePosition.builder()
                        .sourceId("input-1")
                        .partition(absolutePath)
                        .offset(offsetAfterLine1)
                        .build());

        FileCollectReader reader =
                new FileCollectReader(configFor(logFile), new MapSourcePositionStore(positions));
        reader.open();
        try {
            List<EdgeEvent> events = reader.poll(10);
            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload.contains("line2"));
            Assertions.assertFalse(payload.contains("line1"));
        } finally {
            reader.close();
        }
    }

    @Test
    void openRestoresLineNumberFromStoredMetadata() throws Exception {
        Path logFile = tempDir.resolve("a.log");
        Files.write(logFile, "line1\nline2\n".getBytes(StandardCharsets.UTF_8));

        String absolutePath = logFile.toAbsolutePath().toString();
        long offsetAfterLine1 = "line1\n".getBytes(StandardCharsets.UTF_8).length;

        Map<String, String> metadata = new HashMap<>();
        metadata.put("line", "1");

        Map<String, EdgeSourcePosition> positions = new HashMap<>();
        positions.put(
                absolutePath,
                EdgeSourcePosition.builder()
                        .sourceId("input-1")
                        .partition(absolutePath)
                        .offset(offsetAfterLine1)
                        .metadata(metadata)
                        .build());

        FileCollectReader reader =
                new FileCollectReader(configFor(logFile), new MapSourcePositionStore(positions));
        reader.open();
        try {
            List<EdgeEvent> events = reader.poll(10);
            Assertions.assertEquals(1, events.size());
            Assertions.assertEquals("2", events.get(0).getMetadata().get("line"));
        } finally {
            reader.close();
        }
    }

    @Test
    void openWithoutStoreStartsAtEndWhenNotReadFromBeginning() throws Exception {
        Path logFile = tempDir.resolve("a.log");
        Files.write(logFile, "line1\n".getBytes(StandardCharsets.UTF_8));

        FileCollectConfig config = configFor(logFile);
        Assertions.assertFalse(config.isReadFromBeginning());

        FileCollectReader reader =
                new FileCollectReader(config, new MapSourcePositionStore(Collections.emptyMap()));
        reader.open();
        try {
            Assertions.assertTrue(reader.poll(10).isEmpty());
        } finally {
            reader.close();
        }
    }

    @Test
    void openWithEmptyStoreReadsFromBeginningWhenEnabled() throws Exception {
        Path logFile = tempDir.resolve("a.log");
        Files.write(logFile, "line1\nline2\n".getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = baseConfigMap(logFile);
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), true);
        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new MapSourcePositionStore(Collections.emptyMap()));
        reader.open();
        try {
            Assertions.assertEquals(2, reader.poll(10).size());
        } finally {
            reader.close();
        }
    }

    private static FileCollectConfig configFor(Path logFile) {
        return FileCollectConfig.from(ReadonlyConfig.fromMap(baseConfigMap(logFile)));
    }

    private static Map<String, Object> baseConfigMap(Path logFile) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-1");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList(logFile.toString()));
        map.put(FileCollectOptions.GLOB_SCAN_INTERVAL_MS.key(), Long.MAX_VALUE);
        return map;
    }

    private static final class MapSourcePositionStore implements EdgeSourcePositionStore {

        private final Map<String, EdgeSourcePosition> byPartition;

        MapSourcePositionStore(Map<String, EdgeSourcePosition> byPartition) {
            this.byPartition = byPartition;
        }

        @Override
        public EdgeSourcePosition load(String sourceId, String partition) {
            return byPartition.get(partition);
        }

        @Override
        public Map<String, EdgeSourcePosition> loadBySource(String sourceId) {
            return byPartition;
        }

        @Override
        public void save(EdgeSourcePosition position) {}
    }
}
