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

package org.apache.seatunnel.edge.agent.starter.wal;

import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

public class SqliteSourcePositionStoreTest {

    @TempDir Path tempDir;

    @Test
    void saveAndLoadPositionRoundTrip() throws Exception {
        Path dbPath = tempDir.resolve("positions.db");
        try (SqliteSourcePositionStore store = new SqliteSourcePositionStore(dbPath)) {
            EdgeSourcePosition position =
                    EdgeSourcePosition.builder()
                            .sourceId("src-1")
                            .partition("file:/tmp/a.log")
                            .offset(42L)
                            .updatedAt(1_000L)
                            .build();
            store.save(position);

            EdgeSourcePosition loaded = store.load("src-1", "file:/tmp/a.log");
            Assertions.assertNotNull(loaded);
            Assertions.assertEquals(42L, loaded.getOffset());
            Assertions.assertEquals(1_000L, loaded.getUpdatedAt());

            Map<String, EdgeSourcePosition> bySource = store.loadBySource("src-1");
            Assertions.assertEquals(1, bySource.size());
            Assertions.assertEquals(42L, bySource.get("file:/tmp/a.log").getOffset());
        }
    }
}
