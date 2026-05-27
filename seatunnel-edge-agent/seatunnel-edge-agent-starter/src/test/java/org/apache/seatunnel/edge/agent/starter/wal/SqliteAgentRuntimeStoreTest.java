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

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.starter.wal.sqlite.SqliteWalStore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

public class SqliteAgentRuntimeStoreTest {

    @TempDir Path tempDir;

    @Test
    void walStoreSharesWalAndPositionOnSingleConnection() throws Exception {
        Path dbPath = tempDir.resolve("agent.db");
        try (SqliteWalStore store = new SqliteWalStore(dbPath)) {
            EdgeSourcePosition position =
                    EdgeSourcePosition.builder()
                            .sourceId("src-1")
                            .partition("file:/tmp/a.log")
                            .offset(42L)
                            .build();
            store.sourcePositionStore().save(position);

            long id =
                    store.append(
                            EdgeEvent.builder()
                                    .sourceId("src-1")
                                    .payload("{\"x\":1}".getBytes(StandardCharsets.UTF_8))
                                    .eventTime(1L)
                                    .build());

            Map<String, EdgeSourcePosition> loaded =
                    store.sourcePositionStore().loadBySource("src-1");
            Assertions.assertEquals(42L, loaded.get("file:/tmp/a.log").getOffset());
            Assertions.assertEquals(1L, id);
        }
    }
}
