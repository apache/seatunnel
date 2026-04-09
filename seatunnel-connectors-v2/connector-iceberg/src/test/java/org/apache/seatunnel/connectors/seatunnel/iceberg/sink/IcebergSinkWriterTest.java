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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.state.IcebergSinkState;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class IcebergSinkWriterTest {

    @Mock private IcebergTableLoader tableLoader;
    @Mock private IcebergSinkConfig config;

    private TableSchema minimalSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, true, null, null))
                .build();
    }

    /**
     * Restoring from state must only recover commitUser — no commit or I/O against the table should
     * happen. The actual re-commit is the sole responsibility of
     * IcebergAggregatedCommitter.restoreCommit().
     */
    @Test
    void testRestoreFromStateDoesNotCommit() {
        IcebergSinkState state = new IcebergSinkState("restored-user", 5L);

        new IcebergSinkWriter(
                tableLoader, config, minimalSchema(), Collections.singletonList(state));

        verifyNoInteractions(tableLoader);
    }

    @Test
    void testRestoreFromStateRecoversPreviousCommitUser() throws IOException {
        String originalUser = "original-commit-user";
        IcebergSinkState state = new IcebergSinkState(originalUser, 3L);

        IcebergSinkWriter writer =
                new IcebergSinkWriter(
                        tableLoader, config, minimalSchema(), Collections.singletonList(state));

        List<IcebergSinkState> snapped = writer.snapshotState(4L);
        assertEquals(originalUser, snapped.get(0).getCommitUser());
    }

    @Test
    void testNewWriterWithoutStateGeneratesRandomCommitUser() throws IOException {
        IcebergSinkWriter w1 = new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);
        IcebergSinkWriter w2 = new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        List<IcebergSinkState> s1 = w1.snapshotState(1L);
        List<IcebergSinkState> s2 = w2.snapshotState(1L);
        assertNotEquals(
                s1.get(0).getCommitUser(),
                s2.get(0).getCommitUser(),
                "Each new writer must get a unique commitUser");
    }

    @Test
    void testPrepareCommitPassesCheckpointIdToCommitInfo() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        Optional<IcebergCommitInfo> info = writer.prepareCommit(42L);
        assertEquals(42L, info.get().getCheckpointId());
    }

    @Test
    void testSnapshotStatePersistsCheckpointId() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        List<IcebergSinkState> states = writer.snapshotState(99L);
        assertEquals(1, states.size());
        assertEquals(99L, states.get(0).getCheckpointId());
    }
}
