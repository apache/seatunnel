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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;

class SnapshotOnlySplitAssignerTest {

    @Test
    void testRestoreMarksRemainingTablesCheckpointedWithoutDiscovery() {
        TableId checkpointTable = TableId.parse("db.checkpoint_table");
        SnapshotPhaseState legacyCheckpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.singletonList(checkpointTable),
                        false,
                        false);
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        SplitAssigner.Context<SourceConfig> context =
                new SplitAssigner.Context<>(
                        null,
                        Collections.singleton(checkpointTable),
                        new HashMap<>(),
                        new HashMap<>());

        SnapshotOnlySplitAssigner<SourceConfig> assigner =
                new SnapshotOnlySplitAssigner<>(context, 2, legacyCheckpointState, dialect);
        assigner.open();

        Mockito.verify(dialect, Mockito.never()).discoverDataCollections(Mockito.any());
        SnapshotPhaseState restoredState = (SnapshotPhaseState) assigner.snapshotState(1L);
        Assertions.assertTrue(restoredState.isRemainingTablesCheckpointed());
        Assertions.assertEquals(
                Collections.singletonList(checkpointTable), restoredState.getRemainingTables());
    }

    @Test
    void testRejectsRestoredIncrementalSplit() {
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        true,
                        Collections.emptyList(),
                        false,
                        true);
        SplitAssigner.Context<SourceConfig> context =
                new SplitAssigner.Context<>(
                        null, Collections.emptySet(), new HashMap<>(), new HashMap<>());
        SnapshotOnlySplitAssigner<SourceConfig> assigner =
                new SnapshotOnlySplitAssigner<>(context, 1, checkpointState, null);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "db.table.stream",
                        Collections.singletonList(TableId.parse("db.table")),
                        null,
                        null,
                        Collections.emptyList());

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> assigner.addSplits(Collections.singletonList(incrementalSplit)));
    }
}
