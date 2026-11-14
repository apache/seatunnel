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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset.MariaDbBinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;

public class MariaDbTimestampStartupOffsetTest {

    @Test
    public void testTimestampStartupResolvesConfiguredTimestampOnlyForBootstrapOffset() {
        MariaDbBinlogOffset timestampOffset = new MariaDbBinlogOffset(1716076800L);
        MariaDbBinlogOffset restoredOffset = new MariaDbBinlogOffset("mariadb-bin.000021", 4096L);

        Assertions.assertTrue(
                MariaDbSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, timestampOffset));
        Assertions.assertFalse(
                MariaDbSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredOffset));
    }

    @Test
    public void testCheckpointRestoreUsesPersistedBinlogOffsetAfterTimestampBootstrap() {
        MariaDbBinlogOffset timestampOffset = new MariaDbBinlogOffset(1716076800L);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        timestampOffset,
                        MariaDbBinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
        MariaDbBinlogOffset checkpointOffset = new MariaDbBinlogOffset("mariadb-bin.000021", 4096L);
        splitState.setStartupOffset(checkpointOffset);

        IncrementalSplit restoredSplit = splitState.toSourceSplit();

        Assertions.assertEquals(checkpointOffset, restoredSplit.getStartupOffset());
        Assertions.assertFalse(
                MariaDbSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }
}
