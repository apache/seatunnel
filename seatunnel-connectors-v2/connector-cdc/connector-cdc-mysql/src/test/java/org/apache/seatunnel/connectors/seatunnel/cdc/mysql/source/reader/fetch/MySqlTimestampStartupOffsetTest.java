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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch;

import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;

public class MySqlTimestampStartupOffsetTest {

    @Test
    public void testTimestampStartupResolvesConfiguredTimestampOnlyForBootstrapOffset() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        BinlogOffset restoredOffset = new BinlogOffset("mysql-bin.000021", 4096L);

        Assertions.assertTrue(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, timestampOffset));
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredOffset));
    }

    @Test
    public void testCheckpointRestoreUsesPersistedBinlogOffsetAfterTimestampBootstrap() {
        BinlogOffset timestampOffset = new BinlogOffset(1716076800L);
        IncrementalSplit incrementalSplit =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(TableId.parse("test.orders")),
                        timestampOffset,
                        BinlogOffset.NO_STOPPING_OFFSET,
                        Collections.emptyList());

        IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
        BinlogOffset checkpointOffset = new BinlogOffset("mysql-bin.000021", 4096L);
        splitState.setStartupOffset(checkpointOffset);

        IncrementalSplit restoredSplit = splitState.toSourceSplit();

        Assertions.assertEquals(checkpointOffset, restoredSplit.getStartupOffset());
        Assertions.assertFalse(
                MySqlSourceFetchTaskContext.shouldResolveTimestampStartupOffset(
                        StartupMode.TIMESTAMP, restoredSplit.getStartupOffset()));
    }
}
