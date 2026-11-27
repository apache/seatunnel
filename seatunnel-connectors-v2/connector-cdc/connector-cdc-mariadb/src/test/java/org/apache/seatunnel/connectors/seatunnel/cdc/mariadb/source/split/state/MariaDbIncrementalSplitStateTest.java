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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.split.state;

import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset.MariaDbBinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MariaDbIncrementalSplitStateTest {

    private static final String GTID_SET = "0-1-100";

    @Test
    public void testToSourceSplitPreservesGtidAndSkipStartupOffset() {
        MariaDbBinlogOffset startupOffset = gtidOffset(3L, 10L);
        IncrementalSplit split =
                new IncrementalSplit(
                        "incremental-split",
                        Collections.singletonList(new TableId("test_db", null, "test_table")),
                        startupOffset,
                        null,
                        Collections.emptyList(),
                        null,
                        Collections.emptyMap());

        IncrementalSplit restoredSplit = new IncrementalSplitState(split).toSourceSplit();

        Assertions.assertEquals(startupOffset, restoredSplit.getStartupOffset());
        Assertions.assertEquals(
                GTID_SET,
                restoredSplit.getStartupOffset().getOffset().get(MariaDbBinlogOffset.GTID_SET_KEY));
        Assertions.assertEquals(
                "3",
                restoredSplit
                        .getStartupOffset()
                        .getOffset()
                        .get(MariaDbBinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY));
        Assertions.assertEquals(
                "10",
                restoredSplit
                        .getStartupOffset()
                        .getOffset()
                        .get(MariaDbBinlogOffset.ROWS_TO_SKIP_OFFSET_KEY));
    }

    private static MariaDbBinlogOffset gtidOffset(long skipEvents, long skipRows) {
        Map<String, String> offset = new HashMap<>();
        offset.put(MariaDbBinlogOffset.GTID_SET_KEY, GTID_SET);
        offset.put(MariaDbBinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY, String.valueOf(skipEvents));
        offset.put(MariaDbBinlogOffset.ROWS_TO_SKIP_OFFSET_KEY, String.valueOf(skipRows));
        return new MariaDbBinlogOffset(offset);
    }
}
