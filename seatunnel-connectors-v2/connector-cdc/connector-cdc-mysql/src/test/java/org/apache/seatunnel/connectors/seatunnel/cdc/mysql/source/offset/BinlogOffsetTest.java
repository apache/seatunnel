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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BinlogOffsetTest {

    private static final String GTID_SET_A = "036d85a9-64e5-11e6-9b48-42010af0000c:1-100";
    private static final String GTID_SET_B = "036d85a9-64e5-11e6-9b48-42010af0000c:1-200";

    @Test
    public void testCompareToWithEqualGtidSetConsidersRestartSkipRows() {
        BinlogOffset lower = new BinlogOffset("mysql-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        BinlogOffset higher = new BinlogOffset("mysql-bin.000001", 4L, 1L, 9L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(
                lower.compareTo(higher) < 0,
                "offset with smaller restartSkipRows must be ordered before a larger one "
                        + "when the GTID set and restartSkipEvents are equal");
        Assertions.assertTrue(higher.compareTo(lower) > 0);
        Assertions.assertEquals(0, lower.compareTo(lower));
    }

    @Test
    public void testCompareToWithEqualGtidSetPrefersRestartSkipEvents() {
        BinlogOffset earlierEvent =
                new BinlogOffset("mysql-bin.000001", 4L, 1L, 9L, 0L, GTID_SET_A, 1);
        BinlogOffset laterEvent =
                new BinlogOffset("mysql-bin.000001", 4L, 2L, 0L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(
                earlierEvent.compareTo(laterEvent) < 0,
                "restartSkipEvents must take precedence over restartSkipRows");
    }

    @Test
    public void testCompareToWithEqualGtidSetAndEqualProgress() {
        BinlogOffset a = new BinlogOffset("mysql-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        BinlogOffset b = new BinlogOffset("mysql-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);

        Assertions.assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testCompareToWithGtidSubsetAndSuperset() {
        BinlogOffset subset = new BinlogOffset("mysql-bin.000001", 4L, 0L, 0L, 0L, GTID_SET_A, 1);
        BinlogOffset superset = new BinlogOffset("mysql-bin.000001", 4L, 0L, 0L, 0L, GTID_SET_B, 1);

        Assertions.assertTrue(subset.compareTo(superset) < 0);
        Assertions.assertTrue(superset.compareTo(subset) > 0);
    }

    @Test
    public void testCompareToWithoutGtidFallsBackToEventsAndRows() {
        BinlogOffset lowerRow = new BinlogOffset("mysql-bin.000001", 4L, 1L, 3L, 0L, null, 1);
        BinlogOffset higherRow = new BinlogOffset("mysql-bin.000001", 4L, 1L, 7L, 0L, null, 1);

        Assertions.assertTrue(lowerRow.compareTo(higherRow) < 0);
        Assertions.assertTrue(higherRow.compareTo(lowerRow) > 0);
    }

    @Test
    public void testNoStoppingOffsetIsAlwaysMaximum() {
        BinlogOffset regular = new BinlogOffset("mysql-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(BinlogOffset.NO_STOPPING_OFFSET.compareTo(regular) > 0);
        Assertions.assertTrue(regular.compareTo(BinlogOffset.NO_STOPPING_OFFSET) < 0);
        Assertions.assertEquals(
                0, BinlogOffset.NO_STOPPING_OFFSET.compareTo(BinlogOffset.NO_STOPPING_OFFSET));
    }
}
