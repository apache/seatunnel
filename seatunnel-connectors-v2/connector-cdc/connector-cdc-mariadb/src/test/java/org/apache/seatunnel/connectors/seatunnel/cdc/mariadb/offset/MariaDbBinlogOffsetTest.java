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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MariaDbBinlogOffsetTest {

    private static final String GTID_SET_A = "0-1-100";
    private static final String GTID_SET_B = "0-1-200";

    @Test
    public void testCompareToWithEqualGtidSetConsidersRestartSkipRows() {
        MariaDbBinlogOffset lower =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset higher =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 9L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(
                lower.compareTo(higher) < 0,
                "offset with smaller restartSkipRows must be ordered before a larger one "
                        + "when the GTID set and restartSkipEvents are equal");
        Assertions.assertTrue(higher.compareTo(lower) > 0);
        Assertions.assertEquals(0, lower.compareTo(lower));
    }

    @Test
    public void testCompareToWithEqualGtidSetPrefersRestartSkipEvents() {
        MariaDbBinlogOffset earlierEvent =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 9L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset laterEvent =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 2L, 0L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(
                earlierEvent.compareTo(laterEvent) < 0,
                "restartSkipEvents must take precedence over restartSkipRows");
    }

    @Test
    public void testCompareToWithEqualGtidSetAndEqualProgress() {
        MariaDbBinlogOffset a =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset b =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);

        Assertions.assertEquals(0, a.compareTo(b));
    }

    @Test
    public void testCompareToWithGtidSubsetAndSuperset() {
        MariaDbBinlogOffset subset =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 0L, 0L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset superset =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 0L, 0L, 0L, GTID_SET_B, 1);

        Assertions.assertTrue(subset.compareTo(superset) < 0);
        Assertions.assertTrue(superset.compareTo(subset) > 0);
    }

    @Test
    public void testCompareToWithoutGtidFallsBackToEventsAndRows() {
        MariaDbBinlogOffset lowerRow =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 3L, 0L, null, 1);
        MariaDbBinlogOffset higherRow =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 7L, 0L, null, 1);

        Assertions.assertTrue(lowerRow.compareTo(higherRow) < 0);
        Assertions.assertTrue(higherRow.compareTo(lowerRow) > 0);
    }

    @Test
    public void testNoStoppingOffsetIsAlwaysMaximum() {
        MariaDbBinlogOffset regular =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);

        Assertions.assertTrue(MariaDbBinlogOffset.NO_STOPPING_OFFSET.compareTo(regular) > 0);
        Assertions.assertTrue(regular.compareTo(MariaDbBinlogOffset.NO_STOPPING_OFFSET) < 0);
        Assertions.assertEquals(
                0,
                MariaDbBinlogOffset.NO_STOPPING_OFFSET.compareTo(
                        MariaDbBinlogOffset.NO_STOPPING_OFFSET));
    }

    @Test
    public void testCompareToWithThisHasGtidAndThatDoesNotFallsBackToFilename() {
        MariaDbBinlogOffset thisWithGtid =
                new MariaDbBinlogOffset("mariadb-bin.000002", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset thatWithoutGtid =
                new MariaDbBinlogOffset("mariadb-bin.000001", 999L, 1L, 5L, 0L, null, 1);

        Assertions.assertTrue(
                thisWithGtid.compareTo(thatWithoutGtid) > 0,
                "when this has a GTID but that does not, comparison must fall back to "
                        + "binlog filename ordering");
        Assertions.assertTrue(thatWithoutGtid.compareTo(thisWithGtid) < 0);
    }

    @Test
    public void testCompareToWithThisHasGtidAndThatDoesNotSameFileUsesPosition() {
        MariaDbBinlogOffset thisWithGtid =
                new MariaDbBinlogOffset("mariadb-bin.000001", 100L, 1L, 5L, 0L, GTID_SET_A, 1);
        MariaDbBinlogOffset thatWithoutGtid =
                new MariaDbBinlogOffset("mariadb-bin.000001", 50L, 1L, 5L, 0L, null, 1);

        Assertions.assertTrue(
                thisWithGtid.compareTo(thatWithoutGtid) > 0,
                "same binlog file must be ordered by position when GTIDs are mixed");
        Assertions.assertTrue(thatWithoutGtid.compareTo(thisWithGtid) < 0);
    }

    @Test
    public void testIsNeverStop() {
        Assertions.assertTrue(
                MariaDbBinlogOffset.NO_STOPPING_OFFSET.isNeverStop(),
                "the unbounded sentinel must report itself as never-stop");
        MariaDbBinlogOffset regular =
                new MariaDbBinlogOffset("mariadb-bin.000001", 4L, 1L, 5L, 0L, GTID_SET_A, 1);
        Assertions.assertFalse(regular.isNeverStop());
    }
}
