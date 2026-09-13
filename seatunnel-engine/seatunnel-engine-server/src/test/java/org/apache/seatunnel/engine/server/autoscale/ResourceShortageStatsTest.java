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

package org.apache.seatunnel.engine.server.autoscale;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ResourceShortageStatsTest {

    @Test
    void recordsCumulativeWaitAndRejectShortages() {
        ResourceShortageStats stats = new ResourceShortageStats();

        stats.recordWaitShortage(3, "cpu=1");
        stats.recordRejectShortage(1, "slot");

        ResourceShortageSnapshot snapshot = stats.snapshot();
        Assertions.assertEquals(2L, snapshot.getSequence());
        Assertions.assertEquals(1L, snapshot.getWaitCount());
        Assertions.assertEquals(1L, snapshot.getRejectCount());
        Assertions.assertTrue(snapshot.isLatestReject());
        Assertions.assertEquals(1, snapshot.getLatestTaskGroupCount());
        Assertions.assertEquals("slot", snapshot.getLatestRequestedResourceProfile());
    }

    @Test
    void computesDeltaFromPreviousSequence() {
        ResourceShortageStats stats = new ResourceShortageStats();
        stats.recordWaitShortage(1, "slot");
        ResourceShortageSnapshot first = stats.snapshotSince(0L);

        stats.recordWaitShortage(1, "slot");
        stats.recordRejectShortage(1, "slot");
        ResourceShortageSnapshot second = stats.snapshotSince(first.getSequence());

        Assertions.assertEquals(1L, first.getShortageCount());
        Assertions.assertEquals(2L, second.getShortageCount());
        Assertions.assertTrue(second.isLatestWait());
        Assertions.assertTrue(second.isLatestReject());
    }

    @Test
    void deltaOnlyReportsStrategiesSeenSincePreviousSequence() {
        ResourceShortageStats stats = new ResourceShortageStats();
        stats.recordWaitShortage(1, "slot");
        long previousSequence = stats.snapshot().getSequence();

        stats.recordRejectShortage(1, "slot");
        ResourceShortageSnapshot snapshot = stats.snapshotSince(previousSequence);

        Assertions.assertFalse(snapshot.isLatestWait());
        Assertions.assertTrue(snapshot.isLatestReject());
    }
}
