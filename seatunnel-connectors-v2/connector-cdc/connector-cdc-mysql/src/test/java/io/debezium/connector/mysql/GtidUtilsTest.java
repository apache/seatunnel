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

package io.debezium.connector.mysql;

import org.junit.jupiter.api.Test;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;
import static io.debezium.connector.mysql.GtidUtils.untrackedGtids;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {
    @Test
    void testFixingRestoredGtidSet() {
        GtidSet serverGtidSet = new GtidSet("A:1-100");
        GtidSet restoredGtidSet = new GtidSet("A:30-100");
        assertEquals("A:1-100", fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new GtidSet("A:1-100");
        restoredGtidSet = new GtidSet("A:30-50");
        assertEquals("A:1-50", fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150");
        assertEquals(
                "A:1-100:102-150,B:20-200",
                fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150,C:1-100");
        assertEquals(
                "A:1-100:102-150,B:20-200,C:1-100",
                fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150:152-200,C:1-100");
        assertEquals(
                "A:1-100:102-200,B:20-200,C:1-100",
                fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());
    }

    @Test
    void testMergingGtidSets() {
        GtidSet base = new GtidSet("A:1-100");
        GtidSet toMerge = new GtidSet("A:1-10");
        assertEquals("A:1-100", mergeGtidSetInto(base, toMerge).toString());

        base = new GtidSet("A:1-100");
        toMerge = new GtidSet("B:1-10");
        assertEquals("A:1-100,B:1-10", mergeGtidSetInto(base, toMerge).toString());

        base = new GtidSet("A:1-100,C:1-100");
        toMerge = new GtidSet("A:1-10,B:1-10");
        assertEquals("A:1-100,B:1-10,C:1-100", mergeGtidSetInto(base, toMerge).toString());
    }

    @Test
    void testUntrackedGtids() {
        GtidSet restored = new GtidSet("A:9-90");

        // A lineage the restored set does not mention is returned, one it does is dropped.
        assertEquals("B:1-1000", untrackedGtids("A:1-80,B:1-1000", restored).toString());
        assertEquals("", untrackedGtids("A:1-80", restored).toString());

        // The oldest retained binlog file reports no earlier transactions.
        assertEquals("", untrackedGtids("", restored).toString());
        assertEquals("", untrackedGtids("   ", restored).toString());
        assertEquals("", untrackedGtids(null, restored).toString());
    }

    @Test
    void testCompletingLineageMissingFromRestoredSet() {
        // B is inherited from a previous master and no longer written to, so a job started from a
        // binlog file and position never recorded it. It was executed before the resume file.
        GtidSet server = new GtidSet("A:1-100,B:1-1000");
        GtidSet purged = new GtidSet("B:1-900");
        GtidSet restored = new GtidSet("A:9-90");
        String previousGtids = "A:1-80,B:1-1000";

        // Without the Previous_gtids adjustment B is claimed only as far as the purge point, so
        // the 100 transactions the server still retains for it are re-delivered.
        assertEquals("A:1-90,B:1-900", merge(server, purged, restored, null).toString());

        assertEquals("A:1-90,B:1-1000", merge(server, purged, restored, previousGtids).toString());
    }

    @Test
    void testRestoredSetTrackingEveryLineageIsUnchanged() {
        GtidSet server = new GtidSet("A:1-100,B:1-1000");
        GtidSet purged = new GtidSet("B:1-900");
        GtidSet restored = new GtidSet("A:9-90,B:1-1000");

        assertEquals(
                merge(server, purged, restored, null).toString(),
                merge(server, purged, restored, "A:1-80,B:1-1000").toString());
    }

    @Test
    void testLineageWrittenAfterTheResumePositionIsNotClaimed() {
        // C has just become active, as after a failover, so it is absent from Previous_gtids and
        // has to keep being read from its earliest available position.
        GtidSet server = new GtidSet("A:1-100,C:1-50");
        GtidSet restored = new GtidSet("A:9-90");

        GtidSet merged = merge(server, new GtidSet(""), restored, "A:1-80");

        assertEquals("A:1-90", merged.toString());
    }

    /** The merge performed by MySqlStreamingChangeEventSource#filterGtidSet. */
    private static GtidSet merge(
            GtidSet server, GtidSet purged, GtidSet restored, String previousGtids) {
        GtidSet tracked = server.retainAll(uuid -> restored.forServerWithId(uuid) != null);
        return fixRestoredGtidSet(
                mergeGtidSetInto(
                        mergeGtidSetInto(tracked, untrackedGtids(previousGtids, restored)), purged),
                restored);
    }
}
