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

public class MariaDbGtidUtilsTest {

    @Test
    public void testFixRestoredGtidSet() {
        MariaDbGtidSet serverGtidSet = new MariaDbGtidSet("0-1-100");
        MariaDbGtidSet restoredGtidSet = new MariaDbGtidSet("0-1-100");
        Assertions.assertEquals(
                "0-1-100",
                MariaDbGtidUtils.fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new MariaDbGtidSet("0-1-100");
        restoredGtidSet = new MariaDbGtidSet("0-1-50");
        Assertions.assertEquals(
                "0-1-50",
                MariaDbGtidUtils.fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString());

        serverGtidSet = new MariaDbGtidSet("0-1-100,1-2-200");
        restoredGtidSet = new MariaDbGtidSet("0-1-50,2-3-300");
        MariaDbGtidSet fixed = MariaDbGtidUtils.fixRestoredGtidSet(serverGtidSet, restoredGtidSet);
        Assertions.assertEquals("0-1-50,1-2-200,2-3-300", fixed.toString());

        // Null and empty cases
        Assertions.assertEquals(
                serverGtidSet,
                MariaDbGtidUtils.fixRestoredGtidSet(serverGtidSet, (MariaDbGtidSet) null));
        Assertions.assertEquals(
                restoredGtidSet,
                MariaDbGtidUtils.fixRestoredGtidSet((MariaDbGtidSet) null, restoredGtidSet));
    }

    @Test
    public void testMergeGtidSets() {
        MariaDbGtidSet base = new MariaDbGtidSet("0-1-100");
        MariaDbGtidSet toMerge = new MariaDbGtidSet("0-1-10");
        Assertions.assertEquals(
                "0-1-100", MariaDbGtidUtils.mergeGtidSetInto(base, toMerge).toString());

        base = new MariaDbGtidSet("0-1-100");
        toMerge = new MariaDbGtidSet("1-1-10");
        Assertions.assertEquals(
                "0-1-100,1-1-10", MariaDbGtidUtils.mergeGtidSetInto(base, toMerge).toString());

        base = new MariaDbGtidSet("0-1-100,2-1-100");
        toMerge = new MariaDbGtidSet("0-1-10,1-1-10");
        Assertions.assertEquals(
                "0-1-100,1-1-10,2-1-100",
                MariaDbGtidUtils.mergeGtidSetInto(base, toMerge).toString());

        // Base with smaller sequence gets upgraded by toMerge
        base = new MariaDbGtidSet("0-1-50");
        toMerge = new MariaDbGtidSet("0-1-150");
        Assertions.assertEquals(
                "0-1-150", MariaDbGtidUtils.mergeGtidSetInto(base, toMerge).toString());

        // Null and empty cases
        Assertions.assertEquals(
                base, MariaDbGtidUtils.mergeGtidSetInto(base, (MariaDbGtidSet) null));
        Assertions.assertEquals(
                toMerge, MariaDbGtidUtils.mergeGtidSetInto((MariaDbGtidSet) null, toMerge));
    }
}
