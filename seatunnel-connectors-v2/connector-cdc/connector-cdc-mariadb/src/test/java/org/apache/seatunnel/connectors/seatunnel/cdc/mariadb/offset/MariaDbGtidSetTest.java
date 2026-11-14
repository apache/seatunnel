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

public class MariaDbGtidSetTest {

    @Test
    public void testParseAndToString() {
        String gtid = "0-1-100,1-2-200";
        MariaDbGtidSet gtidSet = new MariaDbGtidSet(gtid);
        Assertions.assertEquals(2, gtidSet.getGtids().size());
        Assertions.assertEquals("0-1-100,1-2-200", gtidSet.toString());
    }

    @Test
    public void testIsContainedWithin() {
        MariaDbGtidSet set1 = new MariaDbGtidSet("0-1-100,1-2-200");
        MariaDbGtidSet set2 = new MariaDbGtidSet("0-1-150,1-2-200");
        MariaDbGtidSet set3 = new MariaDbGtidSet("0-1-50,1-2-200");

        // set1 is contained within set2 (since 100 <= 150)
        Assertions.assertTrue(set1.isContainedWithin(set2));
        // set2 is NOT contained within set1 (since 150 > 100)
        Assertions.assertFalse(set2.isContainedWithin(set1));
        // set3 is contained within set1
        Assertions.assertTrue(set3.isContainedWithin(set1));
    }

    @Test
    public void testEqualsAndHashCode() {
        MariaDbGtidSet set1 = new MariaDbGtidSet("0-1-100,1-2-200");
        MariaDbGtidSet set2 = new MariaDbGtidSet("0-1-100,1-2-200");
        MariaDbGtidSet set3 = new MariaDbGtidSet("0-1-101,1-2-200");

        Assertions.assertEquals(set1, set2);
        Assertions.assertEquals(set1.hashCode(), set2.hashCode());
        Assertions.assertNotEquals(set1, set3);
    }
}
