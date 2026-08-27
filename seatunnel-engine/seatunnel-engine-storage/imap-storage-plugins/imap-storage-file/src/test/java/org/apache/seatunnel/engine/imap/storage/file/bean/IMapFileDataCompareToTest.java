/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.bean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class IMapFileDataCompareToTest {

    @Test
    public void testCompareToOrdersNewerTimestampFirst() {
        IMapFileData older = data("key", "value", false, 1000L);
        IMapFileData newer = data("key", "value", false, 1001L);

        Assertions.assertTrue(newer.compareTo(older) < 0);
        Assertions.assertTrue(older.compareTo(newer) > 0);
    }

    @Test
    public void testCompareToOrdersDeleteBeforePutWithSameTimestamp() {
        IMapFileData put = data("key", "value", false, 1000L);
        IMapFileData delete = data("key", null, true, 1000L);

        Assertions.assertTrue(delete.compareTo(put) < 0);
        Assertions.assertTrue(put.compareTo(delete) > 0);
    }

    @Test
    public void testCompareToGroupsRecordsByKeyBeforeTimestamp() {
        IMapFileData olderA = data("key-a", "older", false, 1000L);
        IMapFileData newerA = data("key-a", "newer", false, 2000L);
        IMapFileData keyB = data("key-b", "value", false, 3000L);

        List<IMapFileData> records = new ArrayList<>();
        records.add(keyB);
        records.add(olderA);
        records.add(newerA);
        Collections.sort(records);

        Assertions.assertEquals(newerA, records.get(0));
        Assertions.assertEquals(olderA, records.get(1));
        Assertions.assertEquals(keyB, records.get(2));
    }

    @Test
    public void testCompareToIsAntisymmetricWithEqualTimestamps() {
        IMapFileData left = data("key-a", "value-a", false, 1000L);
        IMapFileData right = data("key-b", "value-b", true, 1000L);

        int leftToRight = Integer.signum(left.compareTo(right));
        int rightToLeft = Integer.signum(right.compareTo(left));

        Assertions.assertEquals(-rightToLeft, leftToRight);
    }

    @Test
    public void testCompareToReturnsZeroForSameData() {
        IMapFileData left = data("key", "value", false, 1000L);
        IMapFileData right = data("key", "value", false, 1000L);

        Assertions.assertEquals(0, left.compareTo(right));
        Assertions.assertEquals(0, right.compareTo(left));
    }

    @Test
    public void testCollectionsSortHandlesManyEqualTimestamps() {
        List<IMapFileData> data = new ArrayList<>();
        Random random = new Random(42);
        for (int i = 0; i < 4000; i++) {
            data.add(
                    data(
                            "key-" + i,
                            "value-" + i,
                            random.nextBoolean(),
                            1000L + random.nextInt(50)));
        }

        Assertions.assertDoesNotThrow(() -> Collections.sort(data));
    }

    private static IMapFileData data(String key, String value, boolean deleted, long timestamp) {
        return IMapFileData.builder()
                .deleted(deleted)
                .key(bytes(key))
                .keyClassName(String.class.getName())
                .value(bytes(value))
                .valueClassName(value == null ? null : String.class.getName())
                .timestamp(timestamp)
                .build();
    }

    private static byte[] bytes(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }
}
