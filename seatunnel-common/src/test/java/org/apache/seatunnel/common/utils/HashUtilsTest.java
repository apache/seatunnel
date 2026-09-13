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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HashUtilsTest {

    @Test
    public void testIntegerMinValueIsMappedIntoRange() {
        // Math.abs(Integer.MIN_VALUE) overflows back to Integer.MIN_VALUE, so the naive
        // Math.abs(hash) % bucketCount spelling still returns a negative index here.
        Assertions.assertTrue(Math.abs(Integer.MIN_VALUE) % 3 < 0);

        // Clearing the sign bit leaves 0, which is in range for every bucket count.
        for (int bucketCount : new int[] {1, 2, 3, 7, 8, 100}) {
            Assertions.assertEquals(0, HashUtils.bucketIndex(Integer.MIN_VALUE, bucketCount));
        }
        Assertions.assertEquals(1, HashUtils.bucketIndex(Integer.MIN_VALUE + 1, 3));
    }

    @Test
    public void testNegativeHashesStayInRange() {
        for (int bucketCount : new int[] {1, 2, 3, 7, 8, 100}) {
            for (int hash :
                    new int[] {-1, -5, -31, -12345, Integer.MIN_VALUE, Integer.MIN_VALUE + 1}) {
                int bucket = HashUtils.bucketIndex(hash, bucketCount);
                Assertions.assertTrue(
                        bucket >= 0 && bucket < bucketCount,
                        "hash=" + hash + " bucketCount=" + bucketCount + " bucket=" + bucket);
            }
        }
    }

    @Test
    public void testMatchesExistingMaskedSpelling() {
        for (int hash = -1000; hash <= 1000; hash++) {
            for (int bucketCount : new int[] {1, 2, 3, 7, 8, 16, 100}) {
                Assertions.assertEquals(
                        (hash & Integer.MAX_VALUE) % bucketCount,
                        HashUtils.bucketIndex(hash, bucketCount),
                        "hash=" + hash + " bucketCount=" + bucketCount);
            }
        }
    }

    @Test
    public void testSingleBucketAlwaysZero() {
        Assertions.assertEquals(0, HashUtils.bucketIndex(Integer.MIN_VALUE, 1));
        Assertions.assertEquals(0, HashUtils.bucketIndex(Integer.MAX_VALUE, 1));
        Assertions.assertEquals(0, HashUtils.bucketIndex(0, 1));
    }

    @Test
    public void testNonPositiveBucketCountRejected() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> HashUtils.bucketIndex(42, 0));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> HashUtils.bucketIndex(42, -1));
    }

    @Test
    public void testNotEquivalentToFloorModForNonPowerOfTwo() {
        // Guards the invariant that made this a helper rather than a mechanical rename:
        // masking and floorMod agree only when bucketCount is a power of two.
        Assertions.assertEquals(0, HashUtils.bucketIndex(-5, 3));
        Assertions.assertEquals(1, Math.floorMod(-5, 3));

        for (int bucketCount : new int[] {2, 4, 8, 16, 1024}) {
            for (int hash = -500; hash <= 500; hash++) {
                Assertions.assertEquals(
                        Math.floorMod(hash, bucketCount),
                        HashUtils.bucketIndex(hash, bucketCount),
                        "hash=" + hash + " bucketCount=" + bucketCount);
            }
        }
    }

    @Test
    public void testLongHashesStayInRange() {
        for (int bucketCount : new int[] {1, 2, 3, 7, 8, 100}) {
            for (long hash :
                    new long[] {
                        -1L, -5L, -31L, -12345L, Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE
                    }) {
                int bucket = HashUtils.bucketIndex(hash, bucketCount);
                Assertions.assertTrue(
                        bucket >= 0 && bucket < bucketCount,
                        "hash=" + hash + " bucketCount=" + bucketCount + " bucket=" + bucket);
            }
        }
    }

    @Test
    public void testLongMatchesExistingMaskedSpelling() {
        // The clickhouse shard router already spelled this out inline; the helper must not
        // change which shard a value routes to.
        for (long hash = -1000L; hash <= 1000L; hash++) {
            for (int bucketCount : new int[] {1, 2, 3, 7, 8, 16, 100}) {
                Assertions.assertEquals(
                        (int) ((hash & Long.MAX_VALUE) % bucketCount),
                        HashUtils.bucketIndex(hash, bucketCount),
                        "hash=" + hash + " bucketCount=" + bucketCount);
            }
        }
        Assertions.assertEquals(0, HashUtils.bucketIndex(Long.MIN_VALUE, 3));
    }

    @Test
    public void testLongOverloadIsDistinctFromIntOverload() {
        // A 64-bit hash and its truncation to 32 bits generally land in different buckets,
        // so the two overloads are separate mappings and call sites must not be switched.
        long hash = 1L << 32;
        Assertions.assertEquals(1, HashUtils.bucketIndex(hash, 3));
        Assertions.assertEquals(0, HashUtils.bucketIndex((int) hash, 3));
    }

    @Test
    public void testLongNonPositiveBucketCountRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> HashUtils.bucketIndex(42L, 0));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> HashUtils.bucketIndex(42L, -1));
    }
}
