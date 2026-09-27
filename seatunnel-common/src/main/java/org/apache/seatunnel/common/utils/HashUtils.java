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

/** Utilities for mapping hash codes onto a fixed number of buckets. */
public final class HashUtils {

    private HashUtils() {}

    /**
     * Maps an arbitrary hash code onto {@code [0, bucketCount)}.
     *
     * <p>Plain {@code hash % bucketCount} is not usable for bucket routing: {@code %} keeps the
     * sign of its left operand, so a negative hash yields a negative index for every hash that is
     * not an exact multiple of {@code bucketCount}. {@code Math.abs(hash) % bucketCount} is not a
     * fix either, because {@code Math.abs(Integer.MIN_VALUE)} overflows back to {@code
     * Integer.MIN_VALUE} and still leaves the result negative. Clearing the sign bit has neither
     * problem.
     *
     * <p>Note that this is deliberately <em>not</em> equivalent to {@code Math.floorMod(hash,
     * bucketCount)}. Masking clears the sign bit before the division, which adds {@code 2^31} to a
     * negative hash, so the two agree only when {@code bucketCount} is a power of two: for {@code
     * hash = -5} and {@code bucketCount = 3} masking yields {@code 0} while {@code floorMod} yields
     * {@code 1}. Call sites that route splits or partitions by hash must not be switched between
     * the two spellings, as that would silently reassign ownership across a version upgrade.
     *
     * @param hash any hash code, including negative values and {@link Integer#MIN_VALUE}
     * @param bucketCount the number of buckets, must be greater than zero
     * @return a bucket index in {@code [0, bucketCount)}
     * @throws IllegalArgumentException if {@code bucketCount} is not greater than zero
     */
    public static int bucketIndex(int hash, int bucketCount) {
        checkBucketCount(bucketCount);
        return (hash & Integer.MAX_VALUE) % bucketCount;
    }

    /**
     * Maps an arbitrary 64-bit hash onto {@code [0, bucketCount)}.
     *
     * <p>Behaves as {@link #bucketIndex(int, int)} does, clearing the sign bit before the division
     * so that {@link Long#MIN_VALUE} is handled without overflow. The result always fits in an
     * {@code int} because it is smaller than {@code bucketCount}.
     *
     * <p>This is a distinct mapping from the {@code int} overload rather than a widening of it: a
     * 64-bit hash and its truncation to 32 bits generally land in different buckets, so a call site
     * must not be switched between the two overloads.
     *
     * @param hash any 64-bit hash, including negative values and {@link Long#MIN_VALUE}
     * @param bucketCount the number of buckets, must be greater than zero
     * @return a bucket index in {@code [0, bucketCount)}
     * @throws IllegalArgumentException if {@code bucketCount} is not greater than zero
     */
    public static int bucketIndex(long hash, int bucketCount) {
        checkBucketCount(bucketCount);
        return (int) ((hash & Long.MAX_VALUE) % bucketCount);
    }

    private static void checkBucketCount(int bucketCount) {
        if (bucketCount <= 0) {
            throw new IllegalArgumentException(
                    "bucketCount must be greater than zero, but was " + bucketCount);
        }
    }
}
