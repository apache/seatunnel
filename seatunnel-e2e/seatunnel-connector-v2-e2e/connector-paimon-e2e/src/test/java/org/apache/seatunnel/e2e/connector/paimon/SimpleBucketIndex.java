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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.paimon.utils.Int2ShortHashMap;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

public class SimpleBucketIndex {
    @Getter private final Int2ShortHashMap hash2Bucket;
    @Getter private final Map<Integer, Long> bucketInformation;
    private int currentBucket;
    private int numAssigners;
    private int assignId;
    private int targetBucketRowNumber;

    public SimpleBucketIndex(int numAssigners, int assignId, int targetBucketRowNumber) {
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.targetBucketRowNumber = targetBucketRowNumber;
        this.hash2Bucket = new Int2ShortHashMap();
        this.bucketInformation = new HashMap();
        this.loadNewBucket();
    }

    public int assign(int hash) {
        if (this.hash2Bucket.containsKey(hash)) {
            return this.hash2Bucket.get(hash);
        } else {
            Long num =
                    (Long)
                            this.bucketInformation.computeIfAbsent(
                                    this.currentBucket,
                                    (i) -> {
                                        return 0L;
                                    });
            if (num >= this.targetBucketRowNumber) {
                this.loadNewBucket();
            }

            this.bucketInformation.compute(
                    this.currentBucket,
                    (i, l) -> {
                        return l == null ? 1L : l + 1L;
                    });
            this.hash2Bucket.put(hash, (short) this.currentBucket);
            return this.currentBucket;
        }
    }

    private void loadNewBucket() {
        for (int i = 0; i < 32767; ++i) {
            if (i % this.numAssigners == this.assignId && !this.bucketInformation.containsKey(i)) {
                this.currentBucket = i;
                return;
            }
        }

        throw new RuntimeException(
                "Can't find a suitable bucket to assign, all the bucket are assigned?");
    }
}
