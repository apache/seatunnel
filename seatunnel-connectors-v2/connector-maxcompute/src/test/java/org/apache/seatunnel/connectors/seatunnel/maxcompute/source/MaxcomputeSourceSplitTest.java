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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class MaxcomputeSourceSplitTest {

    @Test
    public void testSplitIdUniquenessAndIndexDistribution() {
        int numReaders = 5;
        long recordCount = 105000;
        int splitRow = 10000;

        int splitRowNum = (int) Math.ceil((double) recordCount / numReaders); // 21000
        TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table");

        Set<String> splitIds = new HashSet<>();
        int[] indexCounts = new int[numReaders];
        int totalSplits = 0;

        // Simulate the inner logic of MaxcomputeSourceSplitEnumerator.discoverySplits
        for (int i = 0; i < numReaders; i++) {
            int readerStart = i * splitRowNum;
            int readerEnd = (int) Math.min((i + 1) * splitRowNum, recordCount);
            for (int num = readerStart; num < readerEnd; num += splitRow) {
                MaxcomputeSourceSplit split =
                        new MaxcomputeSourceSplit(
                                num, Math.min(splitRow, readerEnd - num), tablePath, i);

                // Verify splitId uniqueness
                Assertions.assertTrue(
                        splitIds.add(split.splitId()),
                        "Duplicate splitId found: " + split.splitId());

                // Verify index assigned to the split matches the reader index
                Assertions.assertEquals(i, split.getIndex());

                indexCounts[i]++;
                totalSplits++;
            }
        }

        Assertions.assertTrue(totalSplits == 15);

        // Verify index distribution:
        // Record count 105000 / 5 readers = 21000 splits per reader.
        // Split row is 10000. Loop steps: 0, 10000, 20000. So 3 splits per reader.
        for (int i = 0; i < numReaders; i++) {
            Assertions.assertEquals(3, indexCounts[i], "Reader " + i + " split count mismatch!");
        }
    }
}
