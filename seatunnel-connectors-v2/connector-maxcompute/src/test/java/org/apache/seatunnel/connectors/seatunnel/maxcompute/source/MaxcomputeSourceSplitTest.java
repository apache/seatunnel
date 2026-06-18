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

        TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table");
        Set<String> splitIds = new HashSet<>();
        int[] indexCounts = new int[numReaders];
        int totalSplits = 0;
        int chunkIndex = 0;
        // Simulate the inner logic of MaxcomputeSourceSplitEnumerator.discoverySplits
        for (long num = 0; num < recordCount; num += splitRow) {
            int ownerReader = chunkIndex % numReaders;
            MaxcomputeSourceSplit split =
                    new MaxcomputeSourceSplit(
                            num,
                            Math.min((long) splitRow, recordCount - num),
                            tablePath,
                            ownerReader);

            // Verify splitId uniqueness
            Assertions.assertTrue(
                    splitIds.add(split.splitId()), "Duplicate splitId found: " + split.splitId());

            // Verify index assigned to the split matches the reader index
            Assertions.assertEquals(ownerReader, split.getIndex());

            indexCounts[ownerReader]++;
            totalSplits++;
            chunkIndex++;
        }

        // 105000 / 10000 = 10 full splits + 1 remainder split = 11 splits total!
        Assertions.assertEquals(11, totalSplits);

        // Verify index distribution (round-robin):
        // 11 splits / 5 readers = 2 splits each, plus 1 remainder for reader 0.
        Assertions.assertEquals(3, indexCounts[0], "Reader 0 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[1], "Reader 1 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[2], "Reader 2 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[3], "Reader 3 split count mismatch!");
        Assertions.assertEquals(2, indexCounts[4], "Reader 4 split count mismatch!");
    }

    @Test
    public void testMultiTableRoundRobinDistribution() {
        int numReaders = 3;
        int numTables = 5;
        long recordCountPerTable = 15000;
        int splitRow = 10000;

        int[] indexCounts = new int[numReaders];
        int chunkIndex = 0;
        int totalSplits = 0;

        // Simulate multiple tables with 15k rows each (2 splits per table: 10k + 5k)
        for (int t = 0; t < numTables; t++) {
            TablePath tablePath = TablePath.of("test_db", "test_schema", "test_table_" + t);
            for (long num = 0; num < recordCountPerTable; num += splitRow) {
                int ownerReader = chunkIndex % numReaders;
                MaxcomputeSourceSplit split =
                        new MaxcomputeSourceSplit(
                                num,
                                Math.min((long) splitRow, recordCountPerTable - num),
                                tablePath,
                                ownerReader);

                Assertions.assertEquals(ownerReader, split.getIndex());
                indexCounts[ownerReader]++;
                totalSplits++;
                chunkIndex++;
            }
        }

        // 5 tables * 2 splits = 10 total splits
        Assertions.assertEquals(10, totalSplits);

        // 10 splits / 3 readers = 3 splits each, plus 1 remainder for reader 0.
        Assertions.assertEquals(4, indexCounts[0], "Reader 0 split count mismatch!");
        Assertions.assertEquals(3, indexCounts[1], "Reader 1 split count mismatch!");
        Assertions.assertEquals(3, indexCounts[2], "Reader 2 split count mismatch!");
    }
}
