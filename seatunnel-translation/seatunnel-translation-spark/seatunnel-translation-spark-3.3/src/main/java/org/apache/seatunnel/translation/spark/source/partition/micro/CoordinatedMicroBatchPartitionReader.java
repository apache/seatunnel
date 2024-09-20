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

package org.apache.seatunnel.translation.spark.source.partition.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CoordinatedMicroBatchPartitionReader extends ParallelMicroBatchPartitionReader {
    protected final Map<Integer, InternalRowCollector> collectorMap;

    public CoordinatedMicroBatchPartitionReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        super(
                source,
                parallelism,
                jobId,
                subtaskId,
                checkpointId,
                checkpointInterval,
                checkpointPath,
                hdfsRoot,
                hdfsUser,
                envOptions,
                multiTableManager);
        this.collectorMap = new HashMap<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            collectorMap.put(
                    i,
                    multiTableManager.getInternalRowCollector(handover, new Object(), envOptions));
        }
    }

    @Override
    public void virtualCheckpoint() {
        try {
            int checkpointRetries = Math.max(1, CHECKPOINT_RETRIES);
            do {
                checkpointRetries--;
                long collectedReader =
                        collectorMap.values().stream()
                                .mapToLong(e -> e.collectTotalCount() > 0 ? 1 : 0)
                                .sum();
                if (collectedReader == 0) {
                    Thread.sleep(CHECKPOINT_SLEEP_INTERVAL);
                }

                collectedReader =
                        collectorMap.values().stream()
                                .mapToLong(e -> e.collectTotalCount() > 0 ? 1 : 0)
                                .sum();
                if (collectedReader != 0 || checkpointRetries == 0) {
                    checkpointRetries = 0;
                    internalCheckpoint(collectorMap.values().iterator(), 0);
                }
            } while (checkpointRetries > 0);
        } catch (Exception e) {
            throw new RuntimeException("An error occurred in virtual checkpoint execution.", e);
        }
    }

    private void internalCheckpoint(Iterator<InternalRowCollector> iterator, int loop)
            throws Exception {
        if (!iterator.hasNext()) {
            return;
        }
        synchronized (iterator.next().getCheckpointLock()) {
            internalCheckpoint(iterator, ++loop);
            if (loop != this.parallelism) {
                // Avoid backtracking calls
                return;
            }
            while (!handover.isEmpty()) {
                Thread.sleep(CHECKPOINT_SLEEP_INTERVAL);
            }
            // Block #next() method
            synchronized (handover) {
                final int currentCheckpoint = batchId;
                ReaderState readerState = snapshotState();
                saveState(readerState, currentCheckpoint);
                internalSource.notifyCheckpointComplete(currentCheckpoint);
                running = false;
            }
        }
    }

    @Override
    protected String getEnumeratorThreadName() {
        return "coordinated-split-enumerator-executor";
    }

    @Override
    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
        return new InternalCoordinatedSource<>(this, source, null, parallelism, jobId);
    }
}
