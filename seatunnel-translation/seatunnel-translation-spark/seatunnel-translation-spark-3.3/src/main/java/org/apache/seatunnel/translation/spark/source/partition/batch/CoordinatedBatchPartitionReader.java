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

package org.apache.seatunnel.translation.spark.source.partition.batch;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.source.CoordinatedSource;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatedBatchPartitionReader extends ParallelBatchPartitionReader {

    private static final long ENUMERATOR_CHECKPOINT_INTERVAL_MS = 1000L;

    protected final Map<Integer, InternalRowCollector> collectorMap;

    public CoordinatedBatchPartitionReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        super(source, parallelism, jobId, subtaskId, envOptions, multiTableManager);
        this.collectorMap = new HashMap<>(parallelism);
        for (int i = 0; i < parallelism; i++) {
            collectorMap.put(
                    i,
                    multiTableManager.getInternalRowCollector(handover, new Object(), envOptions));
        }
    }

    @Override
    protected String getEnumeratorThreadName() {
        return "coordinated-split-enumerator-executor";
    }

    @Override
    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
        return new InternalCoordinatedSource<>(source, null, parallelism, jobId);
    }

    public class InternalCoordinatedSource<SplitT extends SourceSplit, StateT extends Serializable>
            extends CoordinatedSource<SeaTunnelRow, SplitT, StateT> {

        public InternalCoordinatedSource(
                SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source,
                Map<Integer, List<byte[]>> restoredState,
                int parallelism,
                String jobId) {
            super(source, restoredState, parallelism, jobId);
        }

        @Override
        protected Collector<SeaTunnelRow> getReaderCollector(
                int subtaskId, Collector<SeaTunnelRow> collector) {
            return collectorMap.get(subtaskId);
        }

        /**
         * The batch read never checkpoints the source, so checkpoint the enumerator alone to let
         * enumerators that wait for a completed checkpoint, such as the CDC snapshot split assigner
         * at a parallelism above one, move on. Every reader of this source emits into the same
         * handover queue of this single partition, so records still reach the sink in the order
         * they were emitted.
         */
        @Override
        protected long getEnumeratorCheckpointInterval() {
            return ENUMERATOR_CHECKPOINT_INTERVAL_MS;
        }

        @Override
        protected void handleNoMoreElement(int subtaskId) {
            super.handleNoMoreElement(subtaskId);
            if (!this.running) {
                CoordinatedBatchPartitionReader.this.running = false;
            }
        }
    }
}
