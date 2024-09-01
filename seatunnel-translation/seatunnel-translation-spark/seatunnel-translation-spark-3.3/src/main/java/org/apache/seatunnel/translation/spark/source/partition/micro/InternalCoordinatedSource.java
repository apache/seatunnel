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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.CoordinatedSource;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class InternalCoordinatedSource<SplitT extends SourceSplit, StateT extends Serializable>
        extends CoordinatedSource<SeaTunnelRow, SplitT, StateT> {

    private final CoordinatedMicroBatchPartitionReader coordinatedMicroBatchPartitionReader;

    public InternalCoordinatedSource(
            CoordinatedMicroBatchPartitionReader coordinatedMicroBatchPartitionReader,
            SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source,
            Map<Integer, List<byte[]>> restoredState,
            int parallelism,
            String jobId) {
        super(source, restoredState, parallelism, jobId);
        this.coordinatedMicroBatchPartitionReader = coordinatedMicroBatchPartitionReader;
    }

    @Override
    public void run(Collector<SeaTunnelRow> collector) throws Exception {
        readerMap
                .entrySet()
                .parallelStream()
                .forEach(
                        entry -> {
                            final AtomicBoolean flag = readerRunningMap.get(entry.getKey());
                            final SourceReader<SeaTunnelRow, SplitT> reader = entry.getValue();
                            final Collector<SeaTunnelRow> rowCollector =
                                    coordinatedMicroBatchPartitionReader.collectorMap.get(
                                            entry.getKey());
                            executorService.execute(
                                    () -> {
                                        while (flag.get()) {
                                            try {
                                                reader.pollNext(rowCollector);
                                                if (rowCollector.isEmptyThisPollNext()) {
                                                    Thread.sleep(100);
                                                } else {
                                                    rowCollector.resetEmptyThisPollNext();
                                                    /**
                                                     * sleep(0) is used to prevent the current
                                                     * thread from occupying CPU resources for a
                                                     * long time, thus blocking the checkpoint
                                                     * thread for a long time. It is mentioned in
                                                     * this
                                                     * https://github.com/apache/seatunnel/issues/5694
                                                     */
                                                    Thread.sleep(0L);
                                                }
                                            } catch (Exception e) {
                                                this.running = false;
                                                flag.set(false);
                                                throw new RuntimeException(e);
                                            }
                                        }
                                    });
                        });
        splitEnumerator.run();
        while (this.running) {
            Thread.sleep(SLEEP_TIME_INTERVAL);
        }
    }

    @Override
    protected void handleNoMoreElement(int subtaskId) {
        super.handleNoMoreElement(subtaskId);
        if (!this.running) {
            coordinatedMicroBatchPartitionReader.setRunning(false);
        }
    }
}
