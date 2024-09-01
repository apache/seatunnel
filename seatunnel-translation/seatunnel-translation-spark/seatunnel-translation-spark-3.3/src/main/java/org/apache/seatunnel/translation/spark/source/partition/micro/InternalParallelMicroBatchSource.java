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
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class InternalParallelMicroBatchSource<
                SplitT extends SourceSplit, StateT extends Serializable>
        extends ParallelSource<SeaTunnelRow, SplitT, StateT> {

    private final ParallelMicroBatchPartitionReader parallelBatchPartitionReader;

    public InternalParallelMicroBatchSource(
            ParallelMicroBatchPartitionReader parallelBatchPartitionReader,
            SeaTunnelSource<SeaTunnelRow, SplitT, StateT> source,
            Map<Integer, List<byte[]>> restoredState,
            int parallelism,
            String jobId,
            int subtaskId) {
        super(source, restoredState, parallelism, jobId, subtaskId);
        this.parallelBatchPartitionReader = parallelBatchPartitionReader;
    }

    @Override
    protected void handleNoMoreElement() {
        super.handleNoMoreElement();
        parallelBatchPartitionReader.running = false;
    }
}
