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

package org.apache.seatunnel.translation.spark.source.continnous;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.ReaderState;
import org.apache.seatunnel.translation.spark.source.batch.ParallelBatchPartitionReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousInputPartitionReader;
import org.apache.spark.sql.sources.v2.reader.streaming.PartitionOffset;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ParallelContinuousPartitionReader extends ParallelBatchPartitionReader implements ContinuousInputPartitionReader<InternalRow> {
    protected volatile Integer checkpointId;
    protected final Map<Integer, List<byte[]>> restoredState;

    public ParallelContinuousPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer subtaskId, Integer checkpointId, Map<Integer, List<byte[]>> restoredState) {
        super(source, parallelism, subtaskId);
        this.checkpointId = checkpointId;
        this.restoredState = restoredState;
    }

    @Override
    protected ParallelSource<SeaTunnelRow, ?, ?> createInternalSource() {
        return new InternalParallelSource<>(source,
            restoredState,
            parallelism,
            subtaskId);
    }

    @Override
    public PartitionOffset getOffset() {
        Map<Integer, List<byte[]>> bytes;
        try {
            bytes = internalSource.snapshotState(checkpointId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ReaderState readerState = new ReaderState(bytes, subtaskId, checkpointId++);
        return readerState;
    }

    // TODO: RPC call

    /**
     * The method is called by RPC
     */
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        internalSource.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() throws IOException {
        super.close();
        // TODO: close rpc
    }
}
