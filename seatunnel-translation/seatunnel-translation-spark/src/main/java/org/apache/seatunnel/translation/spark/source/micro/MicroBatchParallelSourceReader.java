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

package org.apache.seatunnel.translation.spark.source.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MicroBatchParallelSourceReader implements MicroBatchReader {

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final StructType rowType;

    protected final Integer checkpointInterval;
    protected Integer checkpointId;
    protected MicroBatchState startOffset;
    protected MicroBatchState endOffset;

    public MicroBatchParallelSourceReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer checkpointId, Integer checkpointInterval, StructType rowType) {
        this.source = source;
        this.parallelism = parallelism;
        this.checkpointInterval = checkpointInterval;
        this.rowType = rowType;
        this.checkpointId = checkpointId;
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        startOffset = (MicroBatchState) start.orElse(new MicroBatchState(checkpointId));
        endOffset = (MicroBatchState) end.orElse(new MicroBatchState(checkpointId + 1));
    }

    @Override
    public Offset getStartOffset() {
        return startOffset;
    }

    @Override
    public Offset getEndOffset() {
        return endOffset;
    }

    @Override
    public Offset deserializeOffset(String microBatchState) {
        return SerializationUtils.stringToObject(microBatchState);
    }

    @Override
    public void commit(Offset end) {
        // nothing
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public StructType readSchema() {
        return this.rowType;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> virtualPartitions = new ArrayList<>(parallelism);
        for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
            // TODO: get state
            List<byte[]> subtaskState = null;
            virtualPartitions.add(new MicroBatchPartition(source, parallelism, subtaskId, rowType, checkpointId, checkpointInterval, subtaskState));
        }
        checkpointId++;
        return virtualPartitions;
    }
}
