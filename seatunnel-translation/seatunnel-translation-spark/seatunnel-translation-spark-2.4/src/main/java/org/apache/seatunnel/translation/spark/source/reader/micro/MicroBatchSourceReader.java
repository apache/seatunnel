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

package org.apache.seatunnel.translation.spark.source.reader.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.partition.micro.MicroBatchPartition;
import org.apache.seatunnel.translation.spark.source.state.MicroBatchState;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.sources.v2.reader.streaming.Offset;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MicroBatchSourceReader implements MicroBatchReader {

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final String jobId;

    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;
    protected Integer checkpointId;
    protected MicroBatchState startOffset;
    protected MicroBatchState endOffset;
    private Map<String, String> envOptions;
    private final MultiTableManager multiTableManager;

    public MicroBatchSourceReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer checkpointId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.checkpointId = checkpointId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
        this.envOptions = envOptions;
        this.multiTableManager = multiTableManager;
    }

    @Override
    public void setOffsetRange(Optional<Offset> start, Optional<Offset> end) {
        startOffset = (MicroBatchState) start.orElse(new MicroBatchState(checkpointId));
        this.checkpointId = startOffset.getCheckpointId();
        endOffset =
                (MicroBatchState)
                        end.orElse(new MicroBatchState(startOffset.getCheckpointId() + 1));
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
        System.out.println(end);
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public StructType readSchema() {
        return multiTableManager.getTableSchema();
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> virtualPartitions;
        if (source instanceof SupportCoordinate) {
            virtualPartitions = new ArrayList<>(1);
            virtualPartitions.add(
                    new MicroBatchPartition(
                            source,
                            parallelism,
                            jobId,
                            0,
                            checkpointId,
                            checkpointInterval,
                            checkpointPath,
                            hdfsRoot,
                            hdfsUser,
                            envOptions,
                            multiTableManager));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new MicroBatchPartition(
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
                                multiTableManager));
            }
        }
        checkpointId++;
        return virtualPartitions;
    }
}
