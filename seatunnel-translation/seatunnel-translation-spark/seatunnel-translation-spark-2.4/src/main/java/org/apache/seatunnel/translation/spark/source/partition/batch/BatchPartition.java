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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.source.reader.SeaTunnelInputPartitionReader;
import org.apache.seatunnel.translation.spark.source.reader.batch.CoordinatedBatchPartitionReader;
import org.apache.seatunnel.translation.spark.source.reader.batch.ParallelBatchPartitionReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

import java.util.Map;

public class BatchPartition implements InputPartition<InternalRow> {
    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final Integer subtaskId;
    private Map<String, String> envOptions;

    public BatchPartition(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            Integer subtaskId,
            Map<String, String> envOptions) {
        this.source = source;
        this.parallelism = parallelism;
        this.subtaskId = subtaskId;
        this.envOptions = envOptions;
    }

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader() {
        ParallelBatchPartitionReader partitionReader;
        if (source instanceof SupportCoordinate) {
            partitionReader =
                    new CoordinatedBatchPartitionReader(source, parallelism, subtaskId, envOptions);
        } else {
            partitionReader =
                    new ParallelBatchPartitionReader(source, parallelism, subtaskId, envOptions);
        }
        return new SeaTunnelInputPartitionReader(partitionReader);
    }
}
