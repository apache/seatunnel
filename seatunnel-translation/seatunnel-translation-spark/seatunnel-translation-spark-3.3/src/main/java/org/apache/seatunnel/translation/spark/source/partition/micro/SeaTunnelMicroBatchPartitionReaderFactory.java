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
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.source.partition.batch.ParallelBatchPartitionReader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SeaTunnelMicroBatchPartitionReaderFactory implements PartitionReaderFactory {

    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;

    private final int parallelism;

    private final String checkpointLocation;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    public SeaTunnelMicroBatchPartitionReaderFactory(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap) {
        this.source = source;
        this.parallelism = parallelism;
        this.checkpointLocation = checkpointLocation;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        SeaTunnelMicroBatchInputPartition seaTunnelPartition =
                (SeaTunnelMicroBatchInputPartition) partition;
        ParallelBatchPartitionReader partitionReader;
        Integer subtaskId = seaTunnelPartition.getSubtaskId();
        Integer checkpointId = seaTunnelPartition.getCheckpointId();
        Integer checkpointInterval = seaTunnelPartition.getCheckpointInterval();
        String hdfsRoot = seaTunnelPartition.getHdfsRoot();
        String hdfsUser = seaTunnelPartition.getHdfsUser();
        Map<String, String> envOptions = caseInsensitiveStringMap.asCaseSensitiveMap();
        if (source instanceof SupportCoordinate) {
            partitionReader =
                    new CoordinatedMicroBatchPartitionReader(
                            source,
                            parallelism,
                            subtaskId,
                            checkpointId,
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser,
                            envOptions);
        } else {
            partitionReader =
                    new ParallelMicroBatchPartitionReader(
                            source,
                            parallelism,
                            subtaskId,
                            checkpointId,
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser,
                            envOptions);
        }
        return new SeaTunnelMicroBatchPartitionReader(partitionReader);
    }
}
