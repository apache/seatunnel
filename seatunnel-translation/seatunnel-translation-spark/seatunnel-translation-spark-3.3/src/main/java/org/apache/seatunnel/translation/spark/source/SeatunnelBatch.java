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

package org.apache.seatunnel.translation.spark.source;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

public class SeatunnelBatch implements Batch {

    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    private final Integer parallelism;

    public SeatunnelBatch(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism) {
        this.source = source;
        this.parallelism = parallelism;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        InputPartition[] partitions;
        if (source instanceof SupportCoordinate) {
            partitions = new SeatunnelInputPartition[1];
            partitions[0] = new SeatunnelInputPartition(0);
        } else {
            partitions = new SeatunnelInputPartition[parallelism];
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                partitions[subtaskId] = new SeatunnelInputPartition(subtaskId);
            }
        }
        return partitions;
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SeatunnelPartitionReaderFactory(source, parallelism);
    }
}
