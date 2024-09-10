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

package org.apache.seatunnel.translation.spark.source.partition.continuous;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;

public class SeaTunnelContinuousPartitionReaderFactory implements ContinuousPartitionReaderFactory {
    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;

    public SeaTunnelContinuousPartitionReaderFactory(SeaTunnelSource<SeaTunnelRow, ?, ?> source) {
        this.source = source;
    }

    @Override
    public ContinuousPartitionReader<InternalRow> createReader(InputPartition partition) {
        SeaTunnelInputPartition inputPartition = (SeaTunnelInputPartition) partition;
        return new SeaTunnelContinuousPartitionReader<>(
                (SeaTunnelSource<SeaTunnelRow, SourceSplit, ?>) source, inputPartition);
    }
}
