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

package org.apache.seatunnel.translation.spark.source.reader.batch;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.partition.batch.BatchPartition;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BatchSourceReader implements DataSourceReader {

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final String jobId;
    protected final Integer parallelism;
    private Map<String, String> envOptions;
    private final MultiTableManager multiTableManager;

    public BatchSourceReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            String jobId,
            Integer parallelism,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.jobId = jobId;
        this.parallelism = parallelism;
        this.envOptions = envOptions;
        this.multiTableManager = multiTableManager;
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
                    new BatchPartition(
                            source, parallelism, jobId, 0, envOptions, multiTableManager));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new BatchPartition(
                                source,
                                parallelism,
                                jobId,
                                subtaskId,
                                envOptions,
                                multiTableManager));
            }
        }
        return virtualPartitions;
    }
}
