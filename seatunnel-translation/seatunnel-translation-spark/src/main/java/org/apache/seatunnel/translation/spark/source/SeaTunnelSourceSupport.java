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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.spark.source.batch.BatchParallelSourceReader;
import org.apache.seatunnel.translation.spark.source.continnous.ContinuousParallelSourceReader;
import org.apache.seatunnel.translation.spark.source.micro.MicroBatchParallelSourceReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.ContinuousReadSupport;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.MicroBatchReadSupport;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader;
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class SeaTunnelSourceSupport implements DataSourceV2, ReadSupport, MicroBatchReadSupport, ContinuousReadSupport, DataSourceRegister {

    public static final String SEA_TUNNEL_SOURCE_NAME = "SeaTunnelSource";
    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    @Override
    public String shortName() {
        return SEA_TUNNEL_SOURCE_NAME;
    }

    @Override
    public DataSourceReader createReader(StructType rowType, DataSourceOptions options) {
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        Integer parallelism = options.getInt("source.parallelism", 1);
        // TODO: case coordinated source
        return new BatchParallelSourceReader(seaTunnelSource, parallelism, rowType);
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        throw createUnspecifiedRowTypeException();
    }

    @Override
    public MicroBatchReader createMicroBatchReader(Optional<StructType> rowTypeOptional, String checkpointLocation, DataSourceOptions options) {
        StructType rowType = checkRowType(rowTypeOptional);
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        Integer parallelism = options.getInt("source.parallelism", 1);
        Integer checkpointInterval = options.getInt("checkpoint.interval", CHECKPOINT_INTERVAL_DEFAULT);
        String checkpointPath = StringUtils.replacePattern(checkpointLocation, "sources/\\d+", "sources-state");
        Configuration configuration = SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot = options.get("hdfs.root").orElse(FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = options.get("hdfs.user").orElseThrow(() -> new UnsupportedOperationException("HDFS user is required."));
        Integer checkpointId = options.getInt("checkpoint.id", 1);
        // TODO: case coordinated source
        return new MicroBatchParallelSourceReader(seaTunnelSource, parallelism, checkpointId, checkpointInterval, checkpointPath, hdfsRoot, hdfsUser, rowType);
    }

    @Override
    public ContinuousReader createContinuousReader(Optional<StructType> rowTypeOptional, String checkpointLocation, DataSourceOptions options) {
        StructType rowType = checkRowType(rowTypeOptional);
        SeaTunnelSource<SeaTunnelRow, ?, ?> seaTunnelSource = getSeaTunnelSource(options);
        Integer parallelism = options.getInt("source.parallelism", 1);
        // TODO: case coordinated source
        return new ContinuousParallelSourceReader(seaTunnelSource, parallelism, rowType);
    }

    private SeaTunnelSource<SeaTunnelRow, ?, ?> getSeaTunnelSource(DataSourceOptions options) {
        return SerializationUtils.stringToObject(options.get("source.serialization")
                .orElseThrow(() -> new UnsupportedOperationException("Serialization information for the SeaTunnelSource is required")));
    }

    private static StructType checkRowType(Optional<StructType> rowTypeOptional) {
        return rowTypeOptional.orElse(null);
    }

    private static RuntimeException createUnspecifiedRowTypeException() {
        return new UnsupportedOperationException("SeaTunnel Spark source does not support user unspecified row type.");
    }
}
