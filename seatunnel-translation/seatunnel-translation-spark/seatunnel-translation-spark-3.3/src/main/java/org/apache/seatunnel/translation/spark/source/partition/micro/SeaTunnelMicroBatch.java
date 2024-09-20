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

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.utils.CaseInsensitiveStringMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Getter
public class SeaTunnelMicroBatch implements MicroBatchStream {

    public static final Integer CHECKPOINT_INTERVAL_DEFAULT = 10000;

    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;

    private final int parallelism;
    private final String jobId;

    private final String checkpointLocation;

    private final CaseInsensitiveStringMap caseInsensitiveStringMap;

    private final Offset initialOffset = SeaTunnelOffset.of(0L);

    private Offset currentOffset = initialOffset;

    private final MultiTableManager multiTableManager;
    private CheckpointDataLogManager logManager;

    public SeaTunnelMicroBatch(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            int parallelism,
            String jobId,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.checkpointLocation = checkpointLocation;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
        this.multiTableManager = multiTableManager;
        SparkSession sparkSession = SparkSession.getActiveSession().get();
        this.logManager =
                new CheckpointDataLogManager(
                        CheckpointFileManager.create(
                                new Path(this.checkpointLocation).getParent().getParent(),
                                sparkSession.sessionState().newHadoopConf()));
    }

    @Override
    public Offset latestOffset() {
        // TODO
        Path commitsPath =
                new Path(new Path(checkpointLocation).getParent().getParent(), "commits");
        int maxBatchId = this.logManager.maxNum(commitsPath, Optional.empty());
        return new SeaTunnelOffset(maxBatchId + 1);
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start, Offset end) {
        SeaTunnelOffset startOffset = (SeaTunnelOffset) start;
        int checkpointInterval =
                caseInsensitiveStringMap.getInt(
                        EnvCommonOptions.CHECKPOINT_INTERVAL.key(), CHECKPOINT_INTERVAL_DEFAULT);
        Configuration configuration =
                SparkSession.getActiveSession().get().sparkContext().hadoopConfiguration();
        String hdfsRoot =
                caseInsensitiveStringMap.getOrDefault(
                        Constants.HDFS_ROOT, FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = caseInsensitiveStringMap.getOrDefault(Constants.HDFS_USER, "");
        List<InputPartition> virtualPartitions;
        if (source instanceof SupportCoordinate) {
            virtualPartitions = new ArrayList<>(1);
            virtualPartitions.add(
                    new SeaTunnelMicroBatchInputPartition(
                            source,
                            parallelism,
                            0,
                            (int) startOffset.getCheckpointId(),
                            checkpointInterval,
                            checkpointLocation,
                            hdfsRoot,
                            hdfsUser));
        } else {
            virtualPartitions = new ArrayList<>(parallelism);
            for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
                virtualPartitions.add(
                        new SeaTunnelMicroBatchInputPartition(
                                source,
                                parallelism,
                                subtaskId,
                                (int) startOffset.getCheckpointId(),
                                checkpointInterval,
                                checkpointLocation,
                                hdfsRoot,
                                hdfsUser));
            }
        }
        return virtualPartitions.toArray(new InputPartition[parallelism]);
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SeaTunnelMicroBatchPartitionReaderFactory(
                source,
                parallelism,
                jobId,
                checkpointLocation,
                caseInsensitiveStringMap,
                multiTableManager);
    }

    @Override
    public Offset initialOffset() {
        return initialOffset;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return JsonUtils.parseObject(json, SeaTunnelOffset.class);
    }

    @Override
    public void commit(Offset end) {
        this.currentOffset = ((SeaTunnelOffset) this.currentOffset).inc();
    }

    @Override
    public void stop() {
        // do nothing
    }
}
