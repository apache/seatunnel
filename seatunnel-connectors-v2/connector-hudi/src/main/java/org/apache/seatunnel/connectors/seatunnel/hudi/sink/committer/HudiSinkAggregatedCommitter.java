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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.committer;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.AvroSchemaConverter.convertToSchema;

@Slf4j
public class HudiSinkAggregatedCommitter
        implements SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo> {

    private HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

    private final HoodieWriteConfig cfg;

    private final HadoopStorageConfiguration hudiStorageConfiguration;

    public HudiSinkAggregatedCommitter(
            HudiSinkConfig hudiSinkConfig, SeaTunnelRowType seaTunnelRowType) {

        Configuration hadoopConf = new Configuration();
        if (hudiSinkConfig.getConfFilesPath() != null) {
            hadoopConf = HudiUtil.getConfiguration(hudiSinkConfig.getConfFilesPath());
        }
        hudiStorageConfiguration = new HadoopStorageConfiguration(hadoopConf);
        cfg =
                HoodieWriteConfig.newBuilder()
                        .withEmbeddedTimelineServerEnabled(false)
                        .withEngineType(EngineType.JAVA)
                        .withPath(hudiSinkConfig.getTableDfsPath())
                        .withSchema(convertToSchema(seaTunnelRowType).toString())
                        .withParallelism(
                                hudiSinkConfig.getInsertShuffleParallelism(),
                                hudiSinkConfig.getUpsertShuffleParallelism())
                        .forTable(hudiSinkConfig.getTableName())
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder()
                                        .withIndexType(HoodieIndex.IndexType.INMEMORY)
                                        .build())
                        .withArchivalConfig(
                                HoodieArchivalConfig.newBuilder()
                                        .archiveCommitsWith(
                                                hudiSinkConfig.getMinCommitsToKeep(),
                                                hudiSinkConfig.getMaxCommitsToKeep())
                                        .build())
                        .withCleanConfig(
                                HoodieCleanConfig.newBuilder()
                                        .withAutoClean(true)
                                        .withAsyncClean(false)
                                        .build())
                        .build();
    }

    @Override
    public List<HudiAggregatedCommitInfo> commit(
            List<HudiAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {
        writeClient =
                new HoodieJavaWriteClient<>(
                        new HoodieJavaEngineContext(hudiStorageConfiguration), cfg);
        aggregatedCommitInfo =
                aggregatedCommitInfo.stream()
                        .filter(
                                commit ->
                                        commit.getHudiCommitInfoList().stream()
                                                .anyMatch(
                                                        aggreeCommit ->
                                                                !writeClient.commit(
                                                                        aggreeCommit
                                                                                .getInstantTime(),
                                                                        aggreeCommit
                                                                                .getWriteStatusList())))
                        .collect(Collectors.toList());

        return aggregatedCommitInfo;
    }

    @Override
    public HudiAggregatedCommitInfo combine(List<HudiCommitInfo> commitInfos) {
        return new HudiAggregatedCommitInfo(commitInfos);
    }

    @Override
    public void abort(List<HudiAggregatedCommitInfo> aggregatedCommitInfo) throws Exception {
        writeClient.rollbackFailedWrites();
    }

    @Override
    public void close() {
        if (writeClient != null) {
            writeClient.close();
        }
    }
}
