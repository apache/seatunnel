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
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.HudiOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.util.List;

public class HudiSinkAggregatedCommitter
        implements SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo> {

    private final HudiSinkConfig hudiSinkConfig;

    private final HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

    private final HudiOutputFormat hudiOutputFormat;

    public HudiSinkAggregatedCommitter(
            HudiSinkConfig hudiSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        this.hudiSinkConfig = hudiSinkConfig;
        Configuration hadoopConf = HudiUtil.getConfiguration(hudiSinkConfig.getConfFile());
        hudiOutputFormat = new HudiOutputFormat();
        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withPath(hudiSinkConfig.getTablePath())
                        .withSchema(hudiOutputFormat.convertSchema(seaTunnelRowType))
                        .withParallelism(
                                hudiSinkConfig.getInsertShuffleParallelism(),
                                hudiSinkConfig.getUpsertShuffleParallelism())
                        .withDeleteParallelism(hudiSinkConfig.getDeleteShuffleParallelism())
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
                        .build();

        writeClient = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
    }

    @Override
    public List<HudiAggregatedCommitInfo> commit(
            List<HudiAggregatedCommitInfo> aggregatedCommitInfo) throws IOException {

        aggregatedCommitInfo.stream()
                .filter(
                        commit ->
                                commit.getHudiCommitInfoList().stream()
                                                .filter(
                                                        aggreeCommit ->
                                                                !writeClient.commit(
                                                                        aggreeCommit
                                                                                .getInstantTime(),
                                                                        aggreeCommit
                                                                                .getWriteStatusList()))
                                                .count()
                                        > 0);

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
