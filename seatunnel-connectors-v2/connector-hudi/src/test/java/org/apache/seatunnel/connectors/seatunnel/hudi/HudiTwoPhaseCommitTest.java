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

package org.apache.seatunnel.connectors.seatunnel.hudi;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSemantics;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.commit.HudiSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.writer.HudiRecordWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiCatalogUtil.inferTablePath;

/**
 * Verifies the two phase commit of the hudi sink against a real hudi table.
 *
 * <p>The writer only writes the records of a checkpoint into a hudi instant, the aggregated
 * committer commits the instant after the checkpoint completes, and a commit that runs twice does
 * not publish the records twice.
 */
@DisabledOnOs(OS.WINDOWS)
class HudiTwoPhaseCommitTest {

    private static final String TABLE_NAME = "hudi_2pc";

    private static final String DATABASE = "default";

    private static final int BATCH_SIZE = 2;

    private static final int RECORD_COUNT = 4;

    private static final SeaTunnelRowType SEA_TUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name"},
                    new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

    @TempDir java.nio.file.Path tempDir;

    @Test
    void shouldPublishTheRecordsOnlyWhenTheCheckpointCompletes() throws Exception {
        HudiSinkConfig sinkConfig = sinkConfig(HudiSemantics.EXACTLY_ONCE);
        HoodieTableMetaClient metaClient = metaClient(createTable(sinkConfig));

        // 1. the writer writes the records of one checkpoint with two batches, both batches are
        // written with the same instant and the instant is not committed
        HudiRecordWriter writer = newWriter(sinkConfig);
        Optional<HudiCommitInfo> commitInfo;
        try {
            writeRecords(writer, RECORD_COUNT);
            commitInfo = writer.prepareCommit(1L);
        } finally {
            writer.close();
        }

        Assertions.assertTrue(commitInfo.isPresent());
        HudiCommitInfo hudiCommitInfo = commitInfo.get();
        String instantTime = hudiCommitInfo.getInstantTime();
        Assertions.assertFalse(
                hudiCommitInfo.getWriteStatuses().isEmpty(),
                "the write statuses are required to commit the instant");

        HoodieTimeline activeTimeline = metaClient.reloadActiveTimeline();
        Assertions.assertTrue(
                activeTimeline.containsInstant(instantTime),
                "the instant of the checkpoint must be on the active timeline");
        Assertions.assertEquals(
                1,
                activeTimeline.filterInflightsAndRequested().countInstants(),
                "all the batches of one checkpoint must be written with one instant");
        Assertions.assertFalse(
                activeTimeline.filterCompletedInstants().containsInstant(instantTime),
                "the records must not be visible before the checkpoint completes");

        // 2. the aggregated committer commits the instant after the checkpoint completed
        HudiSinkAggregatedCommitter committer = newCommitter(sinkConfig);
        try {
            Assertions.assertTrue(commit(committer, hudiCommitInfo).isEmpty());

            HoodieTimeline completedTimeline = metaClient.reloadActiveTimeline();
            Assertions.assertEquals(
                    Collections.singletonList(instantTime),
                    commitTimestamps(completedTimeline),
                    "the instant of the completed checkpoint must be committed");
            Assertions.assertEquals(RECORD_COUNT, totalRecords(completedTimeline));

            // 3. the commit is idempotent, a commit that was lost by a failure commits the same
            // instant again without publishing the records twice
            Assertions.assertTrue(commit(committer, hudiCommitInfo).isEmpty());
            HoodieTimeline timelineAfterRestore = metaClient.reloadActiveTimeline();
            Assertions.assertEquals(
                    1,
                    commitTimestamps(timelineAfterRestore).size(),
                    "the instant is committed once");
            Assertions.assertEquals(RECORD_COUNT, totalRecords(timelineAfterRestore));
        } finally {
            committer.close();
        }
    }

    @Test
    void shouldRollbackTheInstantOfAnAbortedCheckpoint() throws Exception {
        HudiSinkConfig sinkConfig = sinkConfig(HudiSemantics.EXACTLY_ONCE);
        HoodieTableMetaClient metaClient = metaClient(createTable(sinkConfig));

        HudiRecordWriter writer = newWriter(sinkConfig);
        HudiCommitInfo hudiCommitInfo;
        try {
            writeRecords(writer, RECORD_COUNT);
            hudiCommitInfo = writer.prepareCommit(1L).get();
        } finally {
            writer.close();
        }
        String instantTime = hudiCommitInfo.getInstantTime();

        HudiSinkAggregatedCommitter committer = newCommitter(sinkConfig);
        try {
            committer.abort(
                    Collections.singletonList(
                            committer.combine(Collections.singletonList(hudiCommitInfo))));

            // the records of a checkpoint that never completed are replayed by the source, so the
            // instant that holds them is rolled back
            Assertions.assertFalse(
                    metaClient.reloadActiveTimeline().containsInstant(instantTime),
                    "the instant of the aborted checkpoint must be rolled back");
            Assertions.assertThrows(
                    HudiConnectorException.class,
                    () -> commit(committer, hudiCommitInfo),
                    "committing an instant that was rolled back must fail instead of losing data silently");
        } finally {
            committer.close();
        }
    }

    @Test
    void shouldCommitEveryBatchImmediatelyWithTheDefaultSemantics() throws Exception {
        HudiSinkConfig sinkConfig = sinkConfig(HudiSemantics.AT_LEAST_ONCE);
        HoodieTableMetaClient metaClient = metaClient(createTable(sinkConfig));

        HudiRecordWriter writer = newWriter(sinkConfig);
        try {
            writeRecords(writer, RECORD_COUNT);

            // the Hudi client auto-commit publishes every batch, nothing waits for a checkpoint
            Assertions.assertFalse(writer.prepareCommit(1L).isPresent());
            HoodieTimeline timeline = metaClient.reloadActiveTimeline();
            Assertions.assertEquals(
                    RECORD_COUNT / BATCH_SIZE,
                    commitTimestamps(timeline).size(),
                    "every flushed batch must be committed by the client itself");
        } finally {
            writer.close();
        }
    }

    private HudiRecordWriter newWriter(HudiSinkConfig sinkConfig) {
        HudiWriteClientProvider writeClientProvider =
                new HudiWriteClientProvider(sinkConfig, TABLE_NAME, SEA_TUNNEL_ROW_TYPE);
        HudiRecordWriter writer =
                new HudiRecordWriter(
                        tableConfig(),
                        writeClientProvider,
                        SEA_TUNNEL_ROW_TYPE,
                        sinkConfig.isExactlyOnce());
        writer.open();
        return writer;
    }

    private void writeRecords(HudiRecordWriter writer, int count) {
        for (int i = 1; i <= count; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {i, "name-" + i});
            row.setRowKind(RowKind.INSERT);
            writer.writeRecord(row);
        }
    }

    private HudiSinkAggregatedCommitter newCommitter(HudiSinkConfig sinkConfig) {
        HudiSinkAggregatedCommitter committer =
                new HudiSinkAggregatedCommitter(sinkConfig, tableConfig(), SEA_TUNNEL_ROW_TYPE);
        committer.init();
        return committer;
    }

    private List<HudiAggregatedCommitInfo> commit(
            HudiSinkAggregatedCommitter committer, HudiCommitInfo commitInfo) {
        return committer.commit(
                Collections.singletonList(
                        committer.combine(Collections.singletonList(commitInfo))));
    }

    private List<String> commitTimestamps(HoodieTimeline timeline) {
        return timeline.filterCompletedInstants().getInstants().stream()
                .filter(instant -> HoodieTimeline.COMMIT_ACTION.equals(instant.getAction()))
                .map(HoodieInstant::getTimestamp)
                .sorted()
                .collect(Collectors.toList());
    }

    private long totalRecords(HoodieTimeline timeline) throws IOException {
        HoodieTimeline completedTimeline = timeline.filterCompletedInstants();
        long total = 0;
        for (HoodieInstant commit : completedTimeline.getInstants()) {
            if (!HoodieTimeline.COMMIT_ACTION.equals(commit.getAction())) {
                continue;
            }
            HoodieCommitMetadata metadata =
                    HoodieCommitMetadata.fromBytes(
                            completedTimeline.getInstantDetails(commit).get(),
                            HoodieCommitMetadata.class);
            total += metadata.fetchTotalRecordsWritten();
        }
        return total;
    }

    private String createTable(HudiSinkConfig sinkConfig) throws IOException {
        String tablePath = inferTablePath(sinkConfig.getTableDfsPath(), DATABASE, TABLE_NAME);
        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName(TABLE_NAME)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new HadoopStorageConfiguration(new Configuration()), tablePath);
        return tablePath;
    }

    private HoodieTableMetaClient metaClient(String tablePath) {
        return HoodieTableMetaClient.builder()
                .setBasePath(tablePath)
                .setConf(HadoopFSUtils.getStorageConfWithCopy(new Configuration()))
                .build();
    }

    private HudiSinkConfig sinkConfig(HudiSemantics semantics) {
        return HudiSinkConfig.builder()
                .tableDfsPath(tempDir.toString())
                .tableList(Collections.singletonList(tableConfig()))
                .semantics(semantics)
                .build();
    }

    private HudiTableConfig tableConfig() {
        return HudiTableConfig.builder()
                .tableName(TABLE_NAME)
                .database(DATABASE)
                .tableType(HoodieTableType.COPY_ON_WRITE)
                .opType(WriteOperationType.INSERT)
                .indexType(HoodieIndex.IndexType.INMEMORY)
                .recordByteSize(1024)
                .batchSize(BATCH_SIZE)
                .batchIntervalMs(1000)
                .insertShuffleParallelism(2)
                .upsertShuffleParallelism(2)
                .minCommitsToKeep(20)
                .maxCommitsToKeep(30)
                .build();
    }
}
