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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.commit;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableConfig;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.HudiWriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.client.WriteClientProvider;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.state.HudiCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiCatalogUtil.inferTablePath;

/**
 * Commits the Hudi instants that the writers prepared for a checkpoint.
 *
 * <p>With the exactly-once semantics the writers only write the records of a checkpoint into a Hudi
 * instant and never commit it. This committer runs after the checkpoint completed, which means that
 * the records of the checkpoint are durably checkpointed and are never replayed again, and commits
 * the instants of that checkpoint.
 *
 * <p>The commit is idempotent: an instant that is already completed on the active timeline is
 * skipped, so a commit that was lost by a failure is simply applied again when the job restores
 * from the checkpoint. The commit infos are part of the checkpoint state of the committer, which is
 * how the engine hands the pending instants back to it after a restart.
 */
@Slf4j
public class HudiSinkAggregatedCommitter
        implements SinkAggregatedCommitter<HudiCommitInfo, HudiAggregatedCommitInfo> {

    private static final long serialVersionUID = 1L;

    private final HudiSinkConfig sinkConfig;

    private final HudiTableConfig tableConfig;

    private final SeaTunnelRowType seaTunnelRowType;

    private transient WriteClientProvider writeClientProvider;

    private transient HoodieTableMetaClient metaClient;

    /**
     * The instants that were aggregated for a checkpoint but not committed yet. They are only used
     * to report a job that ends without committing its last instants.
     */
    private final Set<String> pendingInstants = new HashSet<>();

    public HudiSinkAggregatedCommitter(
            HudiSinkConfig sinkConfig,
            HudiTableConfig tableConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void init() {
        // the SinkAggregatedCommitter contract allows the engine to call init() again on a retry,
        // release the client of the previous attempt before replacing it
        if (writeClientProvider != null) {
            writeClientProvider.close();
        }
        this.writeClientProvider =
                new HudiWriteClientProvider(
                        sinkConfig, tableConfig.getTableName(), seaTunnelRowType);
        // fail fast when the table is not reachable instead of failing on the first commit
        this.writeClientProvider.getOrCreateClient();
        Configuration hadoopConf = HudiUtil.getConfiguration(sinkConfig.getConfFilesPath());
        this.metaClient =
                HoodieTableMetaClient.builder()
                        .setBasePath(
                                inferTablePath(
                                        sinkConfig.getTableDfsPath(),
                                        tableConfig.getDatabase(),
                                        tableConfig.getTableName()))
                        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
                        .build();
    }

    @Override
    public HudiAggregatedCommitInfo combine(List<HudiCommitInfo> commitInfos) {
        // a writer that has no record to commit sends an empty commit info
        List<HudiCommitInfo> pendingCommitInfos =
                commitInfos.stream()
                        .filter(Objects::nonNull)
                        .filter(commitInfo -> commitInfo.getInstantTime() != null)
                        .collect(Collectors.toList());
        pendingCommitInfos.forEach(commitInfo -> pendingInstants.add(commitInfo.getInstantTime()));
        return new HudiAggregatedCommitInfo(pendingCommitInfos);
    }

    /**
     * Commits every instant that the writers prepared for the checkpoints that completed.
     *
     * @param aggregatedCommitInfos the commit infos of the completed checkpoints
     * @return always empty, a failed commit throws instead
     */
    @Override
    public List<HudiAggregatedCommitInfo> commit(
            List<HudiAggregatedCommitInfo> aggregatedCommitInfos) {
        List<HudiCommitInfo> commitInfos = flatten(aggregatedCommitInfos);
        if (commitInfos.isEmpty()) {
            return Collections.emptyList();
        }
        // the timeline of the table is reloaded once per call instead of once per commit info
        HoodieTimeline activeTimeline = reloadActiveTimeline();
        for (HudiCommitInfo commitInfo : commitInfos) {
            commitInstant(commitInfo, activeTimeline);
        }
        return Collections.emptyList();
    }

    /**
     * Commits the pending instants that were restored from the checkpoint state of the committer.
     *
     * <p>This is the recovery path of a commit that was lost by a failure between the checkpoint
     * completion and the commit. It is safe to run again, because the instants that are already
     * completed are skipped.
     */
    @Override
    public List<HudiAggregatedCommitInfo> restoreCommit(
            List<HudiAggregatedCommitInfo> aggregatedCommitInfos) {
        log.info(
                "Restore commit for hudi table [{}], commit info size [{}].",
                tableConfig.getTableName(),
                aggregatedCommitInfos.size());
        return commit(aggregatedCommitInfos);
    }

    /**
     * Rolls back the instants of a checkpoint that was aborted.
     *
     * <p>Per the {@link SinkAggregatedCommitter} contract this is currently only called by the
     * Spark engine; the engines that restart the pipeline after a failure, Zeta and Flink, do not
     * call it. The data of a checkpoint that never completed is replayed by the source after such a
     * restart, so the instants that hold it must not be committed, and the instants of a writer
     * that died are rolled back by Hudi itself once their heartbeat expires, which the exactly-once
     * semantics enables by switching the failed writes cleaning policy to LAZY.
     */
    @Override
    public void abort(List<HudiAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        List<HudiCommitInfo> commitInfos = flatten(aggregatedCommitInfos);
        if (commitInfos.isEmpty()) {
            return;
        }
        HoodieTimeline activeTimeline = reloadActiveTimeline();
        for (HudiCommitInfo commitInfo : commitInfos) {
            rollbackInstant(commitInfo, activeTimeline);
        }
    }

    @Override
    public void close() throws IOException {
        if (!pendingInstants.isEmpty()) {
            log.error(
                    "The hudi table [{}] ends with {} instant(s) that were never committed: {}. With the "
                            + "exactly-once semantics the records of a checkpoint are committed only after that "
                            + "checkpoint completes, so please check that the checkpoint is enabled and that the last "
                            + "checkpoint completed successfully.",
                    tableConfig.getTableName(),
                    pendingInstants.size(),
                    pendingInstants);
        }
        if (writeClientProvider != null) {
            writeClientProvider.close();
        }
    }

    private List<HudiCommitInfo> flatten(List<HudiAggregatedCommitInfo> aggregatedCommitInfos) {
        return aggregatedCommitInfos.stream()
                .filter(Objects::nonNull)
                .flatMap(aggregatedCommitInfo -> aggregatedCommitInfo.getCommitInfos().stream())
                .filter(Objects::nonNull)
                .filter(commitInfo -> commitInfo.getInstantTime() != null)
                .collect(Collectors.toList());
    }

    private void commitInstant(HudiCommitInfo commitInfo, HoodieTimeline activeTimeline) {
        String instantTime = commitInfo.getInstantTime();
        if (activeTimeline.filterCompletedInstants().containsInstant(instantTime)) {
            log.info(
                    "The hudi instant [{}] of table [{}] is already committed, skip committing it for checkpoint [{}].",
                    instantTime,
                    tableConfig.getTableName(),
                    commitInfo.getCheckpointId());
            pendingInstants.remove(instantTime);
            return;
        }
        if (!activeTimeline.containsInstant(instantTime)) {
            throw new HudiConnectorException(
                    HudiErrorCode.COMMIT_INSTANT_FAILED,
                    String.format(
                            "The hudi instant [%s] of table [%s] for checkpoint [%d] is not on the active timeline anymore, "
                                    + "it was rolled back before it could be committed. Please check whether the writer heartbeat "
                                    + "expired, for example because the writer task was stuck for longer than the heartbeat timeout.",
                            instantTime, tableConfig.getTableName(), commitInfo.getCheckpointId()));
        }
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient =
                writeClientProvider.getOrCreateClient();
        writeClient.commit(instantTime, commitInfo.getWriteStatuses(), Option.empty());
        pendingInstants.remove(instantTime);
        log.info(
                "Commit hudi instant [{}] of table [{}] for checkpoint [{}] success.",
                instantTime,
                tableConfig.getTableName(),
                commitInfo.getCheckpointId());
    }

    private void rollbackInstant(HudiCommitInfo commitInfo, HoodieTimeline activeTimeline)
            throws IOException {
        String instantTime = commitInfo.getInstantTime();
        if (!activeTimeline.containsInstant(instantTime)
                || activeTimeline.filterCompletedInstants().containsInstant(instantTime)) {
            pendingInstants.remove(instantTime);
            return;
        }
        log.warn(
                "Roll back the hudi instant [{}] of table [{}] for the aborted checkpoint [{}].",
                instantTime,
                tableConfig.getTableName(),
                commitInfo.getCheckpointId());
        writeClientProvider.getOrCreateClient().rollback(instantTime);
        pendingInstants.remove(instantTime);
    }

    private HoodieTimeline reloadActiveTimeline() {
        return metaClient.reloadActiveTimeline();
    }
}
