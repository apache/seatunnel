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

package org.apache.seatunnel.engine.server.application;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.AbstractJobEnvironment;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;

/** Builds and submits one job on the application master; the caller owns the cluster lifecycle. */
public final class ApplicationJobExecutionEnvironment extends AbstractJobEnvironment {
    private final SeaTunnelServer server;
    private final Config seaTunnelJobConfig;
    private final long jobId;
    private final Long restoreSourceJobId;
    private boolean executed;

    public ApplicationJobExecutionEnvironment(
            JobConfig jobConfig,
            Config seaTunnelJobConfig,
            SeaTunnelServer server,
            long jobId,
            Long restoreSourceJobId) {
        super(jobConfig, restoreSourceJobId == null ? RestoreMode.NONE : RestoreMode.CHECKPOINT);
        this.server = server;
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.jobId = jobId;
        this.restoreSourceJobId = restoreSourceJobId;
        jobConfig.setJobContext(new JobContext(jobId));
    }

    /** Loads the job parser and, when requested, the previous application's checkpoint. */
    @Override
    protected MultipleTableJobConfigParser getJobConfigParser() {
        List<JobPipelineCheckpointData> checkpoints = Collections.emptyList();
        if (restoreMode.isRestore()) {
            checkpoints =
                    server.getCheckpointService()
                            .getLatestCheckpointData(
                                    String.valueOf(restoreSourceJobId), restoreMode);
            if (checkpoints == null || checkpoints.isEmpty()) {
                throw new IllegalArgumentException(
                        "No checkpoint found for jobId="
                                + jobId
                                + ", restoreMode="
                                + restoreMode
                                + ", restoreSourceJobId="
                                + restoreSourceJobId);
            }
        }
        return new MultipleTableJobConfigParser(
                seaTunnelJobConfig,
                idGenerator,
                jobConfig,
                commonPluginJars,
                restoreMode.isRestore(),
                checkpoints,
                server.getSeaTunnelConfig().getEngineConfig().getMetadataConfig());
    }

    /** Builds the DAG using the application distribution's local connector and plugin jars. */
    @Override
    public LogicalDag getLogicalDag() {
        ImmutablePair<List<Action>, Set<URL>> parsed =
                getJobConfigParser().parse(server.getClassLoaderService());
        actions.addAll(parsed.getLeft());
        jarUrls.addAll(commonPluginJars);
        jarUrls.addAll(parsed.getRight());
        actions.forEach(
                action ->
                        addCommonPluginJarsToAction(
                                action, new HashSet<>(commonPluginJars), Collections.emptySet()));
        return getLogicalDagGenerator().generate();
    }

    /**
     * Submits the job and returns its native terminal result, without waiting for workers or
     * releasing resources.
     *
     * @param cancellation caller-owned signal; completing it cancels the actual job, including when
     *     submission is still pending. Canceling the returned future alone does not cancel the job.
     */
    public CompletableFuture<JobResult> execute(CompletableFuture<Void> cancellation) {
        if (executed) {
            throw new IllegalStateException("An application job has already been submitted");
        }
        executed = true;
        LogicalDag dag = getLogicalDag();
        if (cancellation.isDone()) {
            throw new CancellationException("Application stopped before job submission");
        }
        JobImmutableInformation job =
                new JobImmutableInformation(
                        jobId,
                        jobConfig.getName(),
                        restoreMode,
                        restoreSourceJobId,
                        server.getNodeEngine().getSerializationService(),
                        dag,
                        new ArrayList<>(jarUrls),
                        new ArrayList<>(connectorJarIdentifiers));
        CoordinatorService coordinator = server.getCoordinatorService();
        CompletableFuture<Void> submitted =
                coordinator.submitJob(
                        jobId, server.getNodeEngine().toData(job), job.isStartWithSavePoint());
        CompletableFuture<JobResult> result =
                new CompletableFuture<>(
                        submitted.thenCompose(ignored -> coordinator.waitForJobComplete(jobId)));
        // Cancellation must follow submission acknowledgement: cancelJob is a no-op before the
        // coordinator registers the job. The returned result still tracks native job termination.
        cancellation
                .thenCompose(ignored -> submitted)
                .thenCompose(ignored -> coordinator.cancelJob(jobId))
                .whenComplete(
                        (ignored, failure) -> {
                            if (failure != null) {
                                result.completeExceptionally(failure);
                            }
                        });
        return result;
    }
}
