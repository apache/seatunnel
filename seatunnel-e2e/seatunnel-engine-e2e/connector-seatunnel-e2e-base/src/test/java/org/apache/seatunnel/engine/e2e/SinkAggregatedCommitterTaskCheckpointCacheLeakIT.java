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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.task.SinkAggregatedCommitterTask;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Regression test for the checkpoint-cache memory leak fixed by <a
 * href="https://github.com/apache/seatunnel/pull/10189">#10189</a> ("[Fix][Zeta] Fix memory leak in
 * SinkAggregatedCommitterTask"), reported in <a
 * href="https://github.com/apache/seatunnel/issues/10188">#10188</a>: a production deployment
 * running 25 concurrent MySQL-CDC-to-Iceberg streaming jobs with a 5-second checkpoint interval saw
 * heap grow from near-zero to 18.6 GiB over about 3 days, eventually crashing with {@code
 * OutOfMemoryError}.
 *
 * <p>Before the fix, {@code SinkAggregatedCommitterTask#notifyCheckpointComplete} and {@code
 * #notifyCheckpointAborted} removed the completed/aborted checkpoint's entry from {@code
 * checkpointCommitInfoMap} but never from the sibling {@code commitInfoCache} or {@code
 * checkpointBarrierCounter} maps, both keyed by checkpoint id. In a long-running streaming job
 * those two maps accumulate one stale entry per checkpoint cycle, forever, for the life of the job
 * — exactly the shape of the field-reported leak.
 *
 * <p>The fix (two {@code .remove(key)} calls) already ships with a thorough single-JVM unit test
 * ({@code SinkAggregatedCommitterTaskTest}) that drives a directly-instantiated task through mocked
 * checkpoint callbacks and asserts the maps empty out. What that test cannot prove is the actual
 * production scenario: a real streaming job, on a real cluster, running through many real
 * checkpoint cycles back to back, staying bounded rather than accumulating.
 *
 * <p>This test submits a real streaming job with an aggregated-commit sink ({@code LocalFile} with
 * transactions enabled, which routes through {@code FileSinkAggregatedCommitter}) at a 1-second
 * checkpoint interval, locates the live {@code SinkAggregatedCommitterTask} instance on the worker
 * via {@code TaskExecutionService}'s own execution-context bookkeeping, and repeatedly samples
 * {@code commitInfoCache} and {@code checkpointBarrierCounter} by reflection — the identical
 * field-access technique {@code SinkAggregatedCommitterTaskTest} already uses, just against a live
 * task instead of a directly-instantiated one. Over roughly 20 real checkpoint cycles, a pre-fix
 * regression would grow both maps roughly linearly with the checkpoint count; this test asserts
 * every sample stays at or below a small fixed bound instead, which a fixed-size wait alone could
 * not distinguish from "leaking slowly."
 */
public class SinkAggregatedCommitterTaskCheckpointCacheLeakIT {

    private static final String JOB_CONFIG_FILE =
            "streaming_fake_to_localfile_committer_cache_leak.conf";

    /**
     * Upper bound on the observed size of {@code commitInfoCache}/{@code checkpointBarrierCounter}
     * at any single sample. A steady-state job should carry at most the currently in-flight
     * checkpoint's entry (size 0 or 1); this allows headroom for a brief overlap between one
     * checkpoint's completion and the next one's barrier arriving, without tolerating anything
     * close to the ~20 stale entries a pre-fix run would accumulate over the sampling window below.
     */
    private static final int MAX_BOUNDED_CACHE_SIZE = 3;

    private static final int SAMPLE_COUNT = 20;
    private static final long SAMPLE_INTERVAL_MILLIS = 500L;

    @Test
    public void testCommitInfoCacheStaysBoundedAcrossManyCheckpoints() throws Exception {
        String testClusterName =
                "SinkAggregatedCommitterTaskCheckpointCacheLeakIT_"
                        + "testCommitInfoCacheStaysBoundedAcrossManyCheckpoints";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalWorkerNode = workerNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalWorkerNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(
                    "sinkAggregatedCommitterTaskCheckpointCacheLeakIT_"
                            + "testCommitInfoCacheStaysBoundedAcrossManyCheckpoints");
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(JOB_CONFIG_FILE), jobConfig, masterNodeConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            SeaTunnelServer workerServer =
                    workerNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            TaskExecutionService taskExecutionService = workerServer.getTaskExecutionService();

            SinkAggregatedCommitterTask<?, ?> committerTask =
                    awaitSinkAggregatedCommitterTask(taskExecutionService);

            Field commitInfoCacheField =
                    SinkAggregatedCommitterTask.class.getDeclaredField("commitInfoCache");
            commitInfoCacheField.setAccessible(true);
            Field checkpointBarrierCounterField =
                    SinkAggregatedCommitterTask.class.getDeclaredField("checkpointBarrierCounter");
            checkpointBarrierCounterField.setAccessible(true);

            int maxCommitInfoCacheSize = 0;
            int maxCheckpointBarrierCounterSize = 0;
            for (int sample = 0; sample < SAMPLE_COUNT; sample++) {
                Assertions.assertEquals(
                        JobStatus.RUNNING,
                        clientJobProxy.getJobStatus(),
                        "Job must still be RUNNING while sampling checkpoint caches");

                int commitInfoCacheSize =
                        ((Map<?, ?>) commitInfoCacheField.get(committerTask)).size();
                int checkpointBarrierCounterSize =
                        ((Map<?, ?>) checkpointBarrierCounterField.get(committerTask)).size();
                maxCommitInfoCacheSize = Math.max(maxCommitInfoCacheSize, commitInfoCacheSize);
                maxCheckpointBarrierCounterSize =
                        Math.max(maxCheckpointBarrierCounterSize, checkpointBarrierCounterSize);

                Assertions.assertTrue(
                        commitInfoCacheSize <= MAX_BOUNDED_CACHE_SIZE,
                        String.format(
                                "commitInfoCache grew to %d entries at sample %d/%d; a bounded "
                                        + "steady-state job should never carry more than %d, a "
                                        + "regression here means completed/aborted checkpoints "
                                        + "are no longer being removed",
                                commitInfoCacheSize, sample, SAMPLE_COUNT, MAX_BOUNDED_CACHE_SIZE));
                Assertions.assertTrue(
                        checkpointBarrierCounterSize <= MAX_BOUNDED_CACHE_SIZE,
                        String.format(
                                "checkpointBarrierCounter grew to %d entries at sample %d/%d; a "
                                        + "bounded steady-state job should never carry more than "
                                        + "%d, a regression here means completed/aborted "
                                        + "checkpoints are no longer being removed",
                                checkpointBarrierCounterSize,
                                sample,
                                SAMPLE_COUNT,
                                MAX_BOUNDED_CACHE_SIZE));

                Thread.sleep(SAMPLE_INTERVAL_MILLIS);
            }

            // The sampling window above (20 samples x 500ms = 10s, against a 1s checkpoint
            // interval) spans roughly 10 real checkpoint cycles. A pre-fix regression would have
            // grown both maps to roughly that many stale entries by the last sample; recording
            // the observed maximum makes that comparison explicit in the failure message above
            // and gives a human reviewer a concrete number to sanity-check against
            // MAX_BOUNDED_CACHE_SIZE.
            Assertions.assertTrue(
                    maxCommitInfoCacheSize <= MAX_BOUNDED_CACHE_SIZE
                            && maxCheckpointBarrierCounterSize <= MAX_BOUNDED_CACHE_SIZE,
                    String.format(
                            "Observed maximums over the sampling window: commitInfoCache=%d, "
                                    + "checkpointBarrierCounter=%d",
                            maxCommitInfoCacheSize, maxCheckpointBarrierCounterSize));

            clientJobProxy.cancelJob();
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    /**
     * Waits for the streaming job's {@code SinkAggregatedCommitterTask} to be deployed on the
     * worker and returns the live instance, found by scanning {@link TaskExecutionService}'s own
     * execution-context bookkeeping via reflection (there is no public accessor from a {@code
     * TaskGroupLocation} the client side can compute directly, since the committer task's own task
     * id is an engine-internal implementation detail).
     */
    private static SinkAggregatedCommitterTask<?, ?> awaitSinkAggregatedCommitterTask(
            TaskExecutionService taskExecutionService) throws Exception {
        Field executionContextsField =
                TaskExecutionService.class.getDeclaredField("executionContexts");
        executionContextsField.setAccessible(true);

        SinkAggregatedCommitterTask<?, ?>[] found = new SinkAggregatedCommitterTask<?, ?>[1];
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            @SuppressWarnings("unchecked")
                            ConcurrentMap<TaskGroupLocation, TaskGroupContext> contexts =
                                    (ConcurrentMap<TaskGroupLocation, TaskGroupContext>)
                                            executionContextsField.get(taskExecutionService);
                            for (TaskGroupContext context : contexts.values()) {
                                for (Task task : context.getTaskGroup().getTasks()) {
                                    if (task instanceof SinkAggregatedCommitterTask) {
                                        found[0] = (SinkAggregatedCommitterTask<?, ?>) task;
                                        return;
                                    }
                                }
                            }
                            Assertions.fail("SinkAggregatedCommitterTask not yet deployed");
                        });
        return found[0];
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}
