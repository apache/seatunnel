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

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngine;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptySet;

/**
 * Regression test for the classloader leak on failed task deployment fixed by <a
 * href="https://github.com/apache/seatunnel/pull/11812">#11812</a> ("[Fix][Zeta] Release
 * classloaders after failed task deployment"). Before that fix, {@code
 * TaskExecutionService#deployTask} acquired one classloader reference per task in a {@code
 * TaskGroup} before deserializing that task; if a later task in the same group failed to
 * deserialize, the references already acquired for the earlier tasks in that same attempt were
 * silently dropped instead of released, so the reference count leaked further on every failed
 * deployment attempt without ever being reclaimed.
 *
 * <p>This is complementary to {@link org.apache.seatunnel.engine.e2e.classloader.ClassLoaderITBase}
 * and its subclasses, which already prove that classloader/thread counts stay bounded across
 * repeated SUCCESSFUL job restarts (submit, run to completion, resubmit). Those tests never
 * exercise a failed deployment, so they cannot see this bug. This test targets the complementary
 * gap: repeated FAILED deployment attempts within a single multi-task {@code TaskGroup}, which is
 * exactly the circumstance #11812 fixed. Constructing that failure on demand needs a
 * deliberately-broken task definition, which is easiest with white-box control, so this test - like
 * {@code TaskExecutionServiceTest} in seatunnel-engine-server - calls {@link
 * TaskExecutionService#deployTask} directly instead of going through a client job submission. It
 * differs from that unit test by running on a real clustered worker node (matching the {@code
 * SeaTunnelServerStarter}-based house style of {@link SplitClusterPendingJobLifecycleFailoverIT})
 * and by repeating the failure {@value #FAILED_DEPLOYMENT_ATTEMPTS} times to prove the leak does
 * not reappear or accumulate across attempts, which the single-shot unit test does not cover.
 */
public class TaskDeploymentClassLoaderLeakIT {

    /**
     * Number of repeated failed deployment attempts. Matches the iteration count {@code
     * ClassLoaderITBase} uses for its repeated-restart assertions, so a pre-fix linear leak (one
     * extra retained classloader per attempt) would be unambiguous well before the loop ends.
     */
    private static final int FAILED_DEPLOYMENT_ATTEMPTS = 10;

    /**
     * Reproduces #11812 by repeatedly deploying a 3-task {@code TaskGroup} in which the first two
     * tasks deserialize normally - each acquiring its own classloader reference, standing in for
     * the "earlier tasks" in the bug report - while the third task's serialized payload is not a
     * {@code Task} at all, so converting it throws a {@code ClassCastException} inside {@code
     * deployTask}'s deserialization loop, standing in for the "later task" whose deserialization
     * fails. This is the same mechanism #11812's own accompanying unit test uses ({@code
     * TaskExecutionServiceTest#testDeployTaskReleasesClassLoadersWhenDeserializationFails}); it is
     * reused here because it is a genuine deserialization failure - not a synthetically thrown
     * exception - and is the only production-realistic way to force a specific task in a group to
     * fail deserialization on demand.
     *
     * <p>Each attempt uses its own fresh jobId, per-task jars and {@link TaskGroupLocation}, so a
     * live classloader still present after an attempt can only be explained by that attempt's own
     * references leaking, not by state shared with a previous or concurrent attempt. Classloader
     * cache mode is disabled (unlike the engine's default) so a fully released classloader is
     * actually evicted from {@link DefaultClassLoaderService}'s cache instead of merely having its
     * reference count decremented while remaining cached forever; this makes the service's total
     * live classloader count ({@link DefaultClassLoaderService#queryClassLoaderCount()}) a second,
     * coarser leak signal on top of the exact per-attempt reference count, mirroring the
     * bounded-growth style of assertion {@code ClassLoaderITBase} uses for the repeated-restart
     * scenario. No daemon-thread assertion is made here: unlike a job that actually runs, a {@code
     * TaskGroup} that fails during deserialization never reaches {@code taskGroup.init()}, so no
     * sink/source thread is ever spawned on this failure path.
     */
    @Test
    public void testFailedMultiTaskDeploymentDoesNotLeakClassLoaders() throws IOException {
        String testClusterName =
                "TaskDeploymentClassLoaderLeakIT_"
                        + "testFailedMultiTaskDeploymentDoesNotLeakClassLoaders";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;

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

            // deployTask runs on the worker in production (the master issues it as an RPC), so
            // the failure must be reproduced against the worker's own TaskExecutionService.
            NodeEngine workerNodeEngine = workerNode.node.getNodeEngine();
            SeaTunnelServer workerServer =
                    workerNodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            TaskExecutionService taskExecutionService = workerServer.getTaskExecutionService();
            DefaultClassLoaderService classLoaderService =
                    (DefaultClassLoaderService) workerServer.getClassLoaderService();

            int baselineClassLoaderCount = classLoaderService.queryClassLoaderCount();

            for (int attempt = 0; attempt < FAILED_DEPLOYMENT_ATTEMPTS; attempt++) {
                runOneFailedDeploymentAttempt(
                        taskExecutionService,
                        classLoaderService,
                        workerNodeEngine,
                        attempt,
                        baselineClassLoaderCount);
            }
        } finally {
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    /**
     * Builds and deploys one intentionally-failing 3-task {@code TaskGroup} and asserts that every
     * classloader reference it acquired was released, and that the service's total live classloader
     * count returned to the pre-loop baseline - proving that this attempt leaves no residue for the
     * next attempt to accumulate on top of.
     *
     * <p>{@code deployTask}'s deserialization loop is synchronous, and the fix releases leaked
     * references from within the same catch block before returning, so every assertion below
     * observes state immediately after {@code deployTask} returns with no polling required.
     */
    private void runOneFailedDeploymentAttempt(
            TaskExecutionService taskExecutionService,
            DefaultClassLoaderService classLoaderService,
            NodeEngine nodeEngine,
            int attempt,
            int baselineClassLoaderCount)
            throws IOException {
        File firstTaskJar = File.createTempFile("deploy-failure-" + attempt + "-first", ".jar");
        File secondTaskJar = File.createTempFile("deploy-failure-" + attempt + "-second", ".jar");
        File poisonTaskJar = File.createTempFile("deploy-failure-" + attempt + "-poison", ".jar");
        firstTaskJar.deleteOnExit();
        secondTaskJar.deleteOnExit();
        poisonTaskJar.deleteOnExit();
        try {
            Set<URL> firstTaskJars = Collections.singleton(firstTaskJar.toURI().toURL());
            Set<URL> secondTaskJars = Collections.singleton(secondTaskJar.toURI().toURL());
            Set<URL> poisonTaskJars = Collections.singleton(poisonTaskJar.toURI().toURL());
            // Distinct per attempt so a released reference from this attempt can never be
            // mistaken for state a previous attempt already cleaned up.
            long attemptJobId = System.currentTimeMillis() * 1000 + attempt;
            TaskGroupLocation location = new TaskGroupLocation(attemptJobId, 1, 1);

            TaskGroupImmutableInformation taskGroupImmutableInformation =
                    new TaskGroupImmutableInformation(
                            attemptJobId,
                            1,
                            TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                            location,
                            "testFailedMultiTaskDeploymentDoesNotLeakClassLoaders-" + attempt,
                            Arrays.asList(
                                    nodeEngine
                                            .getSerializationService()
                                            .toData(new NoOpValidTask(1L)),
                                    nodeEngine
                                            .getSerializationService()
                                            .toData(new NoOpValidTask(2L)),
                                    // Not a Task: deploying this throws ClassCastException while
                                    // deserializing the third task, after the first two tasks'
                                    // classloaders have already been acquired above.
                                    nodeEngine.getSerializationService().toData("not a task")),
                            Arrays.asList(firstTaskJars, secondTaskJars, poisonTaskJars),
                            Arrays.asList(emptySet(), emptySet(), emptySet()));

            TaskDeployState taskDeployState =
                    taskExecutionService.deployTask(taskGroupImmutableInformation);

            Assertions.assertFalse(
                    taskDeployState.isSuccess(),
                    "Deployment attempt "
                            + attempt
                            + " must fail: the third task cannot deserialize");
            Assertions.assertThrows(
                    TaskGroupContextNotFoundException.class,
                    () -> taskExecutionService.getActiveExecutionContext(location),
                    "A failed deployment attempt must never publish a TaskGroupContext");
            assertClassLoaderFullyReleased(
                    classLoaderService, attempt, attemptJobId, "first", firstTaskJars);
            assertClassLoaderFullyReleased(
                    classLoaderService, attempt, attemptJobId, "second", secondTaskJars);
            assertClassLoaderFullyReleased(
                    classLoaderService, attempt, attemptJobId, "poison", poisonTaskJars);
            Assertions.assertEquals(
                    baselineClassLoaderCount,
                    classLoaderService.queryClassLoaderCount(),
                    "Total live classloader count must return to baseline after attempt "
                            + attempt
                            + "; growth here means a previous attempt's classloader leaked");
        } finally {
            firstTaskJar.delete();
            secondTaskJar.delete();
            poisonTaskJar.delete();
        }
    }

    /**
     * Asserts that the classloader reference acquired for one task's jars during this attempt was
     * released back to zero, which - with cache mode disabled - also implies the classloader
     * instance itself was evicted from {@link DefaultClassLoaderService}'s cache.
     */
    private void assertClassLoaderFullyReleased(
            DefaultClassLoaderService classLoaderService,
            int attempt,
            long attemptJobId,
            String taskLabel,
            Set<URL> taskJars) {
        Assertions.assertEquals(
                0,
                classLoaderService.queryClassLoaderReferenceCount(attemptJobId, taskJars),
                "Classloader reference acquired for the "
                        + taskLabel
                        + " task in attempt "
                        + attempt
                        + " must be released after the deployment fails");
    }

    /**
     * Builds the per-node engine config for this test: a unique cluster name so parallel test runs
     * do not collide, classloader cache mode disabled so a released classloader is actually evicted
     * rather than staying cached forever (see the test Javadoc), and the embedded HTTP server
     * disabled since this test never calls the REST API.
     */
    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().setClassloaderCacheMode(false);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    /**
     * Minimal valid {@link Task} standing in for an "earlier task" that deserializes successfully
     * in a multi-task {@code TaskGroup}. Its {@code call()} behavior is never exercised: the group
     * always fails during deployment, before {@code taskGroup.init()} or any task execution is ever
     * reached.
     */
    private static class NoOpValidTask implements Task {

        private final Long taskId;

        NoOpValidTask(Long taskId) {
            this.taskId = taskId;
        }

        /** Never invoked in this test; deployment always fails before execution starts. */
        @Override
        public ProgressState call() {
            return ProgressState.DONE;
        }

        @Override
        public Long getTaskID() {
            return taskId;
        }
    }
}
