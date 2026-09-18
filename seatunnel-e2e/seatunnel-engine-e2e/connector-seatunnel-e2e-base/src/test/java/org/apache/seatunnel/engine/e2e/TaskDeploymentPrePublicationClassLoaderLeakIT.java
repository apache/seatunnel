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
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;
import org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptySet;

/**
 * Regression test for the second failure site closed by <a
 * href="https://github.com/apache/seatunnel/pull/11812">#11812</a> ("[Fix][Zeta] Release
 * classloaders after failed task deployment", issue <a
 * href="https://github.com/apache/seatunnel/issues/11808">#11808</a>).
 *
 * <p>{@code TaskExecutionService#deployTask} acquires one classloader reference per task while it
 * deserializes the tasks of a {@code TaskGroup}. Ownership of those references is only handed over
 * to the worker's regular bookkeeping when {@code deployLocalTask} publishes the {@link
 * TaskGroupContext} into {@code executionContexts}; from then on they are released by {@code
 * TaskGroupExecutionTracker#taskDone} when the group completes. Before #11812 a deployment could
 * fail at two distinct points without releasing what it had already acquired:
 *
 * <ul>
 *   <li><b>Site A</b>: a task fails to deserialize inside {@code deployTask}'s per-task loop,
 *       before the {@code TaskGroup} is constructed. {@code deployTask} itself throws and returns
 *       {@code TaskDeployState.failed(...)} synchronously; #11812 releases the references from
 *       {@code deployTask}'s own {@code catch (Throwable)}. That site is covered end to end by
 *       {@code TaskDeploymentClassLoaderLeakIT} (#12028), which poisons the serialized payload of
 *       the last task in a group so that it cannot be deserialized.
 *   <li><b>Site B</b>: every task deserializes correctly and the {@code TaskGroup} is built, but
 *       {@code deployLocalTask} then fails before {@code executionContexts.put(...)} publishes the
 *       context. {@code deployLocalTask} never throws: it completes its result future
 *       exceptionally, so {@code deployTask} still returns {@code TaskDeployState.success()} to the
 *       caller synchronously, and the failure only surfaces asynchronously when the future's
 *       completion callback reports {@link ExecutionState#FAILED} to the master through {@code
 *       notifyTaskStatusToMaster}. #11812 releases the references from the {@code
 *       onFailureBeforeContextPublished} callback that {@code deployTask} wires into {@code
 *       deployLocalTask}. Until now this site was only exercised by {@code
 *       TaskExecutionServiceTest#testDeployTaskHandlesFailureBeforeContextPublication}, a unit test
 *       that stubs {@code notifyTaskStatusToMaster} out with Mockito and therefore never runs the
 *       asynchronous report against a master.
 * </ul>
 *
 * <p>This test targets Site B through the real worker-side path on a clustered master + worker pair
 * started with {@link SeaTunnelServerStarter}. The group is deployed through {@link
 * TaskExecutionService#deployTask} exactly as the master's deploy operation invokes it on the
 * worker, the failure is raised by a real {@link Task} lifecycle hook, and the resulting {@code
 * FAILED} state travels through the real {@code NotifyTaskStatusOperation} RPC to the real master.
 * The master's {@code CoordinatorService} answers with {@code JobNotFoundException} because no job
 * is registered for the synthetic job id, which is the production path a worker takes for a task
 * group whose job the master no longer tracks, and the branch that makes {@code
 * notifyTaskStatusToMaster} stop retrying instead of looping forever.
 *
 * <p>Which hook fails matters. {@link Task#init()} is invoked by the worker threads ({@code
 * BlockingWorker#run} and {@code CooperativeTaskWorker}) only after the context has been published,
 * so an {@code init()} failure takes the ordinary post-publication failure path and never reaches
 * Site B. The only per-task hook {@code deployLocalTask} invokes between {@code TaskGroup}
 * construction and publication, in every thread share mode, is {@link
 * Task#setTaskExecutionContext(TaskExecutionContext)}, so {@link ContextInjectionFailureTask} fails
 * there - the same hook #11812's own unit test used.
 *
 * <p>A control deployment of a valid group runs first, proving on the same worker that the acquired
 * reference is owned by the published context while the group runs, is released once the group
 * finishes, and that the completion is reported to the master through the same asynchronous path.
 * That makes the Site B assertions demonstrably meaningful: they observe a reference count and a
 * master-side report that the control deployment has just shown to move.
 */
public class TaskDeploymentPrePublicationClassLoaderLeakIT {

    /**
     * Number of repeated pre-publication deployment failures. Each attempt uses its own job id,
     * jars and {@link TaskGroupLocation}, so a pre-fix leak (one retained reference per task per
     * attempt) would show up as a reference count that never returns to zero and a live classloader
     * count that grows by two on every attempt.
     */
    private static final int FAILED_DEPLOYMENT_ATTEMPTS = 5;

    /**
     * Upper bound for the asynchronous part of a deployment: the completion callback on the
     * worker's executor, the {@code NotifyTaskStatusOperation} round trip to the master and, on a
     * freshly started master, the 1s retry the worker performs while the master is still finishing
     * its job restore. Generous on purpose so the test fails on a real defect, not on CI load.
     */
    private static final long ASYNC_REPORT_TIMEOUT_SECONDS = 60;

    /**
     * Source of distinct job ids for every deployed group, seeded from the wall clock so ids never
     * collide with a previous run of this test in the same JVM.
     */
    private static final AtomicLong JOB_ID_SEQUENCE =
            new AtomicLong(System.currentTimeMillis() * 1000L);

    /**
     * Deploys one valid control group and then {@value #FAILED_DEPLOYMENT_ATTEMPTS} groups that
     * fail before their {@link TaskGroupContext} is published, asserting for every failed attempt
     * that {@code deployTask} reported success synchronously (the Site B signature), that no
     * context was ever published, that every classloader reference the attempt acquired was
     * released before {@code deployTask} returned, and that the failure was afterwards reported to
     * the master as {@code FAILED} through the real notification RPC.
     */
    @Test
    public void testFailureBeforeContextPublicationReleasesClassLoadersAndReportsFailure()
            throws Exception {
        String testClusterName =
                "TaskDeploymentPrePublicationClassLoaderLeakIT_"
                        + "testFailureBeforeContextPublicationReleasesClassLoadersAndReportsFailure";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try (EngineLogCapture engineLog = EngineLogCapture.install()) {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalWorkerNode = workerNode;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalWorkerNode.getCluster().getMembers().size()));

            // deployTask runs on the worker in production (the master issues it as an RPC), so
            // both the control group and the failing groups are deployed against the worker's own
            // TaskExecutionService, and the classloader bookkeeping inspected is the worker's.
            NodeEngine workerNodeEngine = workerNode.node.getNodeEngine();
            SeaTunnelServer workerServer =
                    workerNodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            TaskExecutionService taskExecutionService = workerServer.getTaskExecutionService();
            DefaultClassLoaderService classLoaderService =
                    (DefaultClassLoaderService) workerServer.getClassLoaderService();

            int baselineClassLoaderCount = classLoaderService.queryClassLoaderCount();

            runControlDeployment(
                    taskExecutionService,
                    classLoaderService,
                    workerNodeEngine,
                    engineLog,
                    baselineClassLoaderCount);

            for (int attempt = 0; attempt < FAILED_DEPLOYMENT_ATTEMPTS; attempt++) {
                runOnePrePublicationFailureAttempt(
                        taskExecutionService,
                        classLoaderService,
                        workerNodeEngine,
                        engineLog,
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
     * Deploys a valid single-task group whose task blocks until the test opens its gate, and
     * follows the reference through the successful lifecycle: while the group runs, the published
     * {@link TaskGroupContext} owns the one acquired reference; once the gate opens and the task
     * finishes, {@code taskDone} releases it, the context moves to the finished contexts, and the
     * worker reports {@code FINISHED} to the master, which rejects it with {@code
     * JobNotFoundException} because the synthetic job was never submitted.
     *
     * <p>This is the baseline the Site B assertions are compared against: the same counters and the
     * same log lines, observed on the same worker and master, moving the way a healthy deployment
     * moves them.
     */
    private void runControlDeployment(
            TaskExecutionService taskExecutionService,
            DefaultClassLoaderService classLoaderService,
            NodeEngine nodeEngine,
            EngineLogCapture engineLog,
            int baselineClassLoaderCount)
            throws IOException {
        File controlTaskJar = File.createTempFile("pre-publication-control", ".jar");
        controlTaskJar.deleteOnExit();
        long controlJobId = JOB_ID_SEQUENCE.incrementAndGet();
        long controlTaskId = 1L;
        CountDownLatch completionGate = GatedTask.registerGate(controlJobId);
        try {
            Set<URL> controlTaskJars = Collections.singleton(controlTaskJar.toURI().toURL());
            TaskGroupLocation location = new TaskGroupLocation(controlJobId, 1, 1);
            TaskGroupImmutableInformation taskGroupImmutableInformation =
                    new TaskGroupImmutableInformation(
                            controlJobId,
                            1,
                            TaskGroupType.DEFAULT,
                            location,
                            "control-" + controlJobId,
                            Collections.singletonList(
                                    nodeEngine
                                            .getSerializationService()
                                            .toData(new GatedTask(controlTaskId, controlJobId))),
                            Collections.singletonList(controlTaskJars),
                            Collections.singletonList(emptySet()));

            TaskDeployState taskDeployState =
                    taskExecutionService.deployTask(taskGroupImmutableInformation);

            Assertions.assertTrue(
                    taskDeployState.isSuccess(), "The control deployment must succeed");
            // Ownership was transferred: the published context holds exactly the classloader the
            // service cached for this job, and that classloader carries the one reference
            // deployTask acquired for the task.
            TaskGroupContext publishedContext =
                    taskExecutionService.getActiveExecutionContext(location);
            ClassLoader cachedClassLoader =
                    classLoaderService
                            .queryClassLoaderById(controlJobId, controlTaskJars)
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "The control group's classloader must be cached while the group runs"));
            Assertions.assertSame(
                    cachedClassLoader,
                    publishedContext.getClassLoader(controlTaskId),
                    "The published context must own the classloader acquired during deployment");
            Assertions.assertEquals(
                    1,
                    classLoaderService.queryClassLoaderReferenceCount(
                            controlJobId, controlTaskJars),
                    "A running group must hold exactly the one reference deployTask acquired");
            Assertions.assertEquals(
                    baselineClassLoaderCount + 1,
                    classLoaderService.queryClassLoaderCount(),
                    "A running group must add exactly one live classloader to the service");

            completionGate.countDown();

            Awaitility.await()
                    .atMost(ASYNC_REPORT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertThrows(
                                        TaskGroupContextNotFoundException.class,
                                        () ->
                                                taskExecutionService.getActiveExecutionContext(
                                                        location),
                                        "A finished group must leave the active contexts");
                                assertClassLoaderFullyReleased(
                                        classLoaderService,
                                        "control",
                                        controlJobId,
                                        "control",
                                        controlTaskJars);
                                Assertions.assertEquals(
                                        baselineClassLoaderCount,
                                        classLoaderService.queryClassLoaderCount(),
                                        "The live classloader count must return to baseline once the control group finishes");
                            });
            // Unlike a Site B failure, a group that was published keeps a finished context.
            Assertions.assertNotNull(
                    taskExecutionService.getExecutionContext(location),
                    "A finished group must still be resolvable through its finished context");
            awaitReportedToMaster(engineLog, location, ExecutionState.FINISHED, controlJobId);
        } finally {
            completionGate.countDown();
            GatedTask.unregisterGate(controlJobId);
            controlTaskJar.delete();
        }
    }

    /**
     * Deploys one two-task group in which both tasks deserialize normally, each acquiring its own
     * classloader reference, and the second task then throws from {@link
     * Task#setTaskExecutionContext(TaskExecutionContext)} while {@code deployLocalTask} wires the
     * tasks up, before the {@link TaskGroupContext} is published. Asserts the Site B contract:
     * synchronous success from {@code deployTask}, no published or finished context, both
     * references released before {@code deployTask} returned, no residue in the service's live
     * classloader count, and an asynchronous {@code FAILED} report that reaches the master and
     * terminates on the master's {@code JobNotFoundException}.
     *
     * <p>The classloader assertions need no polling: the release runs inside {@code
     * deployLocalTask}'s catch block, on the deploying thread, before {@code deployTask} returns.
     * Only the report is asynchronous.
     */
    private void runOnePrePublicationFailureAttempt(
            TaskExecutionService taskExecutionService,
            DefaultClassLoaderService classLoaderService,
            NodeEngine nodeEngine,
            EngineLogCapture engineLog,
            int attempt,
            int baselineClassLoaderCount)
            throws IOException {
        File validTaskJar = File.createTempFile("pre-publication-" + attempt + "-valid", ".jar");
        File failingTaskJar =
                File.createTempFile("pre-publication-" + attempt + "-failing", ".jar");
        validTaskJar.deleteOnExit();
        failingTaskJar.deleteOnExit();
        try {
            Set<URL> validTaskJars = Collections.singleton(validTaskJar.toURI().toURL());
            Set<URL> failingTaskJars = Collections.singleton(failingTaskJar.toURI().toURL());
            long attemptJobId = JOB_ID_SEQUENCE.incrementAndGet();
            TaskGroupLocation location = new TaskGroupLocation(attemptJobId, 1, 1);
            String failureMessage =
                    "simulated failure before TaskGroupContext publication, attempt " + attempt;

            TaskGroupImmutableInformation taskGroupImmutableInformation =
                    new TaskGroupImmutableInformation(
                            attemptJobId,
                            1,
                            TaskGroupType.DEFAULT,
                            location,
                            "pre-publication-failure-" + attempt,
                            Arrays.asList(
                                    nodeEngine
                                            .getSerializationService()
                                            .toData(new PassiveTask(1L)),
                                    // Deserializes fine, then throws from setTaskExecutionContext
                                    // inside deployLocalTask, after both classloaders were acquired
                                    // and the TaskGroup was constructed.
                                    nodeEngine
                                            .getSerializationService()
                                            .toData(
                                                    new ContextInjectionFailureTask(
                                                            2L, failureMessage))),
                            Arrays.asList(validTaskJars, failingTaskJars),
                            Arrays.asList(emptySet(), emptySet()));

            TaskDeployState taskDeployState =
                    taskExecutionService.deployTask(taskGroupImmutableInformation);

            // Site B signature: the deploying caller is told the deployment succeeded, because
            // deployLocalTask reports its failure through the result future instead of throwing.
            Assertions.assertTrue(
                    taskDeployState.isSuccess(),
                    "Attempt "
                            + attempt
                            + " must be reported as a successful deployment: a failure before "
                            + "context publication only surfaces asynchronously");
            Assertions.assertThrows(
                    TaskGroupContextNotFoundException.class,
                    () -> taskExecutionService.getActiveExecutionContext(location),
                    "A group that failed before publication must never appear as active");
            Assertions.assertThrows(
                    TaskGroupContextNotFoundException.class,
                    () -> taskExecutionService.getExecutionContext(location),
                    "A group that failed before publication must not appear as finished either");
            assertClassLoaderFullyReleased(
                    classLoaderService, "attempt " + attempt, attemptJobId, "valid", validTaskJars);
            assertClassLoaderFullyReleased(
                    classLoaderService,
                    "attempt " + attempt,
                    attemptJobId,
                    "failing",
                    failingTaskJars);
            Assertions.assertEquals(
                    baselineClassLoaderCount,
                    classLoaderService.queryClassLoaderCount(),
                    "Total live classloader count must return to baseline after attempt "
                            + attempt
                            + "; growth here means a previous attempt's classloaders leaked");

            awaitReportedToMaster(engineLog, location, ExecutionState.FAILED, attemptJobId);
            Assertions.assertTrue(
                    engineLog.contains(
                            TaskExecutionService.class.getName(),
                            "Task " + location + " complete with error",
                            failureMessage),
                    "The error reported for attempt "
                            + attempt
                            + " must be the injected pre-publication failure");
        } finally {
            validTaskJar.delete();
            failingTaskJar.delete();
        }
    }

    /**
     * Waits until the asynchronous completion of the group with the given location has been
     * observed at both ends of the real notification path: the worker completed the group with the
     * expected state and handed it to {@code notifyTaskStatusToMaster}, the master's {@code
     * CoordinatorService} received that state for that location, and the worker stopped retrying
     * once the master answered that the synthetic job is not running.
     */
    private static void awaitReportedToMaster(
            EngineLogCapture engineLog,
            TaskGroupLocation location,
            ExecutionState expectedState,
            long jobId) {
        Awaitility.await()
                .atMost(ASYNC_REPORT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    engineLog.contains(
                                            TaskExecutionService.class.getName(),
                                            "Task "
                                                    + location
                                                    + " complete with state "
                                                    + expectedState),
                                    "The worker must complete "
                                            + location
                                            + " with "
                                            + expectedState);
                            Assertions.assertTrue(
                                    engineLog.contains(
                                            CoordinatorService.class.getName(),
                                            "Received task end from execution "
                                                    + location
                                                    + ", state "
                                                    + expectedState),
                                    "The master must receive "
                                            + expectedState
                                            + " for "
                                            + location
                                            + " through NotifyTaskStatusOperation");
                            Assertions.assertTrue(
                                    engineLog.contains(
                                            TaskExecutionService.class.getName(),
                                            "can't find job",
                                            "Job " + jobId + " not running"),
                                    "The worker must stop retrying the report for job "
                                            + jobId
                                            + " once the master answers JobNotFoundException");
                        });
    }

    /**
     * Asserts that the reference acquired for one task's jars was released back to zero and that,
     * with classloader cache mode disabled, the classloader instance itself was evicted from {@link
     * DefaultClassLoaderService}'s cache rather than merely decremented and kept.
     */
    private static void assertClassLoaderFullyReleased(
            DefaultClassLoaderService classLoaderService,
            String attemptLabel,
            long jobId,
            String taskLabel,
            Set<URL> taskJars) {
        Assertions.assertEquals(
                0,
                classLoaderService.queryClassLoaderReferenceCount(jobId, taskJars),
                "Classloader reference acquired for the "
                        + taskLabel
                        + " task in "
                        + attemptLabel
                        + " must be released");
        Assertions.assertFalse(
                classLoaderService.queryClassLoaderById(jobId, taskJars).isPresent(),
                "Classloader created for the "
                        + taskLabel
                        + " task in "
                        + attemptLabel
                        + " must be evicted once its last reference is released");
    }

    /**
     * Builds the per-node engine config for this test: a unique cluster name so parallel test runs
     * do not collide, classloader cache mode disabled so a fully released classloader is actually
     * evicted (making eviction a second leak signal next to the reference count), blocking thread
     * share mode so {@link GatedTask} may block inside {@code call()} without stalling a shared
     * cooperative worker thread, and the embedded HTTP server disabled since this test never calls
     * the REST API.
     */
    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().setClassloaderCacheMode(false);
        seaTunnelConfig.getEngineConfig().setTaskExecutionThreadShareMode(ThreadShareMode.OFF);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    /**
     * Minimal valid {@link Task} standing in for the "earlier task" of a multi-task group. It
     * deserializes normally so its classloader reference is acquired, and its own {@code
     * setTaskExecutionContext} succeeds; {@code call()} is never reached because the sibling task
     * fails before the group is published.
     */
    private static class PassiveTask implements Task {

        private final Long taskId;

        PassiveTask(Long taskId) {
            this.taskId = taskId;
        }

        /** Never invoked in this test; the group fails before any task starts executing. */
        @Override
        public ProgressState call() {
            return ProgressState.DONE;
        }

        @Override
        public Long getTaskID() {
            return taskId;
        }
    }

    /**
     * {@link Task} that deserializes normally but throws from {@link
     * #setTaskExecutionContext(TaskExecutionContext)}, the hook {@code deployLocalTask} invokes for
     * every task after the {@code TaskGroup} exists and before the {@link TaskGroupContext} is
     * published. Failing here, rather than in {@link #init()} (which the worker threads only call
     * after publication), is what places the failure at Site B.
     */
    private static class ContextInjectionFailureTask implements Task {

        private final Long taskId;

        /** Distinct per attempt so the report observed at the master can be tied to the attempt. */
        private final String failureMessage;

        ContextInjectionFailureTask(Long taskId, String failureMessage) {
            this.taskId = taskId;
            this.failureMessage = failureMessage;
        }

        @Override
        public void setTaskExecutionContext(TaskExecutionContext taskExecutionContext) {
            throw new IllegalStateException(failureMessage);
        }

        /** Never invoked in this test; the group fails before any task starts executing. */
        @Override
        public ProgressState call() {
            return ProgressState.DONE;
        }

        @Override
        public Long getTaskID() {
            return taskId;
        }
    }

    /**
     * Valid {@link Task} for the control deployment. Its {@code call()} blocks on a gate the test
     * owns, so the test can inspect the worker's bookkeeping while the group is provably still
     * running, then open the gate to let the group finish through the ordinary completion path.
     *
     * <p>The gate registry is static because the task instance the worker executes is a
     * deserialized copy of the one the test built; the connector classloader delegates {@code
     * org.apache.seatunnel.} classes to its parent, so both copies share this class and its
     * statics.
     */
    private static class GatedTask implements Task {

        /** Gates keyed by the job id of the control group that owns them. */
        private static final ConcurrentMap<Long, CountDownLatch> GATES = new ConcurrentHashMap<>();

        private final Long taskId;

        /** Job id of the owning group; selects the gate this task blocks on. */
        private final long gateKey;

        GatedTask(Long taskId, long gateKey) {
            this.taskId = taskId;
            this.gateKey = gateKey;
        }

        static CountDownLatch registerGate(long gateKey) {
            CountDownLatch gate = new CountDownLatch(1);
            GATES.put(gateKey, gate);
            return gate;
        }

        static void unregisterGate(long gateKey) {
            GATES.remove(gateKey);
        }

        /**
         * Blocks until the test opens the gate, then finishes. Runs on a dedicated blocking worker
         * thread because the test forces {@link ThreadShareMode#OFF}; an interrupt from worker
         * shutdown ends the task the same way it ends any blocking connector task.
         */
        @Override
        public ProgressState call() throws Exception {
            CountDownLatch gate = GATES.get(gateKey);
            if (gate != null) {
                gate.await();
            }
            return ProgressState.DONE;
        }

        @Override
        public Long getTaskID() {
            return taskId;
        }
    }

    /**
     * Captures what the worker's {@link TaskExecutionService} and the master's {@link
     * CoordinatorService} log, so the asynchronous report can be observed at both ends of the real
     * RPC without stubbing anything. The engine routes Hazelcast's {@code ILogger} to Log4j2, and
     * master and worker share this JVM's Log4j2 context, so a single appender sees both nodes.
     * Dedicated {@code INFO}-level logger configurations are registered for the two loggers so the
     * assertions do not depend on the level the surrounding Log4j2 configuration happens to give
     * them; they stay additive, so the regular console output is unchanged, and they are removed
     * again by {@link #close()}.
     */
    private static final class EngineLogCapture extends AbstractAppender implements AutoCloseable {

        private static final String[] CAPTURED_LOGGERS = {
            TaskExecutionService.class.getName(), CoordinatorService.class.getName()
        };

        private final LoggerContext loggerContext;

        /** One entry per captured event: logger name, message and the cause chain, if any. */
        private final List<String> lines = new CopyOnWriteArrayList<>();

        private EngineLogCapture(LoggerContext loggerContext) {
            super(
                    "TaskDeploymentPrePublicationClassLoaderLeakIT-capture",
                    null,
                    PatternLayout.createDefaultLayout(),
                    false,
                    Property.EMPTY_ARRAY);
            this.loggerContext = loggerContext;
        }

        static EngineLogCapture install() {
            LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
            EngineLogCapture capture = new EngineLogCapture(loggerContext);
            capture.start();
            Configuration configuration = loggerContext.getConfiguration();
            for (String loggerName : CAPTURED_LOGGERS) {
                LoggerConfig loggerConfig = new LoggerConfig(loggerName, Level.INFO, true);
                loggerConfig.addAppender(capture, Level.INFO, null);
                configuration.addLogger(loggerName, loggerConfig);
            }
            loggerContext.updateLoggers();
            return capture;
        }

        @Override
        public void append(LogEvent event) {
            StringBuilder line =
                    new StringBuilder(event.getLoggerName())
                            .append(' ')
                            .append(event.getMessage().getFormattedMessage());
            for (Throwable thrown = event.getThrown(); thrown != null; thrown = thrown.getCause()) {
                line.append(" | ").append(thrown);
            }
            lines.add(line.toString());
        }

        /**
         * Whether the given logger has emitted an event whose message or cause chain contains all
         * of the given fragments.
         */
        boolean contains(String loggerName, String... fragments) {
            String loggerPrefix = loggerName + " ";
            return lines.stream()
                    .anyMatch(
                            line ->
                                    line.startsWith(loggerPrefix)
                                            && Arrays.stream(fragments).allMatch(line::contains));
        }

        @Override
        public void close() {
            Configuration configuration = loggerContext.getConfiguration();
            for (String loggerName : CAPTURED_LOGGERS) {
                configuration.removeLogger(loggerName);
            }
            loggerContext.updateLoggers();
            stop();
        }
    }
}
