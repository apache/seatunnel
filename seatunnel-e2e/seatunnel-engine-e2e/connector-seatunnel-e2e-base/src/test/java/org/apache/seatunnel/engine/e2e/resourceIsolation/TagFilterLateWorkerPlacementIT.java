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

package org.apache.seatunnel.engine.e2e.resourceIsolation;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * End-to-end coverage for tag-filtered slot placement when the only worker that matches the job's
 * tag filter joins the cluster after the job has already been queued.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code tag_filter} is a placement
 * contract, not a data-processing feature: the job still produces correct output on the wrong
 * worker, so a regression that drops the filter (for example a refactor that allocates from the
 * unfiltered {@code registerWorker} map, or that reuses a pre-apply attempt from before the
 * matching worker existed) is invisible to every functional assertion. What breaks instead is
 * hardware isolation and capacity accounting: the job runs on a worker that the operator excluded,
 * and the matching worker may stay idle. The contract has two halves, and the test checks both:
 *
 * <ul>
 *   <li>{@code AbstractResourceManager#applyResources} must filter the candidate workers by tag
 *       before allocating, so a job whose filter matches no registered worker can never take a slot
 *       from a non-matching worker;
 *   <li>the master's pending-job scheduler must re-evaluate that queued job when a matching worker
 *       registers, without a client resubmission and without leaving a failed pre-apply attempt
 *       pinned to the non-matching worker.
 * </ul>
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code WorkerTagClusterTest} only asks
 * {@link ResourceManager#workerCount} about the filter and never submits a job, so it cannot see
 * where a job is actually placed. {@code ResourceIsolationIT} submits jobs through a Testcontainers
 * cluster whose members all share one fixed set of attributes, so it asserts only exit codes and
 * can never observe the placement of a job whose filter matches some workers but not others. {@code
 * SplitClusterPendingJobLifecycleFailoverIT} owns the pending-queue mechanics, but it frees
 * capacity by cancelling or savepoint-stopping a running job and never pins a job behind a tag
 * filter.
 *
 * <p>Kept as its own class instead of extending {@code WorkerTagClusterTest}: that test is a
 * per-class cluster smoke test for {@code ResourceManager#workerCount}, while this contract needs a
 * real submitted job, a deliberately non-matching worker with enough fixed slots to host it, and a
 * matching worker that only appears later.
 */
@Slf4j
public class TagFilterLateWorkerPlacementIT {

    /** Job config with {@code tag_filter { team = "team-b" }} and parallelism 2. */
    private static final String CONF_FILE =
            "resource-isolation/streaming_fake_to_console_tag_filtered.conf";

    private static final String MATCHING_TEAM = "team-b";
    private static final String NON_MATCHING_TEAM = "team-a";
    private static final String MASTER_TEAM = "team-master";

    /**
     * Fixed slots per worker. The non-matching worker is given more than enough capacity for the
     * job, so a {@code PENDING} status can only mean the tag filter excluded it, never that it was
     * too small.
     */
    private static final int WORKER_SLOTS = 4;

    /**
     * One split-enumerator task group plus one chained source/sink task group per parallelism unit
     * (parallelism 2). The Console sink is an {@code AbstractSimpleSink}, so there is no separate
     * aggregated-committer task group.
     */
    private static final int EXPECTED_SLOTS = 3;

    private static final long CLUSTER_TIMEOUT_SECONDS = 60;
    private static final long JOB_TIMEOUT_SECONDS = 120;

    /**
     * Deadline for a single cancellation RPC. {@code ClientJobProxy#cancelJob} joins a future bound
     * to the master, so without this bound a wedged master would block the test thread forever.
     */
    private static final long CANCEL_TIMEOUT_SECONDS = 30;

    /**
     * Deadline for a single job-status read. {@code ClientJobProxy#getJobStatus} synchronously
     * invokes the master, so a lost master would otherwise strand a poll loop before teardown.
     */
    private static final long CLIENT_CALL_TIMEOUT_SECONDS = 30;

    /** How long the queued job must stay PENDING to prove it is not merely slow to start. */
    private static final long PENDING_STABILITY_SECONDS = 6;

    @Test
    public void testJobWaitsForMatchingWorkerAndIsOnlyPlacedOnIt() throws Exception {
        String testClusterName = "TagFilterLateWorkerPlacementIT_" + System.currentTimeMillis();
        int hazelcastPort = TestUtils.getAvailablePort(100);

        HazelcastInstanceImpl master = null;
        HazelcastInstanceImpl nonMatchingWorker = null;
        HazelcastInstanceImpl matchingWorker = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;
        // The cancel and status calls join master-bound futures. Run them on a test-owned daemon
        // executor and await each with a deadline, so a wedged master fails the test instead of
        // stranding it before teardown.
        ExecutorService clientCalls = boundedClientCallExecutor();

        try {
            SeaTunnelConfig masterConfig =
                    getSeaTunnelConfig(testClusterName, hazelcastPort, MASTER_TEAM);
            SeaTunnelConfig nonMatchingWorkerConfig =
                    getSeaTunnelConfig(testClusterName, hazelcastPort, NON_MATCHING_TEAM);
            SeaTunnelConfig matchingWorkerConfig =
                    getSeaTunnelConfig(testClusterName, hazelcastPort, MATCHING_TEAM);

            master = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            nonMatchingWorker =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(nonMatchingWorkerConfig);
            awaitClusterSize(master, 2);
            final HazelcastInstanceImpl masterNode = master;
            // The tag filter is evaluated against the registered WorkerProfile, so the worker must
            // be visible to the resource manager before the job is submitted; otherwise a PENDING
            // job would only prove that the worker had not registered yet.
            awaitWorkerCount(masterNode, NON_MATCHING_TEAM, 1);
            awaitWorkerCount(masterNode, MATCHING_TEAM, 0);

            Common.setDeployMode(DeployMode.CLIENT);
            engineClient =
                    new SeaTunnelClient(
                            clientConfig(
                                    testClusterName,
                                    master.getCluster().getLocalMember().getAddress()));

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("tag-filtered-late-worker");
            job =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CONF_FILE), jobConfig, masterConfig)
                            .execute();
            final ClientJobProxy submittedJob = job;
            long jobId = submittedJob.getJobId();

            // Only a non-matching worker exists and it has enough capacity to host the whole job.
            // The job must stay queued and must not own a single slot on it.
            Awaitility.await()
                    .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .during(PENDING_STABILITY_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        jobStatusWithinDeadline(clientCalls, submittedJob),
                                        "A job whose tag filter matches no registered worker must"
                                                + " stay queued instead of running on a"
                                                + " non-matching worker");
                                Assertions.assertTrue(
                                        assignedSlotsForJob(masterNode, jobId).isEmpty(),
                                        "A queued tag-filtered job must not own any slot: "
                                                + assignedSlotsForJob(masterNode, jobId));
                            });

            // A worker with the matching tag joins later. Registration alone must wake the pending
            // scheduler; no resubmission or other cluster event is needed.
            matchingWorker =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(matchingWorkerConfig);
            awaitClusterSize(masterNode, 3);
            awaitWorkerCount(masterNode, MATCHING_TEAM, 1);
            awaitJobStatus(clientCalls, submittedJob, JobStatus.RUNNING);

            Address nonMatchingWorkerAddress =
                    nonMatchingWorker.getCluster().getLocalMember().getAddress();
            Address matchingWorkerAddress =
                    matchingWorker.getCluster().getLocalMember().getAddress();

            // The job is only scheduled once the matching worker exists, and every slot it owns
            // must be on that worker. This also catches a stale pre-apply attempt from the
            // PENDING phase being reused against the wrong worker after registration.
            Awaitility.await()
                    .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<SlotProfile> slots = assignedSlotsForJob(masterNode, jobId);
                                Assertions.assertEquals(
                                        EXPECTED_SLOTS,
                                        slots.size(),
                                        "Unexpected slot count for the running job: " + slots);
                                Assertions.assertTrue(
                                        slots.stream()
                                                .allMatch(
                                                        slot ->
                                                                matchingWorkerAddress.equals(
                                                                        slot.getWorker())),
                                        "Every slot of a tag-filtered job must be placed on the"
                                                + " matching worker "
                                                + matchingWorkerAddress
                                                + ", but got: "
                                                + slots);
                                Assertions.assertTrue(
                                        slots.stream()
                                                .noneMatch(
                                                        slot ->
                                                                nonMatchingWorkerAddress.equals(
                                                                        slot.getWorker())),
                                        "The non-matching worker must never host this job: "
                                                + slots);
                            });

            // The job stays RUNNING on the matching worker; it must not be torn down or moved once
            // placement happened.
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .during(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        jobStatusWithinDeadline(clientCalls, submittedJob));
                                Assertions.assertEquals(
                                        EXPECTED_SLOTS,
                                        assignedSlotsForJob(masterNode, jobId).size());
                            });

            callWithinDeadline(
                    clientCalls,
                    "cancellation request of job " + jobId,
                    CANCEL_TIMEOUT_SECONDS,
                    () -> {
                        submittedJob.cancelJob();
                        return null;
                    });
            awaitJobStatus(clientCalls, submittedJob, JobStatus.CANCELED);
            Awaitility.await()
                    .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            assignedSlotsForJob(masterNode, jobId).isEmpty(),
                                            "A canceled job must release every slot it owned: "
                                                    + assignedSlotsForJob(masterNode, jobId)));
            job = null;
        } finally {
            // Every teardown step is independently guarded: a failure in one step must not skip the
            // remaining steps, and no teardown exception may mask an assertion failure that is
            // already propagating. The cancellation itself is bounded, so a wedged master cannot
            // keep the created members alive past this method.
            final ClientJobProxy jobToCancel = job;
            cleanupQuietly(
                    "cancel job",
                    () -> {
                        if (jobToCancel != null) {
                            callWithinDeadline(
                                    clientCalls,
                                    "teardown cancellation of job " + jobToCancel.getJobId(),
                                    CANCEL_TIMEOUT_SECONDS,
                                    () -> {
                                        jobToCancel.cancelJob();
                                        return null;
                                    });
                        }
                    });
            final SeaTunnelClient clientToClose = engineClient;
            cleanupQuietly(
                    "close engine client",
                    () -> {
                        if (clientToClose != null) {
                            clientToClose.close();
                        }
                    });
            final HazelcastInstanceImpl matchingWorkerToStop = matchingWorker;
            cleanupQuietly(
                    "shut down matching worker",
                    () -> {
                        if (matchingWorkerToStop != null) {
                            matchingWorkerToStop.shutdown();
                        }
                    });
            final HazelcastInstanceImpl nonMatchingWorkerToStop = nonMatchingWorker;
            cleanupQuietly(
                    "shut down non-matching worker",
                    () -> {
                        if (nonMatchingWorkerToStop != null) {
                            nonMatchingWorkerToStop.shutdown();
                        }
                    });
            final HazelcastInstanceImpl masterToStop = master;
            cleanupQuietly(
                    "shut down master",
                    () -> {
                        if (masterToStop != null) {
                            masterToStop.shutdown();
                        }
                    });
            cleanupQuietly("shut down bounded client-call executor", clientCalls::shutdownNow);
        }
    }

    /** Builds a per-node SeaTunnel config whose Hazelcast member advertises the given team tag. */
    private static SeaTunnelConfig getSeaTunnelConfig(
            String testClusterName, int hazelcastPort, String team) {
        Config hazelcastConfig = Config.loadFromString(hazelcastConfig(hazelcastPort, team));
        hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        // The placement contract does not need the REST endpoint, and disabling it avoids
        // contending for the fixed HTTP port with other test classes.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // A REJECT scheduler would fail the job while no matching worker exists; the contract under
        // test is the WAIT queue, which keeps the job PENDING until capacity appears.
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(WORKER_SLOTS);
        return seaTunnelConfig;
    }

    private static String hazelcastConfig(int hazelcastPort, String team) {
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: "
                + hazelcastPort
                + "\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n"
                + "  member-attributes:\n"
                + "    team:\n"
                + "      type: string\n"
                + "      value: "
                + team
                + "\n";
    }

    /**
     * Points the client at the embedded master instead of the static {@code localhost:5801..}
     * member list in the test {@code hazelcast-client.yaml}: this test's cluster uses an
     * OS-assigned port range so that it cannot collide with other embedded clusters.
     */
    private static ClientConfig clientConfig(String testClusterName, Address masterAddress) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        clientConfig
                .getNetworkConfig()
                .setAddresses(
                        Collections.singletonList(
                                masterAddress.getHost() + ":" + masterAddress.getPort()));
        clientConfig.getNetworkConfig().setConnectionTimeout(10_000);
        return clientConfig;
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitWorkerCount(
            HazelcastInstanceImpl master, String team, int expectedCount) {
        Map<String, String> tagFilter = Collections.singletonMap("team", team);
        Awaitility.await()
                .atMost(CLUSTER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedCount,
                                        resourceManager(master).workerCount(tagFilter),
                                        "Unexpected worker count for tag " + tagFilter));
    }

    private static void awaitJobStatus(
            ExecutorService clientCalls, ClientJobProxy job, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(JOB_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobStatusWithinDeadline(clientCalls, job),
                                        "Unexpected status for job " + job.getJobId()));
    }

    /** Reads the job status with a deadline, so a lost master cannot block the polling thread. */
    private static JobStatus jobStatusWithinDeadline(
            ExecutorService clientCalls, ClientJobProxy job) throws Exception {
        return callWithinDeadline(
                clientCalls,
                "status of job " + job.getJobId(),
                CLIENT_CALL_TIMEOUT_SECONDS,
                job::getJobStatus);
    }

    /**
     * Fixed slots assigned to the job, read from the master's resource manager. Reading the
     * assignment map (rather than the job's in-memory slot map) is what makes the placement
     * observable: the map records the worker address each slot was actually granted on.
     */
    private static List<SlotProfile> assignedSlotsForJob(HazelcastInstanceImpl master, long jobId) {
        return resourceManager(master).getAssignedSlots(Collections.emptyMap()).stream()
                .filter(slot -> slot.getOwnerJobID() == jobId)
                .collect(Collectors.toList());
    }

    private static ResourceManager resourceManager(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getResourceManager();
    }

    /**
     * Creates the daemon executor used to bound master-bound client calls. Daemon threads cannot
     * prevent JVM exit if an invocation ignores interruption, and the test's finally block calls
     * {@link ExecutorService#shutdownNow()} so the pool is always released.
     */
    private static ExecutorService boundedClientCallExecutor() {
        return Executors.newCachedThreadPool(
                runnable -> {
                    Thread thread =
                            new Thread(runnable, "TagFilterLateWorkerPlacementIT-client-call");
                    thread.setDaemon(true);
                    return thread;
                });
    }

    /**
     * Runs one client RPC on the bounded executor and fails once {@code timeoutSeconds} elapse.
     * {@code ClientJobProxy#cancelJob} joins a master-bound future and {@code getJobStatus} invokes
     * the master synchronously, so neither returns on its own when the master is gone. Turning that
     * into a bounded failure keeps the test from hanging and lets teardown run.
     */
    private static <T> T callWithinDeadline(
            ExecutorService executor, String description, long timeoutSeconds, Callable<T> call)
            throws Exception {
        Future<T> future = executor.submit(call);
        try {
            return future.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new AssertionError(
                    "Timed out after " + timeoutSeconds + "s waiting for " + description, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(description + " failed", cause);
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for " + description, e);
        }
    }

    /**
     * Runs one teardown step. Failures are logged instead of thrown, so the remaining steps still
     * run and an assertion failure that is already propagating is preserved.
     */
    private static void cleanupQuietly(String description, Executable cleanup) {
        try {
            cleanup.execute();
        } catch (Throwable e) {
            log.warn("Teardown step '{}' failed; continuing with teardown", description, e);
        }
    }
}
