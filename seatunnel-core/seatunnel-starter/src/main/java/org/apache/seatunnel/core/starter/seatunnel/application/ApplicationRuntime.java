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

package org.apache.seatunnel.core.starter.seatunnel.application;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.config.ApplicationClusterConfig;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one native SeaTunnel job in an isolated master and fixed-size worker cluster.
 *
 * <p>The calling thread owns the application lifecycle. Driver callbacks may report failures from
 * other threads; native job submission and waiting run on a daemon executor so callbacks and
 * cancellation remain observable. Startup has a deployment deadline; streaming job duration is
 * unbounded. Every exit path reclaims allocations, stops native resources, publishes the result,
 * and closes the driver. Cleanup operations share one 90-second budget; a failure is included in
 * the returned outcome rather than hidden behind a successful job result.
 */
@Slf4j
public final class ApplicationRuntime {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 90;

    private ApplicationRuntime() {}

    /**
     * Starts an application using the distribution's engine configuration and blocks until it ends.
     *
     * <p>The driver must be fresh and uninitialized. This method takes ownership even when startup
     * fails. Interruption cancels the application and is restored on the calling thread after
     * cleanup. A VM shutdown hook requests the same cleanup. Separate application processes are
     * required; this entrypoint is not a multi-application service inside one JVM.
     *
     * @param id external platform identity for this application
     * @param specification immutable resolved job content and fixed deployment resources
     * @param driver deployment-specific worker allocator, closed before this method returns
     * @return terminal job and cleanup outcome; operational failures are returned as FAILED
     * @throws NullPointerException if a required argument is null
     */
    public static ApplicationResult run(
            ApplicationId id,
            ApplicationSpecification specification,
            ResourceManagerDriver driver) {
        return new RuntimeContext(id, specification, driver).run(null);
    }

    static ApplicationResult run(
            ApplicationId id,
            ApplicationSpecification specification,
            ResourceManagerDriver driver,
            SeaTunnelConfig config) {
        return new RuntimeContext(id, specification, driver).run(config);
    }

    static ApplicationStatus applicationStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        return status == JobStatus.CANCELED ? ApplicationStatus.CANCELED : ApplicationStatus.FAILED;
    }

    private static final class RuntimeContext implements ResourceManagerContext {
        private final ApplicationId id;
        private final ApplicationSpecification specification;
        private final ResourceManagerDriver driver;
        private final String clusterName = "seatunnel-application-" + UUID.randomUUID();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final List<WorkerRegistration> workers = new ArrayList<>();
        private final List<CompletableFuture<WorkerRegistration>> requests = new ArrayList<>();
        private final CountDownLatch completed = new CountDownLatch(1);
        private final ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-application-job");
                            thread.setDaemon(true);
                            return thread;
                        });
        private volatile boolean closing;
        private volatile SeaTunnelClient client;
        private volatile ClientJobProxy job;
        private HazelcastInstanceImpl master;
        private final List<String> cleanupFailures = new ArrayList<>();
        private String masterAddress;
        private Path jobFile;
        private Future<JobResult> jobFuture;
        private long cleanupDeadline;

        private RuntimeContext(
                ApplicationId id,
                ApplicationSpecification specification,
                ResourceManagerDriver driver) {
            this.id = Objects.requireNonNull(id, "id");
            this.specification = Objects.requireNonNull(specification, "specification");
            this.driver = Objects.requireNonNull(driver, "driver");
        }

        private ApplicationResult run(SeaTunnelConfig suppliedConfig) {
            Thread owner = Thread.currentThread();
            Thread hook =
                    new Thread(
                            () -> {
                                owner.interrupt();
                                try {
                                    completed.await(SHUTDOWN_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                            },
                            "seatunnel-application-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            ApplicationStatus status = ApplicationStatus.FAILED;
            String diagnostics = null;
            boolean interrupted = false;
            try {
                SeaTunnelConfig config =
                        suppliedConfig == null
                                ? ConfigProvider.locateAndGetSeaTunnelConfig()
                                : suppliedConfig;
                long deadline =
                        System.nanoTime()
                                + TimeUnit.MILLISECONDS.toNanos(
                                        specification.getStartupTimeoutMillis());
                ApplicationClusterConfig.configure(
                        config,
                        clusterName,
                        null,
                        specification.getWorkerSpecification().getSlots());
                ApplicationClusterConfig.configureCheckpointRetention(config);
                config.getHazelcastConfig()
                        .getNetworkConfig()
                        .setPort(specification.getOption(ApplicationOptions.MASTER_PORT))
                        .setPortAutoIncrement(specification.getDeployType() == DeployType.YARN);
                String advertisedHost = System.getenv("SEATUNNEL_APPLICATION_MASTER_HOST");
                if (advertisedHost != null && !advertisedHost.trim().isEmpty()) {
                    config.getHazelcastConfig()
                            .getNetworkConfig()
                            .setPublicAddress(
                                    (advertisedHost.contains(":")
                                                    ? "[" + advertisedHost + "]"
                                                    : advertisedHost)
                                            + ":"
                                            + specification.getOption(
                                                    ApplicationOptions.MASTER_PORT));
                }
                master = SeaTunnelServerStarter.createMasterHazelcastInstance(config);
                Address address = master.getCluster().getLocalMember().getAddress();
                masterAddress =
                        (address.getHost().contains(":")
                                        ? "[" + address.getHost() + "]"
                                        : address.getHost())
                                + ":"
                                + address.getPort();
                master.getCluster()
                        .addMembershipListener(
                                new MembershipListener() {
                                    @Override
                                    public void memberAdded(MembershipEvent event) {}

                                    @Override
                                    public void memberRemoved(MembershipEvent event) {
                                        if (event.getMember().isLiteMember()) {
                                            onWorkerTerminated(
                                                    event.getMember().getUuid().toString(),
                                                    "Worker left the application cluster");
                                        }
                                    }
                                });
                driver.initialize(this);
                for (int i = 0; i < specification.getWorkerCount(); i++) {
                    checkFailure();
                    requests.add(driver.requestWorker(specification.getWorkerSpecification()));
                }
                for (CompletableFuture<WorkerRegistration> request : requests) {
                    workers.add(await(request, deadline));
                }
                while (master.getCluster().getMembers().stream()
                                .filter(member -> member.isLiteMember())
                                .count()
                        < specification.getWorkerCount()) {
                    checkFailure();
                    checkDeadline(deadline);
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                SeaTunnelServer server =
                        master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
                ResourceManager slots =
                        await(
                                executor.submit(
                                        () -> server.getCoordinatorService().getResourceManager()),
                                deadline);
                while (slots.workerCount(Collections.emptyMap()) < specification.getWorkerCount()) {
                    checkFailure();
                    checkDeadline(deadline);
                    TimeUnit.MILLISECONDS.sleep(100);
                }
                checkFailure();
                jobFile = Files.createTempFile("seatunnel-application-", ".conf");
                Files.write(jobFile, specification.getJobConfig().getBytes(StandardCharsets.UTF_8));
                jobFuture = executor.submit(() -> executeJob(config));
                // Native execution may be streaming: only startup, not job duration, is bounded.
                JobResult result = await(jobFuture, Long.MAX_VALUE);
                status = applicationStatus(result.getStatus());
                diagnostics = result.getError();
            } catch (InterruptedException e) {
                interrupted = true;
                status = ApplicationStatus.CANCELED;
                diagnostics = "Application process was interrupted";
            } catch (Exception e) {
                Throwable cause =
                        e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
                diagnostics = cause.getClass().getSimpleName() + ": " + cause.getMessage();
                log.error("Application {} failed", id.getId(), cause);
            } finally {
                closing = true;
                cleanupDeadline =
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
                // Clear interruption while stopping resources, then restore the caller's signal.
                interrupted |= Thread.interrupted();
                if (job != null && jobFuture != null && !jobFuture.isDone()) {
                    cleanup("cancel job", job::cancelJob);
                }
                if (jobFuture != null) {
                    jobFuture.cancel(true);
                }
                if (client != null) {
                    cleanup("close job client", client::close);
                }
                executor.shutdownNow();
                for (CompletableFuture<WorkerRegistration> request : requests) {
                    if (!request.isDone()) {
                        request.cancel(true);
                    }
                }
                if (!workers.isEmpty()) {
                    cleanup("release application workers", this::releaseWorkers, 20);
                }
                cleanup("drain application allocations", driver::stopWorkers, 60);
                if (master != null) {
                    cleanup("stop application master", master::shutdown);
                }
                if (jobFile != null) {
                    cleanup("remove staged job configuration", () -> Files.deleteIfExists(jobFile));
                }
                String primaryDiagnostics = diagnostics;
                if (!cleanupFailures.isEmpty()) {
                    status = ApplicationStatus.FAILED;
                    diagnostics = appendCleanupDiagnostics(primaryDiagnostics);
                }
                ApplicationStatus finalStatus = status;
                String finalDiagnostics = diagnostics;
                cleanup(
                        "publish application result",
                        () -> driver.finish(finalStatus, finalDiagnostics));
                cleanup("close resource manager", driver::close, 60);
                if (!cleanupFailures.isEmpty()) {
                    status = ApplicationStatus.FAILED;
                    diagnostics = appendCleanupDiagnostics(primaryDiagnostics);
                }
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                } catch (IllegalStateException ignored) {
                    // The VM is already shutting down.
                }
                completed.countDown();
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return new ApplicationResult(id, status, diagnostics);
        }

        private JobResult executeJob(SeaTunnelConfig config) throws Exception {
            ClientConfig clientConfig = new ClientConfig().setClusterName(clusterName);
            clientConfig.getNetworkConfig().setAddresses(Collections.singletonList(masterAddress));
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(specification.getStartupTimeoutMillis());
            client = new SeaTunnelClient(clientConfig);
            if (closing) {
                client.close();
                throw new IllegalStateException("Application is already stopping");
            }
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            Long restoreJobId = specification.getOption(ApplicationOptions.RESTORE_JOB_ID);
            long jobId = specification.getJobId();
            if (restoreJobId == null) {
                log.info("Application {} submits native job {}", id.getId(), jobId);
                job =
                        client.createExecutionContext(
                                        jobFile.toString(), null, jobConfig, config, jobId)
                                .execute();
            } else {
                // The native parser accepts an empty checkpoint list. An explicit application
                // restore must fail instead of silently executing a fresh job in that case.
                if (client.getJobClient()
                        .getCheckpointData(restoreJobId, RestoreMode.CHECKPOINT)
                        .isEmpty()) {
                    throw new IllegalStateException(
                            "No eligible checkpoint found for source job " + restoreJobId);
                }
                log.info(
                        "Application {} restores native job {} from checkpoint of job {}",
                        id.getId(),
                        jobId,
                        restoreJobId);
                job =
                        client.restoreFromCheckpointExecutionContext(
                                        jobFile.toString(),
                                        null,
                                        jobConfig,
                                        config,
                                        restoreJobId,
                                        jobId)
                                .execute();
            }
            return job.waitForJobCompleteV2();
        }

        private <T> T await(Future<T> future, long deadline) throws Exception {
            while (true) {
                // A durable native result already available wins over a later worker-exit callback.
                if (future.isDone()) {
                    return future.get();
                }
                try {
                    checkFailure();
                } catch (ExecutionException failure) {
                    if (future.isDone()) {
                        return future.get();
                    }
                    throw failure;
                }
                checkDeadline(deadline);
                try {
                    return future.get(100, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // Recheck asynchronous resource-manager failures while the operation is
                    // pending.
                }
            }
        }

        private void checkFailure() throws ExecutionException {
            Throwable error = failure.get();
            if (error != null) {
                throw new ExecutionException(error);
            }
        }

        private void checkDeadline(long deadline) throws TimeoutException {
            if (deadline != Long.MAX_VALUE && System.nanoTime() >= deadline) {
                throw new TimeoutException(
                        "Timed out starting "
                                + specification.getWorkerCount()
                                + " application workers");
            }
        }

        private void releaseWorkers() throws Exception {
            ExecutorService releases =
                    Executors.newFixedThreadPool(
                            Math.min(workers.size(), 32),
                            runnable -> {
                                Thread thread =
                                        new Thread(
                                                runnable, "seatunnel-application-worker-release");
                                thread.setDaemon(true);
                                return thread;
                            });
            try {
                List<Future<?>> pending = new ArrayList<>();
                for (WorkerRegistration worker : workers) {
                    pending.add(
                            releases.submit(
                                    () -> {
                                        driver.releaseWorker(worker);
                                        return null;
                                    }));
                }
                Exception failure = null;
                for (Future<?> release : pending) {
                    try {
                        release.get();
                    } catch (ExecutionException e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            } finally {
                releases.shutdownNow();
            }
        }

        private void cleanup(String action, CleanupAction cleanup) {
            cleanup(action, cleanup, 10);
        }

        private void cleanup(String action, CleanupAction cleanup, long operationTimeoutSeconds) {
            ExecutorService cleanupExecutor =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "seatunnel-application-cleanup");
                                thread.setDaemon(true);
                                return thread;
                            });
            try {
                long remaining =
                        Math.max(
                                1,
                                Math.min(
                                        cleanupDeadline - System.nanoTime(),
                                        TimeUnit.SECONDS.toNanos(operationTimeoutSeconds)));
                cleanupExecutor
                        .submit(
                                () -> {
                                    cleanup.run();
                                    return null;
                                })
                        .get(remaining, TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                Throwable cause =
                        e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
                cleanupFailures.add(
                        action
                                + ": "
                                + cause.getClass().getSimpleName()
                                + ": "
                                + cause.getMessage());
                log.warn("Could not {} for application {}", action, id.getId(), cause);
            } finally {
                cleanupExecutor.shutdownNow();
            }
        }

        private String appendCleanupDiagnostics(String primary) {
            return (primary == null || primary.isEmpty() ? "" : primary + "; ")
                    + "Application cleanup failed: "
                    + String.join("; ", cleanupFailures);
        }

        @Override
        public ApplicationId getApplicationId() {
            return id;
        }

        @Override
        public ApplicationSpecification getSpecification() {
            return specification;
        }

        @Override
        public String getClusterName() {
            return clusterName;
        }

        @Override
        public String getMasterAddress() {
            return masterAddress;
        }

        @Override
        public void onError(Throwable error) {
            if (!closing) {
                failure.compareAndSet(null, error);
            }
        }

        @Override
        public void onWorkerTerminated(String workerId, String diagnostics) {
            onError(
                    new IllegalStateException(
                            "Application worker " + workerId + " terminated: " + diagnostics));
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Exception;
    }
}
