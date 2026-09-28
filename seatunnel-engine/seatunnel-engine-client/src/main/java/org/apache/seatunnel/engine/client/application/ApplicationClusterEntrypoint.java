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

package org.apache.seatunnel.engine.client.application;

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
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.UnsupportedDeployTypeException;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.kubernetes.KubernetesResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.thirdparty.yarn.YarnResourceManagerFactory;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.config.ApplicationClusterConfig;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public final class ApplicationClusterEntrypoint {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 90;

    private ApplicationClusterEntrypoint() {}

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
        return new ApplicationExecution(id, specification, driver).execute(null);
    }

    static ApplicationResult run(
            ApplicationId id,
            ApplicationSpecification specification,
            ResourceManagerDriver driver,
            SeaTunnelConfig config) {
        return new ApplicationExecution(id, specification, driver).execute(config);
    }

    static ApplicationStatus applicationStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        return status == JobStatus.CANCELED ? ApplicationStatus.CANCELED : ApplicationStatus.FAILED;
    }

    /**
     * Cancels a job that completed remote submission after application shutdown started.
     *
     * <p>The caller must publish the submitted proxy before invoking this method. This closes the
     * window where normal cleanup observed no job while the remote submission was still in flight.
     *
     * @param submittedJob remotely submitted native job
     * @param closing whether application cleanup has started
     * @throws IllegalStateException after the submitted job is synchronously canceled
     */
    static void cancelSubmittedJobIfClosing(ClientJobProxy submittedJob, boolean closing) {
        if (closing) {
            submittedJob.cancelJob();
            throw new IllegalStateException("Application stopped while submitting its native job");
        }
    }

    /** Owns every resource created while one application is executing. */
    private static final class ApplicationExecution {
        private final ApplicationId id;
        private final ApplicationSpecification specification;
        private final ResourceManagerDriver driver;
        private final String clusterName = "seatunnel-application-" + UUID.randomUUID();
        private final CountDownLatch completed = new CountDownLatch(1);
        private final ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-application-job");
                            thread.setDaemon(true);
                            return thread;
                        });
        private final ExecutorService cleanupExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-application-cleanup");
                            thread.setDaemon(true);
                            return thread;
                        });
        private volatile boolean closing;
        private volatile SeaTunnelClient client;
        private volatile ClientJobProxy job;
        private HazelcastInstanceImpl master;
        private volatile ApplicationResourceManager resourceManager;
        private final List<String> cleanupFailures = new ArrayList<>();
        private String masterAddress;
        private Path jobFile;
        private Future<JobResult> jobFuture;
        private long cleanupDeadline;

        private ApplicationExecution(
                ApplicationId id,
                ApplicationSpecification specification,
                ResourceManagerDriver driver) {
            this.id = Objects.requireNonNull(id, "id");
            this.specification = Objects.requireNonNull(specification, "specification");
            this.driver = Objects.requireNonNull(driver, "driver");
        }

        private ApplicationResult execute(SeaTunnelConfig suppliedConfig) {
            Thread owner = Thread.currentThread();
            Thread hook = shutdownHook(owner);
            Runtime.getRuntime().addShutdownHook(hook);
            ApplicationResult result =
                    new ApplicationResult(
                            id, ApplicationStatus.FAILED, "Application did not start");
            boolean interrupted = false;
            try {
                result = startAndRun(suppliedConfig);
            } catch (InterruptedException e) {
                interrupted = true;
                result =
                        new ApplicationResult(
                                id,
                                ApplicationStatus.CANCELED,
                                "Application process was interrupted");
            } catch (Exception e) {
                Throwable cause =
                        e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
                result =
                        new ApplicationResult(
                                id,
                                ApplicationStatus.FAILED,
                                cause.getClass().getSimpleName() + ": " + cause.getMessage());
                log.error("Application {} failed", id.getId(), cause);
            } finally {
                // Clear interruption while stopping resources, then restore the caller's signal.
                interrupted |= Thread.interrupted();
                result = closeResources(result);
                completeExecution(hook);
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return result;
        }

        private ApplicationResult startAndRun(SeaTunnelConfig suppliedConfig) throws Exception {
            SeaTunnelConfig config =
                    suppliedConfig == null
                            ? ConfigProvider.locateAndGetSeaTunnelConfig()
                            : suppliedConfig;
            long startupDeadline =
                    System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(
                                    specification.getStartupTimeoutMillis());
            prepareMasterConfiguration(config);
            startCluster(config, startupDeadline);
            return runJob(config);
        }

        private void prepareMasterConfiguration(SeaTunnelConfig config) {
            ApplicationClusterConfig.configure(
                    config, clusterName, null, specification.getWorkerSpecification().getSlots());
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
                                        + specification.getOption(ApplicationOptions.MASTER_PORT));
            }
        }

        private void startCluster(SeaTunnelConfig config, long startupDeadline) throws Exception {
            master =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            config, createResourceManagerFactory());
            Address address = master.getCluster().getLocalMember().getAddress();
            masterAddress = formatAddress(address);
            SeaTunnelServer server =
                    master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            resourceManager =
                    (ApplicationResourceManager)
                            awaitStartup(
                                    executor.submit(
                                            () ->
                                                    server.getCoordinatorService()
                                                            .getResourceManager()),
                                    startupDeadline);
            resourceManager.awaitWorkerRegistration(startupDeadline);
        }

        private ApplicationResult runJob(SeaTunnelConfig config) throws Exception {
            jobFile = Files.createTempFile("seatunnel-application-", ".conf");
            Files.write(jobFile, specification.getJobConfig().getBytes(StandardCharsets.UTF_8));
            jobFuture = executor.submit(() -> submitAndWaitForJob(config));
            // Native execution may be streaming: only startup, not job duration, is bounded.
            JobResult result = awaitJob(jobFuture);
            return new ApplicationResult(
                    id, applicationStatus(result.getStatus()), result.getError());
        }

        private ApplicationResult closeResources(ApplicationResult result) {
            closing = true;
            cleanupDeadline =
                    System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
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
            if (resourceManager != null) {
                cleanup("stop application workers", resourceManager::stopApplicationWorkers, 60);
            }
            if (jobFile != null) {
                cleanup("remove staged job configuration", () -> Files.deleteIfExists(jobFile));
            }

            String primaryDiagnostics = result.getDiagnostics();
            ApplicationStatus finalStatus =
                    cleanupFailures.isEmpty() ? result.getStatus() : ApplicationStatus.FAILED;
            String finalDiagnostics =
                    cleanupFailures.isEmpty()
                            ? primaryDiagnostics
                            : appendCleanupDiagnostics(primaryDiagnostics);
            if (resourceManager == null) {
                cleanup("close resource manager driver", driver::close, 60);
            } else {
                ApplicationStatus publishedStatus = finalStatus;
                String publishedDiagnostics = finalDiagnostics;
                cleanup(
                        "publish application result",
                        () -> resourceManager.finish(publishedStatus, publishedDiagnostics));
            }
            if (!cleanupFailures.isEmpty()) {
                finalStatus = ApplicationStatus.FAILED;
                finalDiagnostics = appendCleanupDiagnostics(primaryDiagnostics);
            }
            if (master != null) {
                cleanupAfterPublication("stop application master", master::shutdown, 60);
            }
            cleanupExecutor.shutdownNow();
            return new ApplicationResult(id, finalStatus, finalDiagnostics);
        }

        private Thread shutdownHook(Thread owner) {
            return new Thread(
                    () -> {
                        owner.interrupt();
                        try {
                            completed.await(SHUTDOWN_TIMEOUT_SECONDS + 5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    },
                    "seatunnel-application-shutdown");
        }

        private void completeExecution(Thread hook) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // The VM is already shutting down.
            }
            completed.countDown();
        }

        private JobResult submitAndWaitForJob(SeaTunnelConfig config) throws Exception {
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
            ClientJobProxy submittedJob;
            if (restoreJobId == null) {
                log.info("Application {} submits native job {}", id.getId(), jobId);
                submittedJob =
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
                submittedJob =
                        client.restoreFromCheckpointExecutionContext(
                                        jobFile.toString(),
                                        null,
                                        jobConfig,
                                        config,
                                        restoreJobId,
                                        jobId)
                                .execute();
            }
            job = submittedJob;
            cancelSubmittedJobIfClosing(submittedJob, closing);
            return submittedJob.waitForJobCompleteV2();
        }

        private <T> T awaitStartup(Future<T> future, long deadline) throws Exception {
            while (!future.isDone()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    future.cancel(true);
                    throw new TimeoutException("Timed out initializing application resources");
                }
                try {
                    return future.get(
                            Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100)),
                            TimeUnit.NANOSECONDS);
                } catch (TimeoutException ignored) {
                    // Continue until the single startup deadline expires.
                }
            }
            return future.get();
        }

        private JobResult awaitJob(Future<JobResult> future) throws Exception {
            while (!future.isDone()) {
                try {
                    resourceManager.checkFailure();
                } catch (ExecutionException failure) {
                    if (future.isDone()) {
                        return future.get();
                    }
                    throw failure;
                }
                try {
                    return future.get(100, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // Streaming jobs are unbounded; only asynchronous resource failures stop them.
                }
            }
            return future.get();
        }

        private String formatAddress(Address address) {
            String host = address.getHost();
            return (host.contains(":") ? "[" + host + "]" : host) + ":" + address.getPort();
        }

        private ResourceManagerFactory createResourceManagerFactory() {
            switch (specification.getDeployType()) {
                case KUBERNETES:
                    return new KubernetesResourceManagerFactory(
                            id, specification, clusterName, driver);
                case YARN:
                    return new YarnResourceManagerFactory(id, specification, clusterName, driver);
                default:
                    throw new UnsupportedDeployTypeException(specification.getDeployType());
            }
        }

        private void cleanup(String action, CleanupAction cleanup) {
            cleanup(action, cleanup, 10);
        }

        private void cleanup(String action, CleanupAction cleanup, long operationTimeoutSeconds) {
            try {
                long remaining = cleanupTimeout(operationTimeoutSeconds);
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
            }
        }

        private void cleanupAfterPublication(
                String action, CleanupAction cleanup, long operationTimeoutSeconds) {
            try {
                cleanupExecutor
                        .submit(
                                () -> {
                                    cleanup.run();
                                    return null;
                                })
                        .get(cleanupTimeout(operationTimeoutSeconds), TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                Throwable cause =
                        e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
                log.warn(
                        "Could not {} for application {} after publishing its result",
                        action,
                        id.getId(),
                        cause);
            }
        }

        private long cleanupTimeout(long operationTimeoutSeconds) {
            return Math.max(
                    1,
                    Math.min(
                            cleanupDeadline - System.nanoTime(),
                            TimeUnit.SECONDS.toNanos(operationTimeoutSeconds)));
        }

        private String appendCleanupDiagnostics(String primary) {
            return (primary == null || primary.isEmpty() ? "" : primary + "; ")
                    + "Application cleanup failed: "
                    + String.join("; ", cleanupFailures);
        }
    }

    @FunctionalInterface
    private interface CleanupAction {
        void run() throws Exception;
    }
}
