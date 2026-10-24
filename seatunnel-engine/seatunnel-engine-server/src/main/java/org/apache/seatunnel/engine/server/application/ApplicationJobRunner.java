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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs one application job on an existing master, independently of the resource platform. */
@Slf4j
public final class ApplicationJobRunner {

    private final SeaTunnelServer server;
    private final ApplicationSpecification specification;
    private final Lifecycle lifecycle;

    /** Application ownership policy; coordinator detachment is distinct from terminal cleanup. */
    public interface Lifecycle {
        /** Waits for provider readiness before deciding whether to submit the native job. */
        default void awaitReady() throws Exception {}

        /**
         * Returns true to submit once, or false to attach to the immutable existing native job ID.
         */
        default boolean submit() throws Exception {
            return true;
        }

        /**
         * Returns whether this runner still owns the active coordinator; false detaches without
         * cleanup.
         */
        default boolean isCurrent() {
            return true;
        }

        /** Persists that native execution reached RUNNING, before any later status absence. */
        default void running() throws Exception {}

        /**
         * On an unknown native result, returns whether persisted RUNNING history disables the
         * startup timeout.
         */
        default boolean hasStarted() throws Exception {
            return false;
        }

        /**
         * Records a terminal native outcome only while this lifecycle still owns the application.
         */
        default void finished(ApplicationStatus status) throws Exception {}
    }

    /** Ownership metadata is unavailable; detach without changing the native job outcome. */
    public static final class LifecycleException extends Exception {
        public LifecycleException(Exception cause) {
            super(cause);
        }
    }

    private static <T> T policy(Callable<T> action) throws LifecycleException {
        try {
            return action.call();
        } catch (Exception failure) {
            throw new LifecycleException(failure);
        }
    }

    private static final Lifecycle LOCAL = new Lifecycle() {};

    /** Uses an existing master and its localized, resolved application specification. */
    public ApplicationJobRunner(SeaTunnelServer server, ApplicationSpecification specification) {
        this(server, specification, LOCAL);
    }

    public ApplicationJobRunner(
            SeaTunnelServer server, ApplicationSpecification specification, Lifecycle lifecycle) {
        this.server = server;
        this.specification = specification;
        this.lifecycle = lifecycle;
    }

    /**
     * Waits for workers, executes the native job and finishes application resources.
     *
     * <p>Resource failure or interruption cancels an unfinished job before resource cleanup. The
     * caller retains ownership of the master and shuts it down after this method returns.
     *
     * @throws Exception if startup, execution, cancellation or resource cleanup fails
     */
    public void run() throws Exception {
        // The application's own startup budget covers a slow but healthy coordinator activation:
        // the cluster-mode retry window of getCoordinatorService() is meant for callers that can
        // retry, and this runner executes exactly one job without such a caller.
        if (!lifecycle.isCurrent()) {
            return;
        }
        CoordinatorService coordinator =
                server.awaitCoordinatorService(specification.getStartupTimeoutMillis());
        ApplicationResourceManager<?> resources =
                (ApplicationResourceManager<?>) coordinator.getResourceManager();
        CompletableFuture<Void> cancellation = new CompletableFuture<>();
        CompletableFuture<JobResult> execution = null;
        JobResult result = null;
        Throwable failure = null;
        boolean runningRecorded = false;
        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            // Resolve remaining placeholders against this Pod's environment, including values
            // injected from Kubernetes.
            Config job =
                    ConfigShadeUtils.decryptConfig(
                            ConfigFactory.parseString(
                                            specification.getJobConfig(),
                                            ConfigParseOptions.defaults()
                                                    .setSyntax(ConfigSyntax.CONF))
                                    .resolve());
            resources.awaitWorkerRegistration();
            policy(
                    () -> {
                        lifecycle.awaitReady();
                        return null;
                    });
            if (!lifecycle.isCurrent()) {
                return;
            }
            execution =
                    policy(lifecycle::submit)
                            ? new ApplicationJobExecutionEnvironment(
                                            jobConfig,
                                            job,
                                            server,
                                            specification.getJobId(),
                                            specification.getRestoreJobId())
                                    .execute(cancellation)
                            : coordinator.waitForJobComplete(specification.getJobId());
            long deadline =
                    System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(
                                    specification.getStartupTimeoutMillis());
            while (lifecycle.isCurrent()) {
                if (!execution.isDone() && resources.getFailureFuture().isDone()) {
                    resources.getFailureFuture().get();
                }
                try {
                    result = execution.get(100, TimeUnit.MILLISECONDS);
                    if (result.getStatus() != JobStatus.UNKNOWABLE) {
                        break;
                    }
                    if (resources.getFailureFuture().isDone()) {
                        resources.getFailureFuture().get();
                    }
                    // Absence while a coordinator is taking over is not a terminal failure.
                    if (!policy(lifecycle::hasStarted) && System.nanoTime() > deadline) {
                        throw new IllegalStateException(
                                "Native application submission is unresolved; refusing resubmission");
                    }
                    TimeUnit.MILLISECONDS.sleep(100);
                    execution = coordinator.waitForJobComplete(specification.getJobId());
                } catch (TimeoutException waiting) {
                    if (!runningRecorded
                            && lifecycle != LOCAL
                            && coordinator.getJobStatus(specification.getJobId())
                                    == JobStatus.RUNNING) {
                        policy(
                                () -> {
                                    lifecycle.running();
                                    return null;
                                });
                        runningRecorded = true;
                    }
                }
            }
        } catch (Throwable e) {
            failure = e;
        } finally {
            boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
            try {
                if (failure instanceof LifecycleException) {
                    throw (LifecycleException) failure;
                }
                if (lifecycle.isCurrent()) {
                    if (execution != null && !execution.isDone()) {
                        cancellation.complete(null);
                        try {
                            execution.get(10, TimeUnit.SECONDS);
                        } catch (Exception cleanup) {
                            if (failure == null) {
                                failure = cleanup;
                            } else {
                                failure.addSuppressed(cleanup);
                            }
                        }
                    }
                    lifecycle.finished(determineStatus(result, ExceptionUtils.unwrap(failure)));
                    completed(resources, result, failure);
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Releases workers, publishes the application outcome and closes the driver, in that order.
     * Cleanup failures are retained on the original execution failure; when the native job already
     * finished, the application stays successful and the cleanup failure is only logged, because
     * workers are job-owned and exit with the master while a failed status would make orchestrators
     * re-run a committed job. The master must remain alive until this method returns so late
     * allocations can be drained. A JVM {@link Error} is rethrown as-is instead of being replaced
     * by a synthetic failure, so the original diagnosis survives the cleanup.
     */
    private void completed(
            ApplicationResourceManager<?> resources, JobResult result, Throwable executionFailure)
            throws Exception {
        Throwable outcome = ExceptionUtils.unwrap(executionFailure);
        ApplicationStatus status = determineStatus(result, outcome);

        if (outcome == null && status != ApplicationStatus.SUCCEEDED) {
            outcome = createFailure(resources, result, status);
        }

        try {
            runCleanup(
                    () -> {
                        try {
                            resources.stopApplicationWorkers();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    60,
                    "stop application workers");
        } catch (Exception failure) {
            Exception cleanupFailure = ExceptionUtils.unwrap(failure);
            if (status == ApplicationStatus.SUCCEEDED) {
                log.warn(
                        "Application {} finished but worker cleanup failed",
                        resources.getApplicationId(),
                        cleanupFailure);
            } else {
                outcome = ExceptionUtils.collect(outcome, cleanupFailure);
                status = ApplicationStatus.FAILED;
            }
        }

        String diagnostics = outcome == null ? "" : outcome.toString();
        try {
            ApplicationStatus finalStatus = status;
            runCleanup(
                    () -> {
                        try {
                            resources.finish(finalStatus, diagnostics);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    10,
                    "finish application");
        } catch (Exception failure) {
            outcome = ExceptionUtils.collect(outcome, ExceptionUtils.unwrap(failure));
        }

        try {
            runCleanup(resources::close, 20, "close application resources");
        } catch (Exception failure) {
            outcome = ExceptionUtils.collect(outcome, ExceptionUtils.unwrap(failure));
        }

        if (outcome instanceof Error) {
            throw (Error) outcome;
        }
        if (outcome instanceof Exception) {
            throw (Exception) outcome;
        }
        if (outcome != null) {
            throw new IllegalStateException(outcome);
        }
    }

    private void runCleanup(Runnable action, long timeoutSeconds, String operation)
            throws Exception {
        ExecutorService cleanupExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "seatunnel-application-resources-cleanup");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            cleanupExecutor.submit(action).get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw ExceptionUtils.unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out while trying to " + operation);
        } finally {
            cleanupExecutor.shutdownNow();
        }
    }

    private ApplicationStatus determineStatus(JobResult result, Throwable outcome) {
        if (outcome == null && result != null) {
            return applicationStatus(result.getStatus());
        }

        if (outcome != null
                && outcome.getSuppressed().length == 0
                && (outcome instanceof InterruptedException
                        || outcome instanceof CancellationException)) {
            return ApplicationStatus.CANCELED;
        }

        return ApplicationStatus.FAILED;
    }

    private Exception createFailure(
            ApplicationResourceManager<?> resources, JobResult result, ApplicationStatus status) {
        if (status == ApplicationStatus.CANCELED) {
            return new CancellationException(
                    "Application " + resources.getApplicationId() + " was canceled");
        }

        return new IllegalStateException(
                "Application "
                        + resources.getApplicationId()
                        + " failed: "
                        + (result == null ? "job did not complete" : result.getError()));
    }

    /** Maps native job termination to the external application's terminal state. */
    public static ApplicationStatus terminalStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        if (status == JobStatus.CANCELED) {
            return ApplicationStatus.CANCELED;
        }
        return status == JobStatus.FAILED ? ApplicationStatus.FAILED : null;
    }

    private ApplicationStatus applicationStatus(JobStatus status) {
        ApplicationStatus terminal = terminalStatus(status);
        return terminal == null ? ApplicationStatus.FAILED : terminal;
    }
}
