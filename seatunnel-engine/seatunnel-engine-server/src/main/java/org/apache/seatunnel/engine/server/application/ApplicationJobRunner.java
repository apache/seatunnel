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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigParseOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigSyntax;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Runs one application job on an existing master, independently of the resource platform. */
public final class ApplicationJobRunner {

    private final SeaTunnelServer server;
    private final ApplicationSpecification specification;

    /** Uses an existing master and its localized, resolved application specification. */
    public ApplicationJobRunner(SeaTunnelServer server, ApplicationSpecification specification) {
        this.server = server;
        this.specification = specification;
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
        ApplicationResourceManager<?> resources =
                (ApplicationResourceManager<?>) server.getCoordinatorService().getResourceManager();
        CompletableFuture<Void> cancellation = new CompletableFuture<>();
        CompletableFuture<JobResult> execution = null;
        JobResult result = null;
        Exception failure = null;
        try {
            resources.awaitWorkerRegistration();
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(specification.getName());
            execution =
                    new ApplicationJobExecutionEnvironment(
                                    jobConfig,
                                    ConfigFactory.parseString(
                                            specification.getJobConfig(),
                                            ConfigParseOptions.defaults()
                                                    .setSyntax(ConfigSyntax.JSON)),
                                    server,
                                    specification.getJobId(),
                                    specification.getRestoreJobId())
                            .execute(cancellation);
            result =
                    (JobResult)
                            CompletableFuture.anyOf(execution, resources.getFailureFuture()).get();
        } catch (Exception e) {
            failure = e;
        } finally {
            boolean interrupted = Thread.interrupted() || failure instanceof InterruptedException;
            try {
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
                completed(resources, result, failure);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Releases workers, publishes the application outcome and closes the driver, in that order.
     * Cleanup failures are retained on the original execution failure and turn success into
     * failure. The master must remain alive until this method returns so late allocations can be
     * drained.
     */
    private void completed(
            ApplicationResourceManager<?> resources, JobResult result, Exception executionFailure)
            throws Exception {
        Exception outcome =
                executionFailure == null ? null : ExceptionUtils.unwrap(executionFailure);
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
            outcome = ExceptionUtils.collect(outcome, ExceptionUtils.unwrap(failure));
            status = ApplicationStatus.FAILED;
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

        if (outcome != null) {
            throw outcome;
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

    private ApplicationStatus determineStatus(JobResult result, Exception outcome) {
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
    private ApplicationStatus applicationStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }

        return status == JobStatus.CANCELED ? ApplicationStatus.CANCELED : ApplicationStatus.FAILED;
    }
}
