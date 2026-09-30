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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates external worker processes with Engine worker registration for one application.
 *
 * <p>The platform driver owns pods or containers; this resource manager owns the complete driver
 * lifecycle and is also the coordinator's normal slot registry. A worker becomes ready only after
 * it has registered through the Engine heartbeat path inherited from {@link
 * AbstractResourceManager}.
 */
@Slf4j
public class ApplicationResourceManager<WorkerType extends ResourceIDRetrievable>
        extends AbstractResourceManager implements ResourceManagerContext {

    private final String applicationId;
    private final ApplicationSpecification specification;
    private final Address masterAddress;
    private final ResourceManagerDriver<WorkerType> driver;
    private final List<CompletableFuture<WorkerType>> requests = new CopyOnWriteArrayList<>();
    private final List<WorkerType> workers = new CopyOnWriteArrayList<>();
    private final CompletableFuture<Void> workersReady = new CompletableFuture<>();
    private final ExecutorService startup;
    private long startupDeadline;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    /**
     * -- GETTER -- Notifies the job owner of the first asynchronous allocation or worker failure.
     */
    @Getter private final CompletableFuture<Void> failureFuture = new CompletableFuture<>();

    private final AtomicBoolean workersStopped = new AtomicBoolean();
    private final AtomicBoolean resultPublished = new AtomicBoolean();

    protected ApplicationResourceManager(
            NodeEngine nodeEngine,
            EngineConfig engineConfig,
            String applicationId,
            ApplicationSpecification specification,
            Address masterAddress,
            ResourceManagerDriver<WorkerType> driver) {
        super(nodeEngine, engineConfig);
        this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
        this.specification = Objects.requireNonNull(specification, "specification");
        this.masterAddress = Objects.requireNonNull(masterAddress, "masterAddress");
        this.driver = Objects.requireNonNull(driver, "driver");
        this.startup =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "seatunnel-application-resources-startup");
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    /** Initializes this application's worker lifecycle; called once by the coordinator. */
    @Override
    public synchronized void init() {
        super.init();
        log.info("Init application ResourceManager");
        try {
            syncExistingWorkerProfiles();
            initializeResourceManager();
        } catch (Exception e) {
            IllegalStateException initializationFailure =
                    new IllegalStateException(
                            "Could not initialize application resource manager", e);
            try {
                close();
            } catch (RuntimeException cleanupFailure) {
                initializationFailure.addSuppressed(cleanupFailure);
            }
            throw initializationFailure;
        }
    }

    /** Starts driver initialization and worker registration without blocking the master thread. */
    private void initializeResourceManager() {
        startupDeadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(specification.getStartupTimeoutMillis());
        startup.execute(
                () -> {
                    try {
                        driver.initialize(this);
                        for (int index = 0; index < specification.getWorkerCount(); index++) {
                            if (workersStopped.get()) {
                                throw new CancellationException("Application resources stopped");
                            }
                            checkFailure();
                            CompletableFuture<WorkerType> request =
                                    driver.requestWorker(specification.getWorkerSpecification());
                            requests.add(request);
                            if (workersStopped.get()) {
                                request.cancel(true);
                            }
                        }
                        awaitWorkerRegistration(startupDeadline);
                        workersReady.complete(null);
                    } catch (Throwable error) {
                        workersReady.completeExceptionally(error);
                        onError(error);
                    } finally {
                        startup.shutdown();
                    }
                });
    }

    /** Waits for resources within the startup timeout, including driver initialization. */
    public void awaitWorkerRegistration() throws Exception {
        try {
            workersReady.get(
                    Math.max(0, startupDeadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new TimeoutException("Timed out initializing application resources and workers");
        }
    }

    /**
     * Waits until every platform allocation completes and every worker registers with Engine.
     *
     * @param deadlineNanos absolute {@link System#nanoTime()} startup deadline
     */
    private void awaitWorkerRegistration(long deadlineNanos) throws Exception {
        for (CompletableFuture<WorkerType> request : requests) {
            workers.add(await(request, deadlineNanos));
        }
        while (workerCount(Collections.emptyMap()) < specification.getWorkerCount()) {
            checkFailure();
            checkDeadline(deadlineNanos);
            TimeUnit.MILLISECONDS.sleep(100);
        }
        checkFailure();
    }

    /** Throws the first asynchronous platform or worker failure, when one has been reported. */
    public void checkFailure() throws ExecutionException {
        Throwable reported = failure.get();
        if (reported != null) {
            throw new ExecutionException(reported);
        }
    }

    /**
     * Releases workers, publishes the application outcome and closes the driver, in that order.
     * Cleanup failures are retained on the original execution failure and turn success into
     * failure. The master must remain alive until this method returns so late allocations can be
     * drained.
     */
    public void finishApplication(JobResult result, Exception executionFailure) throws Exception {
        Exception outcome = executionFailure == null ? null : unwrap(executionFailure);
        ApplicationStatus status =
                outcome == null && result != null
                        ? applicationStatus(result.getStatus())
                        : outcome != null
                                        && outcome.getSuppressed().length == 0
                                        && (outcome instanceof InterruptedException
                                                || outcome instanceof CancellationException)
                                ? ApplicationStatus.CANCELED
                                : ApplicationStatus.FAILED;
        if (outcome == null && status != ApplicationStatus.SUCCEEDED) {
            outcome =
                    status == ApplicationStatus.CANCELED
                            ? new CancellationException(
                                    "Application " + applicationId + " was canceled")
                            : new IllegalStateException(
                                    "Application "
                                            + applicationId
                                            + " failed: "
                                            + (result == null
                                                    ? "job did not complete"
                                                    : result.getError()));
        }
        ExecutorService cleanup =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "seatunnel-application-resources-cleanup");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            try {
                cleanup.submit(
                                () -> {
                                    stopApplicationWorkers();
                                    return null;
                                })
                        .get(60, TimeUnit.SECONDS);
            } catch (Exception failure) {
                outcome = collect(outcome, unwrap(failure));
                status = ApplicationStatus.FAILED;
            }
            final ApplicationStatus finalStatus = status;
            final String diagnostics = outcome == null ? "" : outcome.toString();
            try {
                cleanup.submit(
                                () -> {
                                    finish(finalStatus, diagnostics);
                                    return null;
                                })
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception failure) {
                outcome = collect(outcome, unwrap(failure));
            }
            try {
                cleanup.submit(
                                () -> {
                                    close();
                                    return null;
                                })
                        .get(20, TimeUnit.SECONDS);
            } catch (Exception failure) {
                outcome = collect(outcome, unwrap(failure));
            }
        } finally {
            cleanup.shutdownNow();
        }
        if (outcome != null) {
            throw outcome;
        }
    }

    /** Maps native job termination to the external application's terminal state. */
    public static ApplicationStatus applicationStatus(JobStatus status) {
        if (status == JobStatus.FINISHED || status == JobStatus.SAVEPOINT_DONE) {
            return ApplicationStatus.SUCCEEDED;
        }
        return status == JobStatus.CANCELED ? ApplicationStatus.CANCELED : ApplicationStatus.FAILED;
    }

    private static Exception unwrap(Exception failure) {
        while ((failure instanceof ExecutionException
                        || failure instanceof java.util.concurrent.CompletionException)
                && failure.getCause() instanceof Exception) {
            Exception cause = (Exception) failure.getCause();
            for (Throwable suppressed : failure.getSuppressed()) {
                if (suppressed != cause) {
                    cause.addSuppressed(suppressed);
                }
            }
            failure = cause;
        }
        return failure;
    }

    /** Stops allocation and releases all platform workers before the Engine master is stopped. */
    public void stopApplicationWorkers() throws Exception {
        if (!workersStopped.compareAndSet(false, true)) {
            return;
        }
        Exception cleanupFailure = null;
        startup.shutdownNow();
        try {
            if (!startup.awaitTermination(10, TimeUnit.SECONDS)) {
                cleanupFailure = new TimeoutException("Application resource startup did not stop");
            }
        } catch (InterruptedException e) {
            cleanupFailure = e;
        }
        workersReady.cancel(false);
        for (CompletableFuture<WorkerType> request : requests) {
            if (!request.isDone()) {
                request.cancel(true);
            }
        }

        List<CompletableFuture<Void>> releases = new ArrayList<>();
        for (WorkerType worker : workers) {
            try {
                releases.add(driver.releaseWorker(worker));
            } catch (RuntimeException e) {
                cleanupFailure = collect(cleanupFailure, e);
            }
        }
        try {
            CompletableFuture.allOf(releases.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
            cleanupFailure = collect(cleanupFailure, e);
        }
        try {
            driver.stopWorkers();
        } catch (Exception e) {
            cleanupFailure = collect(cleanupFailure, e);
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    /** Publishes the final application result after job and worker cleanup. */
    public void finish(ApplicationStatus status, String diagnostics) throws Exception {
        if (!resultPublished.compareAndSet(false, true)) {
            throw new IllegalStateException("Application result has already been published");
        }
        driver.finish(status, diagnostics);
    }

    /** Stops application workers and closes the platform driver. */
    @Override
    protected void closeResourceManager() throws Exception {
        Exception cleanupFailure = null;
        try {
            stopApplicationWorkers();
        } catch (Exception e) {
            cleanupFailure = e;
        }
        if (!resultPublished.get()) {
            try {
                finish(
                        ApplicationStatus.FAILED,
                        "Application master stopped before job completion");
            } catch (Exception e) {
                cleanupFailure = collect(cleanupFailure, e);
            }
        }
        try {
            driver.close();
        } catch (Exception e) {
            cleanupFailure = collect(cleanupFailure, e);
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        super.memberRemoved(event);
        if (!workersStopped.get() && event.getMember().isLiteMember()) {
            onWorkerTerminated(
                    event.getMember().getAddress().toString(),
                    "Worker left the application cluster");
        }
    }

    @Override
    public String getMasterAddress() {
        String host = masterAddress.getHost();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + masterAddress.getPort();
    }

    @Override
    public void onError(Throwable error) {
        if (!workersStopped.get() && failure.compareAndSet(null, error)) {
            failureFuture.completeExceptionally(error);
        }
    }

    @Override
    public void onWorkerTerminated(String workerId, String diagnostics) {
        onError(
                new IllegalStateException(
                        "Application worker " + workerId + " terminated: " + diagnostics));
    }

    private <T> T await(java.util.concurrent.Future<T> future, long deadlineNanos)
            throws Exception {
        while (!future.isDone()) {
            checkFailure();
            checkDeadline(deadlineNanos);
            try {
                return future.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
                // Recheck asynchronous driver failures until startup completes or times out.
            }
        }
        return future.get();
    }

    private void checkDeadline(long deadlineNanos) throws TimeoutException {
        if (System.nanoTime() >= deadlineNanos) {
            throw new TimeoutException(
                    "Timed out starting "
                            + specification.getWorkerCount()
                            + " application workers");
        }
    }

    private static Exception collect(Exception current, Exception next) {
        if (current == null) {
            return next;
        }
        current.addSuppressed(next);
        return current;
    }
}
