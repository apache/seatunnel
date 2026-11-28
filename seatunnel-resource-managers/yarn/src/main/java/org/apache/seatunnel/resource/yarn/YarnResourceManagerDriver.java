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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.resource.yarn.client.YarnApplicationStatus;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.factory.WorkerContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Fixed worker allocation: abnormal container termination fails the application, without recovery.
 */
public final class YarnResourceManagerDriver implements ResourceManagerDriver<YarnWorkerNode> {
    private static final Logger LOG = LoggerFactory.getLogger(YarnResourceManagerDriver.class);

    /** Heartbeat interval used to request allocations and observe completed containers. */
    private static final int HEARTBEAT_INTERVAL_MILLIS = 1000;

    private static final int WORKER_RELEASE_TIMEOUT_SECONDS = 30;

    private final Configuration configuration;
    private final WorkerContainerFactory workerFactory;
    private final String clusterName;
    private final String hadoopUserName;
    private final String masterDistributionHome;
    private final String trackingUrl;
    private final AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;
    private final NMClient nodeManager;
    private final String workerNodeLabel;
    // Access to requests and allocations is serialized with admission and shutdown.
    private final Queue<YarnWorkerRequest> pending = new ArrayDeque<>();
    private final Map<String, YarnWorkerAllocation> workers = new HashMap<>();

    private ScheduledExecutorService mainThreadExecutor;
    private Executor ioExecutor;
    private Supplier<String> masterAddress;
    private ResourceEventHandler<YarnWorkerNode> resourceEventHandler;
    private volatile RegistrationState registration = RegistrationState.UNREGISTERED;
    // Hadoop can invoke a callback while unregister is waiting on its heartbeat thread.
    // Serialize final reports independently of the lock used by worker callbacks.
    private final Object unregisterLock = new Object();
    // Guarded by unregisterLock. Keep the original report until YARN acknowledges it.
    private FinalApplicationStatus requestedFinalStatus;
    private String requestedDiagnostics;

    private enum RegistrationState {
        UNREGISTERED,
        RUNNING,
        DRAINING
    }

    /**
     * Creates the RM and NodeManager clients for the current AM. {@link #initialize} starts them.
     */
    public YarnResourceManagerDriver(
            Configuration configuration, Path staging, YarnDriverSettings settings) {
        this(
                configuration,
                staging,
                settings,
                new DefaultYarnResourceManagerClientFactory().create(),
                new DefaultYarnNodeManagerClientFactory().create());
    }

    /**
     * Takes ownership of the supplied Hadoop clients. The AM uses the RM client for heartbeats and
     * the NodeManager client to start and stop workers.
     */
    public YarnResourceManagerDriver(
            Configuration configuration,
            Path staging,
            YarnDriverSettings settings,
            AMRMClient<AMRMClient.ContainerRequest> resourceManager,
            NMClient nodeManager) {
        this.configuration = new Configuration(configuration);
        this.workerFactory = new WorkerContainerFactory(this.configuration, staging);
        this.clusterName = settings.clusterName;
        this.hadoopUserName = settings.hadoopUserName;
        this.masterDistributionHome = settings.masterDistributionHome;
        this.trackingUrl = settings.trackingUrl;
        this.workerNodeLabel = settings.workerNodeLabel;
        this.resourceManager =
                AMRMClientAsync.createAMRMClientAsync(
                        resourceManager,
                        HEARTBEAT_INTERVAL_MILLIS,
                        new YarnResourceManagerCallbackHandler(
                                clusterName,
                                this::acceptAllocation,
                                this::containerCompleted,
                                this::reportError));
        this.nodeManager = nodeManager;
    }

    /** Immutable identity and placement settings for one YARN application driver. */
    public static final class YarnDriverSettings {
        private final String clusterName;
        private final String workerNodeLabel;
        private final String hadoopUserName;
        private final String masterDistributionHome;
        private final String trackingUrl;

        public YarnDriverSettings(
                String clusterName,
                String workerNodeLabel,
                String hadoopUserName,
                String masterDistributionHome,
                String trackingUrl) {
            this.clusterName = clusterName;
            this.workerNodeLabel = workerNodeLabel;
            this.hadoopUserName = hadoopUserName;
            this.masterDistributionHome = masterDistributionHome;
            this.trackingUrl = trackingUrl;
        }
    }

    /**
     * Starts the Hadoop clients and registers the AM's bound endpoint. Once registered, the RM
     * heartbeat delivers allocations and completions to {@link YarnResourceManagerCallbackHandler}.
     *
     * @param resourceEventHandler receives unexpected worker exits and driver errors
     * @param mainThreadExecutor delivers those events to the engine
     * @param ioExecutor runs blocking NodeManager start and release operations
     * @param masterAddress supplies the bound master endpoint for registration and worker launches
     * @throws YarnClientException if client startup or AM registration fails
     */
    @Override
    public synchronized void initialize(
            ResourceEventHandler<YarnWorkerNode> resourceEventHandler,
            ScheduledExecutorService mainThreadExecutor,
            Executor ioExecutor,
            Supplier<String> masterAddress)
            throws Exception {
        this.masterAddress = masterAddress;
        this.resourceEventHandler = resourceEventHandler;
        this.mainThreadExecutor = mainThreadExecutor;
        this.ioExecutor = ioExecutor;
        String address = masterAddress.get();
        try {
            resourceManager.init(configuration);
            resourceManager.start();
            nodeManager.init(configuration);
            nodeManager.start();
            // The master address is currently formatted as host:port; split before registering.
            int separator = address.lastIndexOf(':');
            resourceManager.registerApplicationMaster(
                    address.substring(0, separator),
                    Integer.parseInt(address.substring(separator + 1)),
                    trackingUrl);
        } catch (Exception failure) {
            throw new YarnClientException(
                    "Could not register YARN application master " + clusterName, failure);
        }
        registration = RegistrationState.RUNNING;
        LOG.info("Registered YARN application master at {}", address);
    }

    /**
     * Queues one fixed-size worker request. The next available container is assigned to the oldest
     * pending request and launched on the I/O executor.
     *
     * @param specification requested resources and execution slots for this worker
     * @return a future completed when the NodeManager accepts the launch. The worker registers with
     *     the engine separately.
     */
    @Override
    public synchronized CompletableFuture<YarnWorkerNode> requestWorker(
            WorkerSpecification specification) {
        CompletableFuture<YarnWorkerNode> result = new CompletableFuture<>();
        if (registration != RegistrationState.RUNNING) {
            result.completeExceptionally(
                    new IllegalStateException("YARN resource manager is not active"));
            return result;
        }
        AMRMClient.ContainerRequest request =
                WorkerContainerFactory.createRequest(specification, workerNodeLabel);
        YarnWorkerRequest worker = new YarnWorkerRequest(request, specification, result);
        pending.add(worker);
        try {
            resourceManager.addContainerRequest(request);
        } catch (RuntimeException failure) {
            pending.remove(worker);
            result.completeExceptionally(
                    new YarnClientException(
                            "Could not request a YARN worker for " + clusterName, failure));
        }
        return result;
    }

    /**
     * Claims the oldest request before dispatching blocking NodeManager I/O off the callback
     * thread.
     */
    private synchronized void acceptAllocation(Container container) {
        YarnWorkerNode node =
                new YarnWorkerNode(container, new ResourceID(container.getId().toString()));
        YarnWorkerRequest request = pending.peek();
        if (registration != RegistrationState.RUNNING || request == null) {
            try {
                resourceManager.releaseAssignedContainer(container.getId());
            } catch (RuntimeException failure) {
                reportError(
                        new YarnClientException(
                                "Could not release unused YARN allocation " + container.getId(),
                                failure));
            }
            return;
        }
        YarnWorkerAllocation allocation = new YarnWorkerAllocation(node, request);
        workers.put(node.getWorkerId(), allocation);
        try {
            resourceManager.removeContainerRequest(request.getContainerRequest());
            pending.remove();
            ioExecutor.execute(() -> launchWorker(allocation));
        } catch (RuntimeException failure) {
            allocation.finishLaunch(false);
            failLaunch(allocation, failure);
        }
    }

    /**
     * Starts the container on the I/O executor outside the driver lock. After the RPC, the lock
     * guards publication against shutdown or container completion. The finally block lets release
     * proceed even when the NodeManager may have accepted a start RPC that threw.
     */
    private void launchWorker(YarnWorkerAllocation allocation) {
        YarnWorkerNode node = allocation.getNode();
        YarnWorkerRequest request = allocation.getRequest();
        CompletableFuture<YarnWorkerNode> result = request.getResult();
        boolean startAttempted = false;
        try {
            if (registration == RegistrationState.RUNNING && !result.isDone()) {
                YarnContainerSpec worker =
                        workerFactory.create(
                                clusterName,
                                masterAddress.get(),
                                request.getSpecification(),
                                hadoopUserName,
                                masterDistributionHome);
                startAttempted = true;
                nodeManager.startContainer(node.getContainer(), worker.getInternalResource());
                synchronized (this) {
                    if (registration == RegistrationState.RUNNING
                            && workers.get(node.getWorkerId()) == allocation
                            && result.complete(node)) {
                        return;
                    }
                }
            }
            result.completeExceptionally(
                    new CancellationException("YARN worker launch was canceled"));
            releaseWorker(node);
        } catch (Exception failure) {
            failLaunch(allocation, failure);
        } finally {
            allocation.finishLaunch(startAttempted);
        }
    }

    /**
     * Fails the request and schedules release from either the allocation callback or the I/O
     * thread. Release waits for the caller's launch completion signal before stopping the worker.
     */
    private void failLaunch(YarnWorkerAllocation allocation, Exception cause) {
        YarnWorkerNode node = allocation.getNode();
        YarnClientException failure =
                new YarnClientException(
                        "Could not launch YARN worker " + node.getContainerId(), cause);
        allocation.getRequest().getResult().completeExceptionally(failure);
        reportError(failure);
        releaseWorker(node);
    }

    /**
     * Retires a completed allocation. An unexpected nonzero exit reaches the engine's main thread
     * while the driver is running. A worker already being released is owned by its release
     * operation.
     */
    private synchronized void containerCompleted(ContainerStatus status) {
        String id = status.getContainerId().toString();
        YarnWorkerAllocation allocation = workers.get(id);
        if (allocation == null || allocation.isReleasing()) {
            return;
        }
        workers.remove(id);
        String diagnostics =
                "YARN container exited with status "
                        + status.getExitStatus()
                        + ": "
                        + status.getDiagnostics();
        CompletableFuture<YarnWorkerNode> result = allocation.getRequest().getResult();
        if (!result.isDone()) {
            result.completeExceptionally(new YarnClientException(id + ": " + diagnostics));
        }
        if (registration == RegistrationState.RUNNING
                && status.getExitStatus() != ContainerExitStatus.SUCCESS) {
            mainThreadExecutor.execute(
                    () ->
                            resourceEventHandler.onWorkerTerminated(
                                    allocation.getNode(), diagnostics));
        }
    }

    private synchronized void reportError(YarnClientException failure) {
        if (registration == RegistrationState.RUNNING) {
            registration = RegistrationState.DRAINING;
            LOG.warn("YARN resource manager failed", failure);
            pending.forEach(worker -> worker.getResult().completeExceptionally(failure));
            workers.values()
                    .forEach(
                            allocation ->
                                    allocation
                                            .getRequest()
                                            .getResult()
                                            .completeExceptionally(failure));
            mainThreadExecutor.execute(() -> resourceEventHandler.onError(failure));
        }
    }

    /**
     * Waits for an in-flight launch before stopping the container and returning it to the RM.
     * Concurrent callers share the underlying release operation.
     *
     * @param node worker allocation to release
     * @return a future for cleanup, already complete if the driver no longer tracks the worker
     */
    @Override
    public synchronized CompletableFuture<Void> releaseWorker(YarnWorkerNode node) {
        YarnWorkerAllocation allocation = workers.get(node.getWorkerId());
        if (allocation == null) {
            return CompletableFuture.completedFuture(null);
        }
        // Canceling a caller's wait must not cancel the shared container cleanup.
        return new CompletableFuture<>(
                allocation.releaseAfterLaunch(
                        ioExecutor,
                        startAttempted -> stopReleasedWorker(allocation, startAttempted)));
    }

    /**
     * Attempts RM release even if the NM stop fails. A failed cleanup stays tracked for a retry.
     */
    private void stopReleasedWorker(YarnWorkerAllocation allocation, boolean startAttempted) {
        YarnWorkerNode worker = allocation.getNode();
        Exception failure = null;
        if (startAttempted) {
            try {
                nodeManager.stopContainer(worker.getContainerId(), worker.getNodeId());
            } catch (Exception error) {
                failure = error;
            }
        }
        try {
            resourceManager.releaseAssignedContainer(worker.getContainerId());
        } catch (Exception error) {
            failure = accumulate(failure, error);
        }
        if (failure != null) {
            throw new YarnClientException(
                    "Could not release YARN worker " + worker.getContainerId(), failure);
        }
        synchronized (this) {
            workers.remove(worker.getWorkerId(), allocation);
        }
    }

    /**
     * Unregisters the AM after the runtime releases its workers. A retry uses the first requested
     * result and diagnostics. A completed unregistration makes subsequent calls return immediately.
     *
     * @param status terminal engine result to publish to YARN
     * @param diagnostics final message included in the RM application report
     * @throws YarnClientException if the RM rejects or fails to acknowledge unregistration
     */
    @Override
    public void finish(ApplicationStatus status, String diagnostics) throws Exception {
        synchronized (unregisterLock) {
            synchronized (this) {
                if (registration == RegistrationState.UNREGISTERED) {
                    return;
                }
                registration = RegistrationState.DRAINING;
            }
            if (requestedFinalStatus == null) {
                requestedFinalStatus = YarnApplicationStatus.toFinalApplicationStatus(status);
                requestedDiagnostics = diagnostics;
            }
            try {
                resourceManager.unregisterApplicationMaster(
                        requestedFinalStatus, requestedDiagnostics, "");
                registration = RegistrationState.UNREGISTERED;
            } catch (Exception failure) {
                throw new YarnClientException(
                        "Could not unregister YARN application master " + clusterName, failure);
            }
        }
    }

    /**
     * Stops admitting allocations, withdraws queued requests and waits for tracked containers to be
     * released within the driver's shutdown budget.
     *
     * @throws Exception if withdrawing a request or waiting for container release fails
     */
    @Override
    public void stopWorkers() throws Exception {
        List<YarnWorkerRequest> waiting;
        List<YarnWorkerAllocation> allocated;
        synchronized (this) {
            if (registration == RegistrationState.RUNNING) {
                registration = RegistrationState.DRAINING;
            }
            waiting = new ArrayList<>(pending);
            pending.clear();
            allocated = new ArrayList<>(workers.values());
        }
        Exception failure = null;
        CancellationException shutdown =
                new CancellationException("YARN application is shutting down");
        for (YarnWorkerRequest worker : waiting) {
            worker.getResult().completeExceptionally(shutdown);
            try {
                resourceManager.removeContainerRequest(worker.getContainerRequest());
            } catch (RuntimeException error) {
                failure =
                        accumulate(
                                failure,
                                new YarnClientException(
                                        "Could not withdraw YARN worker request for " + clusterName,
                                        error));
            }
        }
        long deadline =
                System.nanoTime() + TimeUnit.SECONDS.toNanos(WORKER_RELEASE_TIMEOUT_SECONDS);
        List<Future<?>> stopping = new ArrayList<>();
        for (YarnWorkerAllocation allocation : allocated) {
            allocation.getRequest().getResult().completeExceptionally(shutdown);
            stopping.add(releaseWorker(allocation.getNode()));
        }
        for (Future<?> stop : stopping) {
            try {
                stop.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw interrupted;
            } catch (Exception error) {
                failure = accumulate(failure, error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Drains workers and stops both Hadoop clients. If the AM is still registered, it retries the
     * pending final report or reports failure if the runtime never supplied a result.
     *
     * @throws Exception if cleanup fails, with later failures attached to the first
     */
    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            stopWorkers();
        } catch (Exception error) {
            failure = error;
        }
        try {
            finish(ApplicationStatus.FAILED, "Application master stopped before job completion");
        } catch (Exception error) {
            failure = accumulate(failure, error);
        }
        if (!nodeManager.isInState(Service.STATE.NOTINITED)) {
            try {
                nodeManager.stop();
            } catch (RuntimeException error) {
                failure =
                        accumulate(
                                failure,
                                new YarnClientException(
                                        "Could not close YARN NodeManager client for "
                                                + clusterName,
                                        error));
            }
        }
        try {
            resourceManager.stop();
        } catch (RuntimeException error) {
            failure =
                    accumulate(
                            failure,
                            new YarnClientException(
                                    "Could not close YARN ResourceManager client for "
                                            + clusterName,
                                    error));
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Exception accumulate(Exception first, Exception next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
    }
}
