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
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Fixed worker allocation: abnormal container termination fails the application, without recovery.
 */
public final class YarnResourceManagerDriver implements ResourceManagerDriver<YarnWorkerNode> {
    /** Allocation priority shared by all fixed-capacity workers in one application. */
    private static final int WORKER_PRIORITY = 0;

    /** Heartbeat interval used to request allocations and observe completed containers. */
    private static final long HEARTBEAT_INTERVAL_MILLIS = 500;

    /** YARN progress value while the native job owns the application lifecycle. */
    private static final float APPLICATION_PROGRESS = 0.0f;

    private final Configuration configuration;
    private final Path staging;
    private final String clusterName;
    private final String hadoopUserName;
    private final String masterDistributionHome;
    private final AMRMClient<AMRMClient.ContainerRequest> resourceManager;
    private final NMClient nodeManager;
    private final String workerNodeLabel;
    /**
     * Requests accepted by YARN but not yet matched to an allocated container. Failure drains these
     * exceptionally; shutdown removes their RM requests.
     */
    private final Queue<PendingWorker> pending = new ConcurrentLinkedQueue<>();
    /**
     * Allocated workers currently owned by this driver, keyed by YARN container ID. Completed
     * container events remove entries here, and shutdown releases anything still present.
     */
    private final Map<String, YarnWorkerNode> workers = new ConcurrentHashMap<>();

    private ScheduledFuture<?> heartbeats;
    /**
     * The single in-flight heartbeat. scheduleHeartbeat starts a new one only after this future is
     * done, so RM allocate calls never overlap.
     */
    private volatile CompletableFuture<Void> heartbeatExecution =
            CompletableFuture.completedFuture(null);

    private ScheduledExecutorService mainThreadExecutor;
    private Executor ioExecutor;
    private Supplier<String> masterAddress;
    private ResourceEventHandler<YarnWorkerNode> resourceEventHandler;
    /**
     * Admission and heartbeat gate. Once false, requestWorker is rejected and pending/allocated
     * workers are drained by shutdown or failure handling.
     */
    private final AtomicBoolean active = new AtomicBoolean();
    /**
     * Whether this AM has already registered with the YARN ResourceManager. close() uses this to
     * decide whether a final unregister/failure publication is still required.
     */
    private final AtomicBoolean registered = new AtomicBoolean();
    /** Guards unregister so the terminal status is published exactly once. */
    private final AtomicBoolean finished = new AtomicBoolean();
    /**
     * Whether the NodeManager client has been started and therefore needs an explicit stop during
     * close, even if worker startup failed early.
     */
    private final AtomicBoolean nodeManagerInitialized = new AtomicBoolean();

    public YarnResourceManagerDriver(
            Configuration configuration, Path staging, YarnDriverSettings settings) {
        this(
                configuration,
                staging,
                settings,
                new DefaultYarnResourceManagerClientFactory().create(),
                new DefaultYarnNodeManagerClientFactory().create());
    }

    public YarnResourceManagerDriver(
            Configuration configuration,
            Path staging,
            YarnDriverSettings settings,
            AMRMClient<AMRMClient.ContainerRequest> resourceManager,
            NMClient nodeManager) {
        this.configuration = new Configuration(configuration);
        this.staging = staging;
        this.clusterName = settings.clusterName;
        this.hadoopUserName = settings.hadoopUserName;
        this.masterDistributionHome = settings.masterDistributionHome;
        this.workerNodeLabel = settings.workerNodeLabel;
        this.resourceManager = resourceManager;
        this.nodeManager = nodeManager;
    }

    /** Immutable identity and placement settings for one YARN application driver. */
    public static final class YarnDriverSettings {
        private final String clusterName;
        private final String workerNodeLabel;
        private final String hadoopUserName;
        private final String masterDistributionHome;

        public YarnDriverSettings(
                String clusterName,
                String workerNodeLabel,
                String hadoopUserName,
                String masterDistributionHome) {
            this.clusterName = clusterName;
            this.workerNodeLabel = workerNodeLabel;
            this.hadoopUserName = hadoopUserName;
            this.masterDistributionHome = masterDistributionHome;
        }
    }

    /** Registers this AM before requesting workers and keeps its allocation lease alive. */
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
        resourceManager.init(configuration);
        resourceManager.start();
        nodeManagerInitialized.set(true);
        nodeManager.init(configuration);
        nodeManager.start();
        // The master address is currently formatted as host:port; split before registering.
        String address = masterAddress.get();
        int separator = address.lastIndexOf(':');
        resourceManager.registerApplicationMaster(
                address.substring(0, separator),
                Integer.parseInt(address.substring(separator + 1)),
                "");
        registered.set(true);
        active.set(true);
        heartbeats =
                mainThreadExecutor.scheduleWithFixedDelay(
                        this::scheduleHeartbeat,
                        0,
                        HEARTBEAT_INTERVAL_MILLIS,
                        TimeUnit.MILLISECONDS);
    }

    /** Keeps at most one blocking heartbeat in flight without blocking resource callbacks. */
    private void scheduleHeartbeat() {
        if (active.get() && heartbeatExecution.isDone()) {
            try {
                heartbeatExecution = CompletableFuture.runAsync(this::heartbeat, ioExecutor);
            } catch (RuntimeException failure) {
                reportError(failure);
            }
        }
    }

    @Override
    public synchronized CompletableFuture<YarnWorkerNode> requestWorker(
            WorkerSpecification specification) {
        CompletableFuture<YarnWorkerNode> result = new CompletableFuture<>();
        if (!active.get()) {
            result.completeExceptionally(
                    new IllegalStateException("YARN resource manager is not active"));
            return result;
        }
        AMRMClient.ContainerRequest request =
                new AMRMClient.ContainerRequest(
                        Resource.newInstance(
                                specification.getMemoryMb(), specification.getCpuCores()),
                        null,
                        null,
                        Priority.newInstance(WORKER_PRIORITY),
                        true,
                        workerNodeLabel);
        pending.add(new PendingWorker(request, specification, result));
        resourceManager.addContainerRequest(request);
        return result;
    }

    void heartbeat() {
        if (!active.get()) {
            return;
        }
        try {
            AllocateResponse response = resourceManager.allocate(APPLICATION_PROGRESS);
            // Completed containers first: unexpected non-success exits fail the application.
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                String id = status.getContainerId().toString();
                YarnWorkerNode terminatedWorker;
                terminatedWorker = active.get() ? workers.remove(id) : null;
                if (terminatedWorker != null
                        && status.getExitStatus() != ContainerExitStatus.SUCCESS) {
                    mainThreadExecutor.execute(
                            () ->
                                    resourceEventHandler.onWorkerTerminated(
                                            terminatedWorker,
                                            "YARN container exited with status "
                                                    + status.getExitStatus()
                                                    + ": "
                                                    + status.getDiagnostics()));
                }
            }
            // Allocated containers are paired with the oldest pending worker request.
            for (Container container : response.getAllocatedContainers()) {
                YarnWorkerNode workerNode =
                        new YarnWorkerNode(container, new ResourceID(container.getId().toString()));
                PendingWorker worker;
                worker = active.get() ? pending.poll() : null;
                if (worker != null) {
                    workers.put(workerNode.getWorkerId(), workerNode);
                }
                if (worker == null) {
                    resourceManager.releaseAssignedContainer(workerNode.getContainerId());
                    continue;
                }
                resourceManager.removeContainerRequest(worker.request);
                try {
                    nodeManager.startContainer(
                            workerNode.getContainer(),
                            YarnContainerLaunchContextFactory.worker(
                                    configuration,
                                    staging,
                                    clusterName,
                                    masterAddress.get(),
                                    worker.specification,
                                    hadoopUserName,
                                    masterDistributionHome));
                    if (active.get()
                            && workers.containsKey(workerNode.getWorkerId())
                            && worker.result.complete(workerNode)) {
                        continue;
                    }
                    // A launch can finish after shutdown removed its allocation record.
                    workers.remove(workerNode.getWorkerId());
                    try {
                        nodeManager.stopContainer(
                                workerNode.getContainerId(), workerNode.getNodeId());
                    } finally {
                        resourceManager.releaseAssignedContainer(workerNode.getContainerId());
                    }
                    worker.result.completeExceptionally(
                            new IOException("YARN application stopped during worker launch"));
                } catch (Exception failure) {
                    workers.remove(workerNode.getWorkerId());
                    resourceManager.releaseAssignedContainer(workerNode.getContainerId());
                    worker.result.completeExceptionally(failure);
                    throw failure;
                }
            }
        } catch (Exception failure) {
            reportError(failure);
        }
    }

    private void reportError(Exception failure) {
        boolean report = active.getAndSet(false);
        for (PendingWorker worker : pending) {
            worker.result.completeExceptionally(failure);
        }
        if (report) {
            mainThreadExecutor.execute(() -> resourceEventHandler.onError(failure));
        }
    }

    @Override
    public CompletableFuture<Void> releaseWorker(YarnWorkerNode registration) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        YarnWorkerNode worker = workers.remove(registration.getWorkerId());
        try {
            if (worker != null) {
                // Remove first, so the expected completed-container event cannot fail the
                // application.
                try {
                    nodeManager.stopContainer(worker.getContainerId(), worker.getNodeId());
                } finally {
                    resourceManager.releaseAssignedContainer(worker.getContainerId());
                }
            }
            result.complete(null);
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    /** Reports the job's terminal state; the runtime releases workers before finishing. */
    @Override
    public void finish(ApplicationStatus status, String diagnostics) throws Exception {
        active.set(false);
        if (registered.get() && finished.compareAndSet(false, true)) {
            FinalApplicationStatus finalStatus =
                    YarnApplicationStatus.toFinalApplicationStatus(status);
            try {
                resourceManager.unregisterApplicationMaster(finalStatus, diagnostics, "");
            } catch (Exception failure) {
                finished.set(false);
                throw failure;
            }
        }
    }

    /**
     * Releases outstanding requests and containers even when initialization or job startup failed.
     */
    @Override
    public void stopWorkers() throws Exception {
        List<PendingWorker> waiting;
        List<YarnWorkerNode> allocated;
        CompletableFuture<Void> heartbeat;
        active.set(false);
        if (heartbeats != null) {
            heartbeats.cancel(false);
        }
        // Stop scheduling new heartbeats while allowing the in-flight one to finish.
        heartbeat = heartbeatExecution;
        waiting = new ArrayList<>(pending);
        pending.clear();
        allocated = new ArrayList<>(workers.values());
        Exception failure = null;
        for (PendingWorker worker : waiting) {
            worker.result.completeExceptionally(
                    new IOException("YARN application is shutting down"));
            try {
                resourceManager.removeContainerRequest(worker.request);
            } catch (Exception error) {
                failure = accumulate(failure, error);
            }
        }
        if (!allocated.isEmpty() || !heartbeat.isDone()) {
            // Bound cleanup to the runtime shutdown deadline while releasing every known worker.
            List<Future<?>> stopping = new ArrayList<>();
            stopping.add(heartbeat);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            try {
                for (YarnWorkerNode worker : allocated) {
                    stopping.add(
                            CompletableFuture.runAsync(
                                    () -> releaseWorker(worker).join(), ioExecutor));
                }
                for (Future<?> stop : stopping) {
                    try {
                        stop.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    } catch (Exception error) {
                        failure = accumulate(failure, error);
                    }
                }
            } finally {
                stopping.forEach(stop -> stop.cancel(true));
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Closes SDK clients after worker cleanup and final-result publication. */
    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            stopWorkers();
        } catch (Exception error) {
            failure = error;
        }
        if (registered.get() && !finished.get()) {
            try {
                finish(
                        ApplicationStatus.FAILED,
                        "Application master stopped before job completion");
            } catch (Exception error) {
                failure = accumulate(failure, error);
            }
        }
        if (nodeManagerInitialized.get()) {
            try {
                nodeManager.stop();
            } catch (Exception error) {
                failure = accumulate(failure, error);
            }
        }
        try {
            resourceManager.stop();
        } catch (Exception error) {
            failure = accumulate(failure, error);
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

    private static final class PendingWorker {
        private final AMRMClient.ContainerRequest request;
        private final WorkerSpecification specification;
        private final CompletableFuture<YarnWorkerNode> result;

        private PendingWorker(
                AMRMClient.ContainerRequest request,
                WorkerSpecification specification,
                CompletableFuture<YarnWorkerNode> result) {
            this.request = request;
            this.specification = specification;
            this.result = result;
        }
    }
}
