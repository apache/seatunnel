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

package org.apache.seatunnel.resource.yarn.cluster;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Fixed worker allocation: a lost container fails the application, without replacement or HA. */
final class YarnResourceManagerDriver implements ResourceManagerDriver {
    /** Allocation priority shared by all fixed-capacity workers in one application. */
    private static final int WORKER_PRIORITY = 0;

    /** Heartbeat interval used to request allocations and observe completed containers. */
    private static final long HEARTBEAT_INTERVAL_MILLIS = 500;

    /** YARN progress value while the native job owns the application lifecycle. */
    private static final float APPLICATION_PROGRESS = 0.0f;

    private final Configuration configuration;
    private final Path staging;
    private final AMRMClient<AMRMClient.ContainerRequest> resourceManager;
    private final NMClient nodeManager;
    private final String workerNodeLabel;
    private final Queue<PendingWorker> pending = new ArrayDeque<>();
    private final Map<String, YarnWorkerNode> workers = new HashMap<>();
    private ScheduledExecutorService heartbeats;
    private ResourceManagerContext context;
    private boolean active;
    private boolean registered;
    private boolean finished;
    private boolean nodeManagerInitialized;

    YarnResourceManagerDriver(Configuration configuration, Path staging) {
        this(configuration, staging, null);
    }

    YarnResourceManagerDriver(Configuration configuration, Path staging, String workerNodeLabel) {
        this(
                configuration,
                staging,
                workerNodeLabel,
                new DefaultYarnResourceManagerClientFactory().create(),
                new DefaultYarnNodeManagerClientFactory().create());
    }

    YarnResourceManagerDriver(
            Configuration configuration,
            Path staging,
            AMRMClient<AMRMClient.ContainerRequest> resourceManager,
            NMClient nodeManager) {
        this(configuration, staging, null, resourceManager, nodeManager);
    }

    YarnResourceManagerDriver(
            Configuration configuration,
            Path staging,
            String workerNodeLabel,
            AMRMClient<AMRMClient.ContainerRequest> resourceManager,
            NMClient nodeManager) {
        this.configuration = YarnConfigurationUtils.withBoundedRpc(configuration);
        this.staging = staging;
        this.workerNodeLabel = workerNodeLabel;
        this.resourceManager = resourceManager;
        this.nodeManager = nodeManager;
    }

    /** Registers this AM before requesting workers and keeps its allocation lease alive. */
    @Override
    public synchronized void initialize(ResourceManagerContext context) throws Exception {
        this.context = context;
        resourceManager.init(configuration);
        resourceManager.start();
        nodeManagerInitialized = true;
        nodeManager.init(configuration);
        nodeManager.start();
        String address = context.getMasterAddress();
        int separator = address.lastIndexOf(':');
        resourceManager.registerApplicationMaster(
                address.substring(0, separator),
                Integer.parseInt(address.substring(separator + 1)),
                "");
        registered = true;
        active = true;
        heartbeats =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-yarn-heartbeat");
                            thread.setDaemon(true);
                            return thread;
                        });
        heartbeats.scheduleWithFixedDelay(
                this::heartbeat, 0, HEARTBEAT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized CompletableFuture<WorkerRegistration> requestWorker(
            WorkerSpecification specification) {
        CompletableFuture<WorkerRegistration> result = new CompletableFuture<>();
        if (!active) {
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
        synchronized (this) {
            if (!active) {
                return;
            }
        }
        try {
            AllocateResponse response = resourceManager.allocate(APPLICATION_PROGRESS);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                String id = status.getContainerId().toString();
                boolean unexpected;
                synchronized (this) {
                    unexpected = active && workers.remove(id) != null;
                }
                if (unexpected) {
                    context.onWorkerTerminated(
                            id,
                            "YARN container exited with status "
                                    + status.getExitStatus()
                                    + ": "
                                    + status.getDiagnostics());
                }
            }
            for (Container container : response.getAllocatedContainers()) {
                YarnWorkerNode workerNode = new YarnWorkerNode(container);
                PendingWorker worker;
                synchronized (this) {
                    worker = active ? pending.poll() : null;
                    if (worker != null) {
                        workers.put(workerNode.getWorkerId(), workerNode);
                    }
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
                                    context.getClusterName(),
                                    context.getMasterAddress(),
                                    worker.specification));
                    synchronized (this) {
                        if (active && workers.containsKey(workerNode.getWorkerId())) {
                            worker.result.complete(
                                    new WorkerRegistration(workerNode.getWorkerId()));
                            continue;
                        }
                    }
                    // A launch can finish after shutdown removed its allocation record.
                    try {
                        nodeManager.stopContainer(
                                workerNode.getContainerId(), workerNode.getNodeId());
                    } finally {
                        resourceManager.releaseAssignedContainer(workerNode.getContainerId());
                    }
                    worker.result.completeExceptionally(
                            new IOException("YARN application stopped during worker launch"));
                } catch (Exception failure) {
                    synchronized (this) {
                        workers.remove(workerNode.getWorkerId());
                    }
                    resourceManager.releaseAssignedContainer(workerNode.getContainerId());
                    worker.result.completeExceptionally(failure);
                    throw failure;
                }
            }
        } catch (Exception failure) {
            boolean report;
            synchronized (this) {
                report = active;
                active = false;
                for (PendingWorker worker : pending) {
                    worker.result.completeExceptionally(failure);
                }
            }
            if (report) {
                context.onError(failure);
            }
        }
    }

    @Override
    public void releaseWorker(WorkerRegistration registration) throws Exception {
        YarnWorkerNode worker;
        synchronized (this) {
            worker = workers.remove(registration.getWorkerId());
        }
        if (worker != null) {
            // Remove first, so the expected completed-container event cannot fail the application.
            try {
                nodeManager.stopContainer(worker.getContainerId(), worker.getNodeId());
            } finally {
                resourceManager.releaseAssignedContainer(worker.getContainerId());
            }
        }
    }

    /** Reports the job's terminal state; the runtime releases workers before finishing. */
    @Override
    public synchronized void finish(ApplicationStatus status, String diagnostics) throws Exception {
        active = false;
        if (registered && !finished) {
            FinalApplicationStatus finalStatus =
                    status == ApplicationStatus.SUCCEEDED
                            ? FinalApplicationStatus.SUCCEEDED
                            : status == ApplicationStatus.CANCELED
                                    ? FinalApplicationStatus.KILLED
                                    : FinalApplicationStatus.FAILED;
            resourceManager.unregisterApplicationMaster(
                    finalStatus, diagnostics == null ? "" : diagnostics, "");
            finished = true;
        }
    }

    /**
     * Releases outstanding requests and containers even when initialization or job startup failed.
     */
    @Override
    public void stopWorkers() throws Exception {
        List<PendingWorker> waiting;
        List<String> allocated;
        synchronized (this) {
            active = false;
            if (heartbeats != null) {
                heartbeats.shutdownNow();
            }
            waiting = new ArrayList<>(pending);
            pending.clear();
            allocated = new ArrayList<>(workers.keySet());
        }
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
        if (!allocated.isEmpty()) {
            ExecutorService releases =
                    Executors.newFixedThreadPool(
                            Math.min(32, allocated.size()),
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "seatunnel-yarn-worker-release");
                                thread.setDaemon(true);
                                return thread;
                            });
            List<Future<?>> stopping = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            try {
                for (String worker : allocated) {
                    stopping.add(
                            releases.submit(
                                    () -> {
                                        releaseWorker(new WorkerRegistration(worker));
                                        return null;
                                    }));
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
                releases.shutdownNow();
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
        if (registered && !finished) {
            try {
                finish(
                        ApplicationStatus.FAILED,
                        "Application master stopped before job completion");
            } catch (Exception error) {
                failure = accumulate(failure, error);
            }
        }
        if (nodeManagerInitialized) {
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
        private final CompletableFuture<WorkerRegistration> result;

        private PendingWorker(
                AMRMClient.ContainerRequest request,
                WorkerSpecification specification,
                CompletableFuture<WorkerRegistration> result) {
            this.request = request;
            this.specification = specification;
            this.result = result;
        }
    }
}
