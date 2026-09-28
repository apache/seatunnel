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
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
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
public abstract class ApplicationResourceManager extends AbstractResourceManager
        implements ResourceManagerContext {

    private final ApplicationId applicationId;
    private final ApplicationSpecification specification;
    private final String clusterName;
    private final Address masterAddress;
    private final ResourceManagerDriver driver;
    private final List<CompletableFuture<WorkerRegistration>> requests = new ArrayList<>();
    private final List<WorkerRegistration> workers = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean workersStopped = new AtomicBoolean();
    private final AtomicBoolean resultPublished = new AtomicBoolean();

    protected ApplicationResourceManager(
            NodeEngine nodeEngine,
            EngineConfig engineConfig,
            ApplicationId applicationId,
            ApplicationSpecification specification,
            String clusterName,
            Address masterAddress,
            ResourceManagerDriver driver) {
        super(nodeEngine, engineConfig);
        this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
        this.specification = Objects.requireNonNull(specification, "specification");
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.masterAddress = Objects.requireNonNull(masterAddress, "masterAddress");
        this.driver = Objects.requireNonNull(driver, "driver");
    }

    /** Initializes the platform driver and submits this application's fixed worker requests. */
    @Override
    protected void initializeResourceManager() throws Exception {
        driver.initialize(this);
        for (int index = 0; index < specification.getWorkerCount(); index++) {
            checkFailure();
            requests.add(requestWorker(specification.getWorkerSpecification()));
        }
    }

    /**
     * Waits until every platform allocation completes and every worker registers with Engine.
     *
     * @param deadlineNanos absolute {@link System#nanoTime()} startup deadline
     */
    public void awaitWorkerRegistration(long deadlineNanos) throws Exception {
        for (CompletableFuture<WorkerRegistration> request : requests) {
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

    /** Stops allocation and releases all platform workers before the Engine master is stopped. */
    public void stopApplicationWorkers() throws Exception {
        if (!workersStopped.compareAndSet(false, true)) {
            return;
        }
        for (CompletableFuture<WorkerRegistration> request : requests) {
            if (!request.isDone()) {
                request.cancel(true);
            }
        }

        Exception cleanupFailure = null;
        List<CompletableFuture<Void>> releases = new ArrayList<>();
        for (WorkerRegistration worker : workers) {
            try {
                releases.add(releaseWorker(worker));
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

    /** Requests one platform worker through the concrete YARN or Kubernetes manager. */
    public abstract CompletableFuture<WorkerRegistration> requestWorker(
            WorkerSpecification specification);

    /** Releases one platform worker through the concrete YARN or Kubernetes manager. */
    public abstract CompletableFuture<Void> releaseWorker(WorkerRegistration worker);

    protected final ResourceManagerDriver getDriver() {
        return driver;
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
    public ApplicationId getApplicationId() {
        return applicationId;
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
        String host = masterAddress.getHost();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + masterAddress.getPort();
    }

    @Override
    public void onError(Throwable error) {
        if (!workersStopped.get()) {
            failure.compareAndSet(null, error);
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
