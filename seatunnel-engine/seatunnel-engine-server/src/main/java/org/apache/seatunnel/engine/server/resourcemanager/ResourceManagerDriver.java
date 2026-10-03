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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * Owns external worker allocations for one application independently of SeaTunnel's slot scheduler.
 *
 * <p>The application runtime invokes initialize once, requests a fixed number of workers, releases
 * them during shutdown, invokes stopWorkers to drain late allocations, publishes the terminal
 * outcome with finish, and invokes close before the outer lifecycle stops the master. Drivers may
 * use background callbacks, but must synchronize them with lifecycle calls and stop failure
 * notifications during intentional release and shutdown. No method may alter another application's
 * resources. This contract provides no worker replacement, scaling, or master HA.
 */
public interface ResourceManagerDriver<WorkerType extends ResourceIDRetrievable>
        extends AutoCloseable {
    /**
     * Initializes platform clients and registers the application master where required.
     *
     * @param resourceEventHandler receives unexpected worker terminations and driver failures
     * @param mainThreadExecutor serializes resource callbacks and schedules observation; never run
     *     blocking platform calls on this executor
     * @param ioExecutor executes blocking platform calls; owned by the resource manager, like the
     *     main-thread executor, and must not be shut down by the driver
     * @param masterAddress supplies the current bound master endpoint, including IPv6 brackets
     * @throws Exception if initialization fails; close is still invoked to clean partial state
     */
    void initialize(
            ResourceEventHandler<WorkerType> resourceEventHandler,
            ScheduledExecutorService mainThreadExecutor,
            Executor ioExecutor,
            Supplier<String> masterAddress)
            throws Exception;

    /**
     * Requests and launches one worker with the supplied resource requirements.
     *
     * <p>Successful completion means the external process was launched, not that it registered with
     * Hazelcast or SeaTunnel's slot scheduler. The runtime checks registration separately. Complete
     * exceptionally on allocation or launch failure and publish a worker-terminated event for later
     * unexpected worker death. Cancellation means the allocation is no longer required; pending
     * requests and any resources allocated concurrently with cancellation must be reclaimed no
     * later than close.
     *
     * @param specification immutable memory, CPU, and slot requirements for this worker
     * @return a non-null future containing the launched worker's platform identity
     */
    CompletableFuture<WorkerType> requestWorker(WorkerSpecification specification);

    /**
     * Releases a worker belonging to this application; repeated release must be harmless.
     *
     * @param worker node returned by a successful requestWorker call
     * @return a future completed when release succeeds, or completed exceptionally if release
     *     fails; close must retry outstanding cleanup
     */
    CompletableFuture<Void> releaseWorker(WorkerType worker);

    /**
     * Quiesces allocation and reclaims resources that cannot be handled by explicit worker release.
     *
     * <p>Called after cancellation of pending futures and release of known workers, but before the
     * native master stops. Stop allocation admission and monitoring, drain in-flight launches, and
     * reclaim every late or ambiguously created resource. Keep clients required by finish usable.
     * Implementations must bound SDK calls and draining; all shutdown steps share the runtime's
     * cleanup deadline. Repeated calls must be harmless, including a fallback invocation from
     * close.
     *
     * <p>The default is suitable only for drivers whose requests are synchronous and whose explicit
     * release fully reclaims all resources. Asynchronous platform drivers must override it.
     *
     * @throws Exception if allocations cannot be drained or any owned resource cannot be reclaimed
     */
    default void stopWorkers() throws Exception {}

    /**
     * Publishes the terminal outcome after native job termination and worker cleanup.
     *
     * <p>This is called at most once and before close, including after partial initialization. A
     * backend without an explicit application deregistration operation may keep the default no-op.
     * Platform clients required for reporting must remain usable until this method returns.
     *
     * @param status terminal result, including any worker-release or engine-cleanup failure
     * @param diagnostics failure details, or null when the job completed without diagnostics
     * @throws Exception if the platform cannot accept the terminal result
     */
    default void finish(ApplicationStatus status, String diagnostics) throws Exception {}

    /**
     * Stops callbacks, cancels pending requests, releases all owned resources, and closes clients.
     *
     * <p>Must tolerate partial initialization and repeated invocation. Attempt all cleanup even if
     * one operation fails; aggregate failures rather than silently leaving resources allocated.
     * Invoke stopWorkers as a fallback if quiescence has not already completed. Client-only close
     * failures are returned to the caller, but a previously published platform result may be final.
     *
     * @throws Exception if any owned resource or platform client cannot be cleaned up
     */
    @Override
    void close() throws Exception;
}
