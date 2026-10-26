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

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.RequestSlotOperationStats;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.MembershipServiceEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Master-side registry and allocator for slots supplied by registered Engine workers.
 *
 * <p>The coordinator owns one manager, initializes it before scheduling, and closes it when its
 * coordinator lifecycle ends. Scheduling requests, worker heartbeats, and membership callbacks can
 * arrive concurrently. Worker profiles are the latest reported observations, not an atomic cluster
 * snapshot or a reservation: the worker's slot service validates and owns each actual assignment.
 * Keep the allocation identity in a returned {@link SlotProfile} through deployment and release.
 *
 * <p>Application mode retains this slot scheduling path. Its {@link ResourceManagerDriver} owns
 * external worker processes; releasing an Engine slot does not terminate a container or pod, and
 * closing this manager does not reclaim those platform resources. Future callbacks may run on
 * completion threads; callers must synchronize shared state and observe exceptional completion.
 */
public interface ResourceManager {
    /**
     * Initializes the registry by synchronizing worker profiles from current cluster members.
     *
     * <p>The coordinator calls this once before publishing the manager to scheduling callers. The
     * current implementation waits for synchronization RPCs; completion does not prevent later
     * membership or heartbeat changes. Initialization failures propagate as runtime exceptions.
     */
    void init();

    /**
     * Requests one worker-owned slot for a job using the configured allocation strategy.
     *
     * @param jobId job that will own the granted slot
     * @param resourceProfile CPU and memory requirements for the requested slot
     * @param tagFilter required worker attributes; null or empty matches all registered workers
     * @return future completed with an acknowledged slot assignment, or exceptionally on allocation
     *     or worker communication failure
     * @throws NoEnoughResourceException if no eligible worker is available when requesting
     *     resources; insufficient capacity discovered asynchronously also fails the returned future
     */
    CompletableFuture<SlotProfile> applyResource(
            long jobId, ResourceProfile resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException;

    /**
     * Requests the slots needed by a job's task groups from matching registered workers.
     *
     * <p>Completion requires all requested assignments. The existing request handler attempts to
     * release partial allocations when allocation fails; failure completion is not a guarantee that
     * every rollback RPC has already completed. Successful callers own the returned allocations and
     * must release them when their task groups no longer need them.
     *
     * @param jobId job that will own the granted slots
     * @param resourceProfile nonempty list of requested slot resource requirements
     * @param tagFilter required worker attributes; null or empty matches all registered workers
     * @return future containing the granted slot profiles after all requests succeed, or an
     *     exceptional completion describing allocation or communication failure
     * @throws NoEnoughResourceException if no eligible worker is available when requesting
     *     resources; insufficient capacity discovered asynchronously also fails the returned future
     */
    CompletableFuture<List<SlotProfile>> applyResources(
            long jobId, List<ResourceProfile> resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException;

    /**
     * Releases a collection of job-owned slot assignments through their worker slot services.
     *
     * <p>Each release is attempted. The aggregate future fails if any individual release fails;
     * other releases are not rolled back. An empty list completes successfully.
     *
     * @param jobId owner job used by workers to validate each release
     * @param profiles assignments previously granted to this job, with allocation identities intact
     * @return future completed after all release futures finish, exceptionally if any fails
     */
    CompletableFuture<Void> releaseResources(long jobId, List<SlotProfile> profiles);

    /**
     * Releases a single assignment and refreshes the master's view from the worker's response.
     *
     * <p>The worker validates the slot, owning job, and allocation sequence. A stale or duplicate
     * release can therefore fail; this operation has no general idempotency guarantee. If the
     * worker has already left the cluster, the current implementation completes without sending an
     * RPC. This releases Engine capacity only and does not stop the worker process.
     *
     * @param jobId expected owner of this allocation
     * @param profile assignment to release, retaining its worker address and allocation sequence
     * @return future completed after release acknowledgment and profile refresh, or exceptionally
     *     when ownership validation or communication fails
     */
    CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile);

    /**
     * Checks the latest registered worker profile for the same slot allocation and owner job.
     *
     * <p>A slot released and reassigned, including to another task group of the same job, does not
     * match the supplied allocation sequence. This is a cached check, not a remote reservation or a
     * guarantee against a concurrent release; the worker still validates task deployment.
     *
     * @param profile assignment whose worker, slot ID, owner job, and sequence must still match
     * @return true when that assignment appears in the worker's latest reported assigned slots
     */
    boolean slotActiveCheck(SlotProfile profile);

    /**
     * Registers or refreshes the latest worker resources and slot assignments reported to the
     * master.
     *
     * <p>Called by worker heartbeats and slot-operation responses. In the current implementation a
     * previously unknown worker is first asked to reset its resource state, so this call can wait
     * for an RPC and propagate a runtime communication failure. Reports and membership events may
     * race; callers must not interpret the resulting registry as a linearizable cluster snapshot.
     *
     * @param workerProfile current worker address, attributes, resources, assignments, and load
     */
    void heartbeat(WorkerProfile workerProfile);

    /**
     * Removes a departed member from the master's worker registry.
     *
     * <p>This handles Engine membership only. Job failover and application-level process cleanup
     * belong to their respective lifecycle owners.
     *
     * @param event Hazelcast membership callback identifying the departed member
     */
    void memberRemoved(MembershipServiceEvent event);

    /**
     * Ends this manager's coordinator-owned lifecycle.
     *
     * <p>The current implementation marks the manager stopped, allowing local registration waits to
     * end. It does not drain outstanding allocation futures, release all slots, stop workers, or
     * close platform clients. The owner must arrange job and platform cleanup separately and must
     * not submit new scheduling requests after closing.
     */
    void close();

    /**
     * Collects currently reported free slots from workers matching the supplied attributes.
     *
     * @param tags required worker attributes; null or empty matches every registered worker
     * @return a new list of reported profiles; contained profiles are not deep copies or
     *     reservations
     */
    List<SlotProfile> getUnassignedSlots(Map<String, String> tags);

    /**
     * Collects currently reported assigned slots from workers matching the supplied attributes.
     *
     * @param tags required worker attributes; null or empty matches every registered worker
     * @return a new list of reported profiles; contained profiles are not deep copies and may
     *     become stale as workers allocate or release slots
     */
    List<SlotProfile> getAssignedSlots(Map<String, String> tags);

    /**
     * Counts registered worker profiles whose attributes satisfy every supplied tag.
     *
     * @param tags required worker attributes; null or empty matches every registered worker
     * @return the observed matching worker count, not a count of Hazelcast members or free slots
     */
    int workerCount(Map<String, String> tags);

    /**
     * Exposes the existing live worker registry indexed by Hazelcast address.
     *
     * <p>The map can change concurrently with heartbeats and membership callbacks. Callers should
     * treat it and the contained profiles as read-only observations; direct mutation bypasses the
     * manager's registration and worker-state handling.
     *
     * @return the manager-owned concurrent registry, not a detached snapshot
     */
    ConcurrentMap<Address, WorkerProfile> getRegisterWorker();

    /**
     * Captures master-side counters and latency measurements for slot-request operations.
     *
     * @return an observability snapshot of successful, capacity-rejected, and failed worker RPCs;
     *     these statistics do not represent platform container or pod allocation
     */
    RequestSlotOperationStats getRequestSlotOperationStats();
}
