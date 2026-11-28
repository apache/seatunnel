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

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master-side leases for fixed slots that have been selected but not yet confirmed by the worker.
 *
 * <p>Concurrent resource handlers read the same heartbeat snapshot. Reserving the chosen slot until
 * the request completes, fails, or the lease expires stops a second handler from treating that slot
 * as free and failing the job with {@link NoEnoughResourceException}.
 *
 * <p><b>Ownership.</b> {@link #tryReserve} returns an immutable lease token plus the member
 * generation observed at reserve time. Only a release that presents that same token and generation
 * may remove the map entry ({@code remove(key, expectedLease)}). A late callback from an expired
 * owner therefore cannot clear a replacement lease at the same worker/slot key.
 *
 * <p><b>Synchronization.</b> Callers must hold {@link #lock()} across worker selection and {@link
 * #tryReserve}. {@link #isReserved}, token-fenced {@link #release}, and {@link #releaseWorker} use
 * the concurrent map (and {@link #releaseWorker} also takes {@link #lock()} so it cannot race a
 * reserve for a departing member).
 *
 * <p><b>Expiry.</b> {@link #isReserved} compares the stored deadline to {@code nowNanos}. When the
 * deadline has passed it removes that exact lease value and reports the key as free so another
 * handler may reserve it.
 *
 * <p><b>Member removal.</b> {@link #releaseWorker} bumps the per-address member generation and
 * drops every lease for that address. A callback that still holds a pre-removal token cannot match
 * a later lease created after the worker rejoins.
 *
 * <p><b>Who may release.</b> The in-flight request callback releases after a success or null-slot
 * response (once the worker snapshot has been applied), on send failure, and on callback setup
 * failure. {@link AbstractResourceManager#memberRemoved} releases through {@link #releaseWorker}.
 * Idle expiry is observed lazily on the next {@link #isReserved} probe; there is no background
 * sweeper.
 *
 * <p><b>Timeout assumption.</b> {@link #RESERVATION_TTL_NANOS} matches Hazelcast's <em>default</em>
 * operation call timeout so a lost callback cannot pin a slot forever on an unmodified cluster. It
 * is not an invariant for customized deployments: invocation timeouts are configurable and callback
 * processing can lag. Token fencing, not the TTL alone, protects a live replacement lease.
 */
final class FixedSlotReservations {

    /**
     * Default lease length aligned with Hazelcast's default operation call timeout. Not a
     * cluster-wide guarantee when that timeout is raised; see the class javadoc.
     */
    static final long RESERVATION_TTL_NANOS = TimeUnit.MINUTES.toNanos(2);

    private final ConcurrentMap<Key, Lease> reserved = new ConcurrentHashMap<>();
    private final ConcurrentMap<Address, Long> memberGenerations = new ConcurrentHashMap<>();
    private final AtomicLong nextToken = new AtomicLong(1);
    private final Object lock = new Object();

    Object lock() {
        return lock;
    }

    /**
     * Returns true when {@code worker} still has an unreserved fixed slot that can satisfy {@code
     * required}. Expired leases are dropped as they are observed.
     */
    boolean hasMatchingSlot(WorkerProfile worker, ResourceProfile required, long nowNanos) {
        SlotProfile[] slots = worker.getUnassignedSlots();
        if (slots == null) {
            return false;
        }
        for (SlotProfile slot : slots) {
            if (isAvailable(worker, slot, required, nowNanos)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reserves the lowest matching slot id on {@code worker}. Caller must hold {@link #lock()}.
     *
     * @return the reservation (slot + ownership token), or {@code null} when every matching slot is
     *     already reserved
     */
    FixedSlotReservation tryReserve(WorkerProfile worker, ResourceProfile required, long nowNanos) {
        if (worker.getAddress() == null) {
            return null;
        }
        SlotProfile[] slots = worker.getUnassignedSlots();
        if (slots == null) {
            return null;
        }
        SlotProfile selected = null;
        for (SlotProfile slot : slots) {
            if (!isAvailable(worker, slot, required, nowNanos)) {
                continue;
            }
            if (selected == null || slot.getSlotID() < selected.getSlotID()) {
                selected = slot;
            }
        }
        if (selected == null) {
            return null;
        }
        long generation = memberGenerations.getOrDefault(worker.getAddress(), 0L);
        long token = nextToken.getAndIncrement();
        reserved.put(
                new Key(worker.getAddress(), selected.getSlotID()),
                new Lease(token, nowNanos + RESERVATION_TTL_NANOS, generation));
        return new FixedSlotReservation(selected, token, generation);
    }

    boolean isReserved(Address worker, int slotId, long nowNanos) {
        if (worker == null) {
            return false;
        }
        Key key = new Key(worker, slotId);
        Lease lease = reserved.get(key);
        if (lease == null) {
            return false;
        }
        if (lease.untilNanos <= nowNanos) {
            reserved.remove(key, lease);
            return false;
        }
        return true;
    }

    FixedSlotReservation reserveUntil(Address worker, int slotId, long untilNanos) {
        if (worker == null) {
            return null;
        }
        long generation = memberGenerations.getOrDefault(worker, 0L);
        long token = nextToken.getAndIncrement();
        reserved.put(new Key(worker, slotId), new Lease(token, untilNanos, generation));
        return new FixedSlotReservation(null, token, generation, worker, slotId);
    }

    void overrideDeadline(FixedSlotReservation reservation, long untilNanos) {
        if (reservation == null || reservation.getWorker() == null) {
            return;
        }
        Key key = new Key(reservation.getWorker(), reservation.getSlotID());
        reserved.computeIfPresent(
                key,
                (ignored, lease) -> {
                    if (lease.token != reservation.getToken()
                            || lease.memberGeneration != reservation.getMemberGeneration()) {
                        return lease;
                    }
                    return new Lease(lease.token, untilNanos, lease.memberGeneration);
                });
    }

    void release(FixedSlotReservation reservation) {
        if (reservation == null || reservation.getWorker() == null) {
            return;
        }
        Key key = new Key(reservation.getWorker(), reservation.getSlotID());
        reserved.computeIfPresent(
                key,
                (ignored, lease) -> {
                    if (lease.token == reservation.getToken()
                            && lease.memberGeneration == reservation.getMemberGeneration()) {
                        return null;
                    }
                    return lease;
                });
    }

    void releaseWorker(Address worker) {
        if (worker == null) {
            return;
        }
        synchronized (lock) {
            memberGenerations.merge(worker, 1L, Long::sum);
            reserved.keySet().removeIf(key -> worker.equals(key.worker));
        }
    }

    int activeCount(long nowNanos) {
        int count = 0;
        for (Lease lease : reserved.values()) {
            if (lease.untilNanos > nowNanos) {
                count++;
            }
        }
        return count;
    }

    private boolean isAvailable(
            WorkerProfile worker, SlotProfile slot, ResourceProfile required, long nowNanos) {
        if (slot == null || slot.getResourceProfile() == null || worker.getAddress() == null) {
            return false;
        }
        if (slot.getWorker() == null || !worker.getAddress().equals(slot.getWorker())) {
            return false;
        }
        return slot.getResourceProfile().enoughThan(required)
                && !isReserved(worker.getAddress(), slot.getSlotID(), nowNanos);
    }

    static final class FixedSlotReservation {
        private final SlotProfile slot;
        private final Address worker;
        private final int slotId;
        private final long token;
        private final long memberGeneration;

        FixedSlotReservation(SlotProfile slot, long token, long memberGeneration) {
            this(
                    slot,
                    token,
                    memberGeneration,
                    slot == null ? null : slot.getWorker(),
                    slot == null ? -1 : slot.getSlotID());
        }

        private FixedSlotReservation(
                SlotProfile slot, long token, long memberGeneration, Address worker, int slotId) {
            this.slot = slot;
            this.token = token;
            this.memberGeneration = memberGeneration;
            this.worker = worker;
            this.slotId = slotId;
        }

        SlotProfile getSlot() {
            return slot;
        }

        Address getWorker() {
            return worker;
        }

        int getSlotID() {
            return slotId;
        }

        long getToken() {
            return token;
        }

        long getMemberGeneration() {
            return memberGeneration;
        }
    }

    private static final class Lease {
        private final long token;
        private final long untilNanos;
        private final long memberGeneration;

        private Lease(long token, long untilNanos, long memberGeneration) {
            this.token = token;
            this.untilNanos = untilNanos;
            this.memberGeneration = memberGeneration;
        }
    }

    private static final class Key {
        private final Address worker;
        private final int slotId;

        private Key(Address worker, int slotId) {
            this.worker = worker;
            this.slotId = slotId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            Key key = (Key) other;
            return slotId == key.slotId && worker.equals(key.worker);
        }

        @Override
        public int hashCode() {
            return Objects.hash(worker, slotId);
        }
    }
}
