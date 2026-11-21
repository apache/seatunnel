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

/**
 * Master-side leases for fixed slots that have been selected but not yet confirmed by the worker.
 *
 * <p>Concurrent resource handlers read the same heartbeat snapshot. Reserving the chosen slot until
 * the request completes, fails, or the lease expires stops a second handler from treating that slot
 * as free and failing the job with {@link NoEnoughResourceException}.
 */
final class FixedSlotReservations {

    /**
     * Matches Hazelcast's default operation call timeout. A lost callback cannot pin the slot
     * longer than the invocation that was supposed to release it.
     */
    static final long RESERVATION_TTL_NANOS = TimeUnit.MINUTES.toNanos(2);

    private final ConcurrentMap<Key, Long> reservedUntilNanos = new ConcurrentHashMap<>();
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
     * @return the reserved slot, or {@code null} when every matching slot is already reserved
     */
    SlotProfile tryReserve(WorkerProfile worker, ResourceProfile required, long nowNanos) {
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
        reservedUntilNanos.put(
                new Key(worker.getAddress(), selected.getSlotID()),
                nowNanos + RESERVATION_TTL_NANOS);
        return selected;
    }

    boolean isReserved(Address worker, int slotId, long nowNanos) {
        if (worker == null) {
            return false;
        }
        Key key = new Key(worker, slotId);
        Long untilNanos = reservedUntilNanos.get(key);
        if (untilNanos == null) {
            return false;
        }
        if (untilNanos <= nowNanos) {
            reservedUntilNanos.remove(key, untilNanos);
            return false;
        }
        return true;
    }

    void reserveUntil(Address worker, int slotId, long untilNanos) {
        if (worker == null) {
            return;
        }
        reservedUntilNanos.put(new Key(worker, slotId), untilNanos);
    }

    void release(Address worker, int slotId) {
        if (worker == null) {
            return;
        }
        reservedUntilNanos.remove(new Key(worker, slotId));
    }

    void releaseWorker(Address worker) {
        if (worker == null) {
            return;
        }
        reservedUntilNanos.keySet().removeIf(key -> worker.equals(key.worker));
    }

    int activeCount(long nowNanos) {
        int count = 0;
        for (Long untilNanos : reservedUntilNanos.values()) {
            if (untilNanos > nowNanos) {
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
