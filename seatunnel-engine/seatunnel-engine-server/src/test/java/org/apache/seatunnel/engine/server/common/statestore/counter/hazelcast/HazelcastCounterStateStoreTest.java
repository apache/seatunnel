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

package org.apache.seatunnel.engine.server.common.statestore.counter.hazelcast;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HazelcastCounterStateStoreTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeAll
    static void beforeAll() {
        Config config = new Config();
        config.setClusterName("HazelcastCounterStateStoreTest-" + System.nanoTime());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void afterAll() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void absentCounterShouldStayAbsent() {
        IMap<String, Long> iMap = hazelcastInstance.getMap("counter-state-store-absent");
        iMap.clear();
        HazelcastCounterStateStore<String> store = new HazelcastCounterStateStore<>(iMap);

        assertNull(store.get("missing"));
        assertNull(store.incrementAndGet("missing"));
        assertNull(store.addAndGet("missing", 3L));
        assertNull(store.advanceToAtLeast("missing", 10L));
        assertFalse(iMap.containsKey("missing"));
    }

    @Test
    void initializeIfAbsentShouldOnlySucceedOnce() {
        IMap<String, Long> iMap = hazelcastInstance.getMap("counter-state-store-initialize");
        iMap.clear();
        HazelcastCounterStateStore<String> store = new HazelcastCounterStateStore<>(iMap);

        assertTrue(store.initializeIfAbsent("counter", 1L));
        assertFalse(store.initializeIfAbsent("counter", 99L));
        assertEquals(1L, store.get("counter"));
    }

    @Test
    void incrementAndGetShouldAdvanceExistingCounter() {
        IMap<String, Long> iMap = hazelcastInstance.getMap("counter-state-store-increment");
        iMap.clear();
        HazelcastCounterStateStore<String> store = new HazelcastCounterStateStore<>(iMap);

        store.set("counter", 7L);

        assertEquals(8L, store.incrementAndGet("counter"));
        assertEquals(8L, store.get("counter"));
    }

    @Test
    void addAndGetShouldAdvanceExistingCounterByDelta() {
        IMap<String, Long> iMap = hazelcastInstance.getMap("counter-state-store-add");
        iMap.clear();
        HazelcastCounterStateStore<String> store = new HazelcastCounterStateStore<>(iMap);

        store.set("counter", 7L);

        assertEquals(12L, store.addAndGet("counter", 5L));
        assertEquals(12L, store.get("counter"));
    }

    @Test
    void advanceToAtLeastShouldNeverRewindExistingCounter() {
        IMap<String, Long> iMap = hazelcastInstance.getMap("counter-state-store-minimum");
        iMap.clear();
        HazelcastCounterStateStore<String> store = new HazelcastCounterStateStore<>(iMap);

        store.set("counter", 19L);
        assertEquals(24L, store.advanceToAtLeast("counter", 24L));
        assertEquals(24L, store.advanceToAtLeast("counter", 13L));
        assertEquals(25L, store.incrementAndGet("counter"));
        assertEquals(25L, store.advanceToAtLeast("counter", 24L));
        assertEquals(25L, store.get("counter"));
    }

    @Test
    void advancementShouldRetryAfterSuccessorWinsCompareAndSet() {
        List<Long> observed = new ArrayList<>();
        assertEquals(
                31L,
                advanceWithInterference(
                        "counter-state-store-cas-retry", map -> map.put("counter", 31L), observed));
        assertEquals(Arrays.asList(19L, 31L), observed);
        assertEquals(31L, hazelcastInstance.getMap("counter-state-store-cas-retry").get("counter"));
    }

    @Test
    void advancementShouldNotRecreateCounterRemovedBeforeCompareAndSet() {
        List<Long> observed = new ArrayList<>();
        assertNull(
                advanceWithInterference(
                        "counter-state-store-cas-remove", map -> map.remove("counter"), observed));
        assertEquals(Arrays.asList(19L, null), observed);
        assertFalse(
                hazelcastInstance.getMap("counter-state-store-cas-remove").containsKey("counter"));
    }

    private Long advanceWithInterference(
            String mapName, Consumer<IMap<String, Long>> interference, List<Long> observed) {
        IMap<String, Long> backing = hazelcastInstance.getMap(mapName);
        backing.put("counter", 19L);
        IMap<String, Long> delayed = Mockito.spy(backing);
        AtomicBoolean first = new AtomicBoolean(true);
        Mockito.doAnswer(
                        call -> {
                            BiFunction<String, Long, Long> advance = call.getArgument(1);
                            // Keep Hazelcast's real compute/CAS loop, injecting a competing write
                            // after its first read and before its conditional replacement.
                            return backing.compute(
                                    "counter",
                                    (key, current) -> {
                                        observed.add(current);
                                        Long next = advance.apply(key, current);
                                        if (first.compareAndSet(true, false)) {
                                            interference.accept(backing);
                                        }
                                        return next;
                                    });
                        })
                .when(delayed)
                .compute(ArgumentMatchers.eq("counter"), ArgumentMatchers.any());
        return new HazelcastCounterStateStore<>(delayed).advanceToAtLeast("counter", 24L);
    }
}
