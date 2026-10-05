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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.common.statestore.counter.hazelcast.HazelcastCounterStateStore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StateStoreCheckpointIDCounterTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeAll
    static void beforeAll() {
        Config config = new Config();
        config.setClusterName("StateStoreCheckpointIDCounterTest-" + System.nanoTime());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void afterAll() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void startShouldInitializeAndAdvanceCounter() throws Exception {
        HazelcastCounterStateStore<String> store = newStore("checkpoint-id-counter-sequence");
        StateStoreCheckpointIDCounter counter = new StateStoreCheckpointIDCounter(1L, 1, store);

        counter.start();

        assertEquals(1L, counter.get());
        assertEquals(1L, counter.getAndIncrement());
        assertEquals(2L, counter.get());
    }

    @Test
    void absentCounterShouldFailLikeLegacyIMapCounter() {
        HazelcastCounterStateStore<String> store = newStore("checkpoint-id-counter-absent");
        StateStoreCheckpointIDCounter counter = new StateStoreCheckpointIDCounter(2L, 1, store);

        assertThrows(NullPointerException.class, counter::get);
        assertThrows(NullPointerException.class, counter::getAndIncrement);
    }

    @Test
    void shutdownShouldRemoveCounterForEndState() throws Exception {
        HazelcastCounterStateStore<String> store = newStore("checkpoint-id-counter-shutdown");
        StateStoreCheckpointIDCounter counter = new StateStoreCheckpointIDCounter(3L, 1, store);
        String key = StateStoreCheckpointIDCounter.convertLongIntToBase64(3L, 1);

        counter.start();
        counter.shutdown(PipelineStatus.FINISHED).join();

        assertNull(store.get(key));
    }

    private HazelcastCounterStateStore<String> newStore(String mapName) {
        IMap<String, Long> iMap = hazelcastInstance.getMap(mapName);
        iMap.clear();
        return new HazelcastCounterStateStore<>(iMap);
    }
}
