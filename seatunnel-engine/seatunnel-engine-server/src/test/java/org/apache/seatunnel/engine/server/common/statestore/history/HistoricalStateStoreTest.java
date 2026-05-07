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

package org.apache.seatunnel.engine.server.common.statestore.history;

import org.apache.seatunnel.engine.server.common.statestore.history.hazelcast.HazelcastHistoricalStateStore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import static org.junit.jupiter.api.Assertions.assertThrows;

class HistoricalStateStoreTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeAll
    static void beforeAll() {
        Config config = new Config();
        config.setClusterName("HistoricalStateStoreTest-" + System.nanoTime());
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

    @AfterAll
    static void afterAll() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void putShouldThrowUnsupportedOperationExceptionWithoutRetentionMetadata() {
        HistoricalStateStore<String, String> store =
                new HazelcastHistoricalStateStore<>(
                        hazelcastInstance.getMap("historical-state-put"));

        assertThrows(UnsupportedOperationException.class, () -> store.put("key", "value"));
    }

    @Test
    void putIfAbsentShouldThrowUnsupportedOperationExceptionWithoutRetentionMetadata() {
        HistoricalStateStore<String, String> store =
                new HazelcastHistoricalStateStore<>(
                        hazelcastInstance.getMap("historical-state-put-if-absent"));

        assertThrows(UnsupportedOperationException.class, () -> store.putIfAbsent("key", "value"));
    }
}
