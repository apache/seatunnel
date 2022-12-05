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

package org.apache.seatunnel.engine.server.mapstore;

import static org.awaitility.Awaitility.await;

import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.TimeUnit;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MapStoreTest {

    private HazelcastInstanceImpl instance;

    @Test
    public void testMapStore() {

        String name = this.getClass().getName();
        instance = SeaTunnelServerStarter.createHazelcastInstance(
            TestUtils.getClusterName(name));

        IMap<String, String> mapStore = instance.getMap("engine_mapStore");

        for (int index = 0; index < 100; index++) {
            mapStore.put("key" + index, "value" + index);
        }

        log.info(mapStore.size() + "");

        instance.shutdown();

        instance = SeaTunnelServerStarter.createHazelcastInstance(
            TestUtils.getClusterName(name));

        mapStore = instance.getMap("engine_mapStore");

        log.info(mapStore.size() + "");
        IMap<String, String> finalMapStore = mapStore;
        await().atMost(1, TimeUnit.SECONDS).until(() -> finalMapStore.size() > 0);
    }

    @AfterAll
    public void after() {
        if (instance != null) {
            instance.shutdown();
        }
    }
}
