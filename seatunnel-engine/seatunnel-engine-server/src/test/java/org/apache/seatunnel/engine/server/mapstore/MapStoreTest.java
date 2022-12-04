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

import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;

import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.concurrent.TimeUnit;

@Slf4j
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MapStoreTest extends AbstractSeaTunnelServerTest {

    @Test
    public void testMapStore() {

        IMap<String, String> supplements = instance.getMap("supplements");
        supplements.put("key", "value");
        log.info(supplements.size() + "");
        supplements.evictAll();
        log.info(supplements.size() + "");
        Assertions.assertEquals(0, supplements.size());
        supplements.loadAll(true);
        log.info(supplements.size() + "");
        await().atMost(1, TimeUnit.SECONDS).until(() -> supplements.size() == 1);
    }
}
