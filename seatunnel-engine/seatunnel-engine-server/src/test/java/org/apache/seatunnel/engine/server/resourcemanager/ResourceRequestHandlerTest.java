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
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.RandomStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResourceRequestHandlerTest {

    @Test
    void collectsMissingIndexesFromSparseResultCache() throws Exception {
        AbstractResourceManager manager = mock(AbstractResourceManager.class);
        when(manager.getEngineConfig()).thenReturn(new EngineConfig());
        List<ResourceProfile> requests =
                Arrays.asList(new ResourceProfile(), new ResourceProfile(), new ResourceProfile());
        ResourceRequestHandler handler =
                new ResourceRequestHandler(
                        1L, requests, new ConcurrentHashMap<>(), manager, new RandomStrategy());

        Assertions.assertEquals(Arrays.asList(0, 1, 2), handler.missingResourceIndexes());

        Field cacheField = ResourceRequestHandler.class.getDeclaredField("resultSlotProfiles");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentMap<Integer, SlotProfile> cache =
                (ConcurrentMap<Integer, SlotProfile>) cacheField.get(handler);
        cache.put(0, mock(SlotProfile.class));

        Assertions.assertEquals(Arrays.asList(1, 2), handler.missingResourceIndexes());
    }
}
