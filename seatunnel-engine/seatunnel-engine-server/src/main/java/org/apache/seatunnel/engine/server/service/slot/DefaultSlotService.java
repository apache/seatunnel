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

package org.apache.seatunnel.engine.server.service.slot;

import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The slot service of seatunnel server, used for manage slot in worker.
 */
public class DefaultSlotService implements SlotService {

    private NodeEngineImpl nodeEngine;
    private AtomicReference<ResourceProfile> nodeProfile;

    private AtomicReference<ResourceProfile> unassignedResource;

    private AtomicReference<ResourceProfile> assignedResource;

    private Map<Integer, SlotProfile> assignedSlots;

    private Map<Integer, SlotProfile> unassignedSlots;
    private final boolean dynamicSlot;
    private Map<Integer, SlotContext> contexts;

    public DefaultSlotService(NodeEngineImpl nodeEngine, boolean dynamicSlot) {
        this.nodeEngine = nodeEngine;
        this.dynamicSlot = dynamicSlot;
    }

    @Override
    public void init() {
        contexts = new ConcurrentHashMap<>();
        assignedSlots = new ConcurrentHashMap<>();
        unassignedSlots = new ConcurrentHashMap<>();
        nodeProfile = new AtomicReference<>();
        unassignedResource = new AtomicReference<>();
        assignedResource = new AtomicReference<>();
        if (!dynamicSlot) {

        }

    }

    @Override
    public SlotProfile requestSlot(ResourceProfile resourceProfile) {
        SlotProfile profile = selectBestMatchSlot(resourceProfile);
        if (profile != null) {
            assignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::merge);
            unassignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::unmerge);
            unassignedSlots.remove(profile.getSlotID());
            assignedSlots.put(profile.getSlotID(), profile);
            contexts.computeIfAbsent(profile.getSlotID(), p -> new SlotContext(nodeEngine));
        }
        return profile;
    }

    @Override
    public SlotContext getSlotContext(int slotID) {
        return contexts.get(slotID);
    }

    @Override
    public void releaseSlot(SlotProfile profile) {
        assignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::unmerge);
        unassignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::merge);
        unassignedSlots.put(profile.getSlotID(), profile);
        assignedSlots.remove(profile.getSlotID());
        contexts.remove(profile.getSlotID()).close();
    }

    @Override
    public void close() {
        contexts.values().forEach(SlotContext::close);
    }

    private SlotProfile selectBestMatchSlot(ResourceProfile profile) {
        if (unassignedSlots.isEmpty() && !dynamicSlot) {
            return null;
        }
        if (dynamicSlot) {

        } else {
            unassignedSlots
        }
    }

    private ResourceProfile getNodeResource() {
        return new ResourceProfile(CPU.of(0), Memory.of(Runtime.getRuntime().maxMemory()));
    }
}
