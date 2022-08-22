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

import org.apache.seatunnel.engine.server.resourcemanager.heartbeat.HeartbeatListener;
import org.apache.seatunnel.engine.server.resourcemanager.heartbeat.HeartbeatManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public abstract class AbstractResourceManager implements ResourceManager {

    private final Map<String, WorkerProfile> registerWorker;

    private final WorkerHeartbeatListener listener;

    private final HeartbeatManager heartbeatManager;

    public AbstractResourceManager() {
        this.registerWorker = new ConcurrentHashMap<>();
        this.listener = new WorkerHeartbeatListener();
        // TODO just use hazelcast member config server?
        this.heartbeatManager = new HeartbeatManager(listener);
        heartbeatManager.start(Executors.newSingleThreadScheduledExecutor());
    }

    @Override
    public CompletableFuture<SlotProfile> applyResource(long jobId, ResourceProfile resourceProfile) {
        return null;
    }

    @Override
    public CompletableFuture<Void>[] releaseResources(long jobId, SlotProfile[] profiles) {
        return new CompletableFuture[0];
    }

    @Override
    public CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile) {
        return null;
    }

    @Override
    public void workerRegister(WorkerProfile workerProfile) {
        registerWorker.put(workerProfile.getWorkerID(), workerProfile);
        heartbeatFromWorker(workerProfile.getWorkerID());
    }

    @Override
    public void heartbeatFromWorker(String workerID) {
        heartbeatManager.heartbeat(workerID);
    }

    private class WorkerHeartbeatListener implements HeartbeatListener {
        @Override
        public void nodeDisconnected(String nodeID) {
            heartbeatManager.removeNode(nodeID);
            registerWorker.remove(nodeID);
        }
    }
}
