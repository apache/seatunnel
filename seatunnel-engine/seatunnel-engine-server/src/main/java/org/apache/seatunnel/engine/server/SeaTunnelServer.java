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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;

import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import lombok.NonNull;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SeaTunnelServer implements ManagedService, MembershipAwareService, LiveOperationsTracker {

    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);

    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private final LiveOperationRegistry liveOperationRegistry;

    private volatile SlotService slotService;
    private TaskExecutionService taskExecutionService;
    private CoordinatorService coordinatorService;
    private ScheduledExecutorService monitorService;

    private final SeaTunnelConfig seaTunnelConfig;

    private volatile boolean isRunning = true;

    public SeaTunnelServer(@NonNull SeaTunnelConfig seaTunnelConfig) {
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        LOGGER.info("SeaTunnel server start...");
    }

    /**
     * Lazy load for Slot Service
     */
    public SlotService getSlotService() {
        if (slotService == null) {
            synchronized (this) {
                if (slotService == null) {
                    SlotService service = new DefaultSlotService(nodeEngine, taskExecutionService, seaTunnelConfig.getEngineConfig().getSlotServiceConfig());
                    service.init();
                    slotService = service;
                }
            }
        }
        return slotService;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        // TODO Determine whether to execute there method on the master node according to the deploy type
        taskExecutionService = new TaskExecutionService(
            nodeEngine, nodeEngine.getProperties()
        );
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(taskExecutionService);
        taskExecutionService.start();
        getSlotService();
        coordinatorService = new CoordinatorService(nodeEngine, this, seaTunnelConfig.getEngineConfig());
        monitorService = Executors.newSingleThreadScheduledExecutor();
        monitorService.scheduleAtFixedRate(this::printExecutionInfo, 0, seaTunnelConfig.getEngineConfig().getPrintExecutionInfoInterval(), TimeUnit.SECONDS);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        isRunning = false;
        if (taskExecutionService != null) {
            taskExecutionService.shutdown();
        }
        if (monitorService != null) {
            monitorService.shutdownNow();
        }
        if (slotService != null) {
            slotService.close();
        }
        if (coordinatorService != null) {
            coordinatorService.shutdown();
        }
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {

    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        try {
            if (isMasterNode()) {
                this.getCoordinatorService().memberRemoved(event);
            }
        } catch (SeaTunnelEngineException e) {
            LOGGER.severe("Error when handle member removed event", e);
        }
    }

    @Override
    public void populate(LiveOperations liveOperations) {

    }

    /**
     * Used for debugging on call
     */
    public String printMessage(String message) {
        LOGGER.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public CoordinatorService getCoordinatorService() {
        int retryCount = 0;
        if (isMasterNode()) {
            // TODO the retry count and sleep time need configurable
            while (!coordinatorService.isCoordinatorActive() && retryCount < 20 && isRunning) {
                try {
                    LOGGER.warning("This is master node, waiting the coordinator service init finished");
                    Thread.sleep(1000);
                    retryCount++;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (coordinatorService.isCoordinatorActive()) {
                return coordinatorService;
            }

            throw new SeaTunnelEngineException("Can not get coordinator service from an active master node.");
        } else {
            throw new SeaTunnelEngineException("Please don't get coordinator service from an inactive master node");
        }
    }

    public TaskExecutionService getTaskExecutionService() {
        return taskExecutionService;
    }

    /**
     * return whether task is end
     *
     * @param taskGroupLocation taskGroupLocation
     */
    public boolean taskIsEnded(@NonNull TaskGroupLocation taskGroupLocation) {
        IMap<Object, Object> runningJobState =
            nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);

        Object taskState = runningJobState.get(taskGroupLocation);
        return taskState != null && ((ExecutionState) taskState).isEndState();
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public boolean isMasterNode() {
        // must retry until the cluster have master node
        try {
            return RetryUtils.retryWithException(() -> {
                return nodeEngine.getMasterAddress().equals(nodeEngine.getThisAddress());
            }, new RetryUtils.RetryMaterial(20, true,
                exception -> exception instanceof NullPointerException && isRunning, 1000));
        } catch (InterruptedException e) {
            LOGGER.info("master node check interrupted");
            return false;
        } catch (Exception e) {
            throw new SeaTunnelEngineException("cluster have no master node", e);
        }
    }

    private void printExecutionInfo() {
        coordinatorService.printExecutionInfo();
    }
}
