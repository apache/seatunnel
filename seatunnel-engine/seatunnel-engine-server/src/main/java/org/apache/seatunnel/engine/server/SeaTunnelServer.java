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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import lombok.NonNull;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SeaTunnelServer implements ManagedService, MembershipAwareService, LiveOperationsTracker {
    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);
    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LiveOperationRegistry liveOperationRegistry;

    private volatile SlotService slotService;
    private TaskExecutionService taskExecutionService;
    private CoordinatorService coordinatorService;

    private final ExecutorService executorService;

    private final SeaTunnelConfig seaTunnelConfig;

    public SeaTunnelServer(@NonNull Node node, @NonNull SeaTunnelConfig seaTunnelConfig) {
        this.logger = node.getLogger(getClass());
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        this.executorService =
            Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("seatunnel-server-executor-%d").build());
        logger.info("SeaTunnel server start...");
    }

    /**
     * Lazy load for Slot Service
     */
    public SlotService getSlotService() {
        if (slotService == null) {
            synchronized (this) {
                if (slotService == null) {
                    SlotService service = new DefaultSlotService(nodeEngine, taskExecutionService, true, 2);
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
        taskExecutionService.start();
        getSlotService();
        coordinatorService = new CoordinatorService(nodeEngine, executorService);
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> printExecutionInfo(), 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        if (slotService != null) {
            slotService.close();
        }
        if (coordinatorService != null) {
            coordinatorService.shutdown();
        }
        executorService.shutdown();
        taskExecutionService.shutdown();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {

    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (coordinatorService.isCoordinatorActive()) {
            coordinatorService.getResourceManager().memberRemoved(event);
        }
    }

    @Override
    public void populate(LiveOperations liveOperations) {

    }

    /**
     * Used for debugging on call
     */
    public String printMessage(String message) {
        this.logger.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public CoordinatorService getCoordinatorService() {
        int retryCount = 0;
        while (!coordinatorService.isCoordinatorActive() && retryCount < 20) {
            try {
                Thread.sleep(1000);
                retryCount++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        if (!coordinatorService.isCoordinatorActive()) {
            throw new SeaTunnelEngineException("Can not get coordinator service from an inactive master node.");
        }
        return coordinatorService;
    }

    public TaskExecutionService getTaskExecutionService() {
        return taskExecutionService;
    }

    public void failedTaskOnMemberRemoved(MembershipServiceEvent event) {
        if (coordinatorService.isCoordinatorActive()) {
            coordinatorService.failedTaskOnMemberRemoved(event);
        }
    }

    private void printExecutionInfo() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
        int activeCount = threadPoolExecutor.getActiveCount();
        int corePoolSize = threadPoolExecutor.getCorePoolSize();
        int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
        int poolSize = threadPoolExecutor.getPoolSize();
        long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
        long taskCount = threadPoolExecutor.getTaskCount();
        StringBuffer sbf = new StringBuffer();
        sbf.append("activeCount=")
            .append(activeCount)
            .append("\n")
            .append("corePoolSize=")
            .append(corePoolSize)
            .append("\n")
            .append("maximumPoolSize=")
            .append(maximumPoolSize)
            .append("\n")
            .append("poolSize=")
            .append(poolSize)
            .append("\n")
            .append("completedTaskCount=")
            .append(completedTaskCount)
            .append("\n")
            .append("taskCount=")
            .append(taskCount);
        logger.info(sbf.toString());
    }
}
