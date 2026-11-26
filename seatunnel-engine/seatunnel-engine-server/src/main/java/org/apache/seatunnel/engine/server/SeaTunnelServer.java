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

import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineRetryableException;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.common.SeaTunnelEngineContext;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.hazelcast.HazelcastEngineStateStores;
import org.apache.seatunnel.engine.server.common.statestore.metrics.MetricsSnapshotStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.observability.RealtimeMetricsService;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.rest.service.BaseService;
import org.apache.seatunnel.engine.server.service.jar.ConnectorPackageService;
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.telemetry.log.TaskLogManagerService;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

import org.apache.hadoop.fs.FileSystem;

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
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class SeaTunnelServer
        implements ManagedService, MembershipAwareService, LiveOperationsTracker {

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);

    /** Pause between two checks whether the coordinator service became active. */
    private static final long COORDINATOR_ACTIVATION_POLL_MILLIS = 200L;

    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    @Getter private SeaTunnelEngineContext engineContext;
    @Getter private NodeEngineImpl nodeEngine;
    @Getter private final LiveOperationRegistry liveOperationRegistry;

    private volatile SlotService slotService;
    @Getter private TaskExecutionService taskExecutionService;
    @Getter private ClassLoaderService classLoaderService;
    private CoordinatorService coordinatorService;
    @Getter private CheckpointService checkpointService;
    @Getter private CheckpointMonitorService checkpointMonitorService;
    @Getter private ScheduledExecutorService monitorService;
    @Getter private volatile RealtimeMetricsService realtimeMetricsService;
    private volatile JettyService jettyService;
    @Getter private TaskLogManagerService taskLogManagerService;

    @Getter private SeaTunnelHealthMonitor seaTunnelHealthMonitor;

    @Getter private final SeaTunnelConfig seaTunnelConfig;
    @Getter private final ResourceManagerFactory resourceManagerFactory;

    private volatile boolean isRunning = true;

    @Getter private EventService eventService;

    /**
     * Constructs an uninitialized server.
     *
     * @param seaTunnelConfig Engine and cluster configuration retained for initialization
     */
    public SeaTunnelServer(@NonNull SeaTunnelConfig seaTunnelConfig) {
        this(seaTunnelConfig, new ResourceManagerFactory());
    }

    public SeaTunnelServer(
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull ResourceManagerFactory resourceManagerFactory) {
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        this.resourceManagerFactory = resourceManagerFactory;
    }

    /** Lazy load for Slot Service */
    public SlotService getSlotService() {
        // If the node is master node, the slot service is not needed.
        if (EngineConfig.ClusterRole.MASTER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            return null;
        }

        if (slotService == null) {
            synchronized (this) {
                if (slotService == null) {
                    SlotService service =
                            new DefaultSlotService(
                                    nodeEngine,
                                    taskExecutionService,
                                    seaTunnelConfig.getEngineConfig().getSlotServiceConfig());
                    service.init();
                    slotService = service;
                }
            }
        }
        return slotService;
    }

    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        BaseService.retainRunningJobDagJsonCache();
        // TODO Determine whether to execute there method on the master node according to the deploy
        // type

        EngineStateStores stateStores =
                new HazelcastEngineStateStores(
                        nodeEngine,
                        seaTunnelConfig.getEngineConfig().getJobMetricsPartitionCount());
        this.engineContext = SeaTunnelEngineContext.builder(stateStores).build();

        classLoaderService =
                new DefaultClassLoaderService(
                        seaTunnelConfig.getEngineConfig().isClassloaderCacheMode(), nodeEngine);

        eventService = new EventService(nodeEngine);

        if (EngineConfig.ClusterRole.MASTER_AND_WORKER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            startWorker();
            startMaster();

        } else if (EngineConfig.ClusterRole.WORKER.ordinal()
                == seaTunnelConfig.getEngineConfig().getClusterRole().ordinal()) {
            startWorker();
        } else {
            startMaster();
        }

        seaTunnelHealthMonitor = new SeaTunnelHealthMonitor(((NodeEngineImpl) engine).getNode());

        // task log manager service
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs() != null
                && seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs().isEnabled()) {
            taskLogManagerService =
                    new TaskLogManagerService(
                            seaTunnelConfig.getEngineConfig().getTelemetryConfig().getLogs());
            taskLogManagerService.initClean();
        }

        // Start Jetty server
        if (seaTunnelConfig.getEngineConfig().getHttpConfig().isEnabled()
                || seaTunnelConfig.getEngineConfig().getHttpConfig().isEnableHttps()) {
            jettyService = new JettyService(nodeEngine, seaTunnelConfig);
            jettyService.createJettyServer();
        }

        // a trick way to fix StatisticsDataReferenceCleaner thread class loader leak.
        // see https://issues.apache.org/jira/browse/HADOOP-19049
        FileSystem.Statistics statistics = new FileSystem.Statistics("SeaTunnel");
    }

    private void startMaster() {
        checkpointService =
                new CheckpointService(seaTunnelConfig.getEngineConfig().getCheckpointConfig());
        int monitorHistorySize =
                seaTunnelConfig.getEngineConfig().getCheckpointConfig().getMonitorHistorySize();
        if (monitorHistorySize
                > ServerConfigOptions.MasterServerConfigOptions
                        .CHECKPOINT_MONITOR_HISTORY_SIZE_WARN_THRESHOLD) {
            // Legal but worth saying out loud: every checkpoint event rewrites
            // the whole checkpoint overview entry in the IMap, so a large
            // window is paid on every trigger and acknowledgement rather than
            // once. Logged here, at the only place the value is read, so the
            // line appears in the master log next to the rest of startup.
            log.warn(
                    "seatunnel.engine.checkpoint.monitor-history-size is set to {}, above the "
                            + "recommended {}. The checkpoint overview is rewritten in full on "
                            + "every checkpoint event, so a window this large costs master CPU, "
                            + "network and backup traffic proportional to history size x running "
                            + "pipelines. Lower it unless you need that much history.",
                    monitorHistorySize,
                    ServerConfigOptions.MasterServerConfigOptions
                            .CHECKPOINT_MONITOR_HISTORY_SIZE_WARN_THRESHOLD);
        }
        checkpointMonitorService = new CheckpointMonitorService(engineContext, monitorHistorySize);
        monitorService = Executors.newSingleThreadScheduledExecutor();
        coordinatorService =
                new CoordinatorService(
                        nodeEngine,
                        this,
                        engineContext,
                        seaTunnelConfig.getEngineConfig(),
                        resourceManagerFactory);
        monitorService.scheduleAtFixedRate(
                this::printExecutionInfo,
                0,
                seaTunnelConfig.getEngineConfig().getPrintExecutionInfoInterval(),
                TimeUnit.SECONDS);
    }

    private void startWorker() {
        taskExecutionService =
                new TaskExecutionService(
                        classLoaderService, nodeEngine, engineContext, eventService);
        nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(taskExecutionService);
        taskExecutionService.start();
        getSlotService();
    }

    @Override
    public void reset() {}

    @Override
    public void shutdown(boolean terminate) {
        isRunning = false;

        if (jettyService != null) {
            jettyService.shutdownJettyServer();
        }
        BaseService.releaseRunningJobDagJsonCache();
        if (taskExecutionService != null) {
            taskExecutionService.shutdown();
        }
        if (classLoaderService != null) {
            classLoaderService.close();
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
        stopRealtimeMetricsService();

        if (eventService != null) {
            eventService.shutdownNow();
        }

        MetadataProviderManager.closeProviders();
        engineContext.close();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {}

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
    public void populate(LiveOperations liveOperations) {}

    /** Used for debugging on call */
    public String printMessage(String message) {
        LOGGER.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public CoordinatorService getCoordinatorService() {
        int retryCount = 0;
        if (isMasterNode()) {
            int maxRetry = 3;
            int retryPause = 500;
            while (isRunning
                    && retryCount < maxRetry
                    && !coordinatorService.isCoordinatorActive()
                    && isMasterNode()) {
                try {
                    LOGGER.warning(
                            "This is master node, waiting the coordinator service init finished");
                    Thread.sleep(retryPause);
                    retryCount++;
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            if (coordinatorService.isCoordinatorActive()) {
                return coordinatorService;
            }

            if (!isMasterNode()) {
                throw new SeaTunnelEngineException("This is not a master node now.");
            }
            // Return retryable exception to retry from the worker node, because the coordinator is
            // not ready yet. By this way, we can release the operation thread and retry later.
            throw new SeaTunnelEngineRetryableException(
                    "Can not get coordinator service from an active master node.");
        } else {
            throw new SeaTunnelEngineException(
                    "Please don't get coordinator service from an inactive master node");
        }
    }

    /**
     * Waits until this member's coordinator service has finished initialization.
     *
     * <p>{@link #getCoordinatorService()} only tolerates a short activation window because its
     * cluster-mode callers retry on a higher level. Callers that cannot retry, such as the
     * application master that runs exactly one job, wait with their own startup budget instead.
     *
     * @param timeoutMillis budget for the coordinator service to become active
     * @return the active coordinator service
     * @throws SeaTunnelEngineRetryableException if the budget expires before activation
     * @throws SeaTunnelEngineException if this member stopped being the master
     * @throws InterruptedException if the wait is interrupted
     */
    public CoordinatorService awaitCoordinatorService(long timeoutMillis)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (true) {
            if (!isMasterNode()) {
                throw new SeaTunnelEngineException("This is not a master node now.");
            }
            if (isCoordinatorActive()) {
                return coordinatorService;
            }
            if (System.nanoTime() >= deadline) {
                throw new SeaTunnelEngineRetryableException(
                        "Can not get coordinator service from an active master node.");
            }
            Thread.sleep(COORDINATOR_ACTIVATION_POLL_MILLIS);
        }
    }

    synchronized void startRealtimeMetricsService(CoordinatorService activeCoordinatorService) {
        if (realtimeMetricsService != null) {
            return;
        }
        realtimeMetricsService =
                new RealtimeMetricsService((NodeEngineImpl) nodeEngine, activeCoordinatorService);
        realtimeMetricsService.start();
    }

    synchronized void stopRealtimeMetricsService() {
        if (realtimeMetricsService != null) {
            realtimeMetricsService.shutdown();
            realtimeMetricsService = null;
        }
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

    public boolean isMasterNode() {
        // must retry until the cluster have master node
        try {
            return Boolean.TRUE.equals(
                    RetryUtils.retryWithException(
                            () -> nodeEngine.getThisAddress().equals(nodeEngine.getMasterAddress()),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    exception ->
                                            isRunning && exception instanceof NullPointerException,
                                    Constant.OPERATION_RETRY_SLEEP)));
        } catch (InterruptedException e) {
            LOGGER.info("master node check interrupted");
            return false;
        } catch (Exception e) {
            throw new SeaTunnelEngineException("cluster have no master node", e);
        }
    }

    private void printExecutionInfo() {
        coordinatorService.printExecutionInfo();
        if (coordinatorService.isCoordinatorActive() && this.isMasterNode()) {
            coordinatorService.printJobDetailInfo();
        }
    }

    public void updateMetrics(Map<TaskLocation, SeaTunnelMetricsContext> localMap) {
        MetricsSnapshotStateStore metricsSnapshotStateStore =
                engineContext.getStateStores().metricsSnapshotStore();
        metricsSnapshotStateStore.merge(localMap);
    }

    public void removeMetrics(PipelineLocation pipelineLocation) {
        MetricsSnapshotStateStore metricsSnapshotStateStore =
                engineContext.getStateStores().metricsSnapshotStore();
        metricsSnapshotStateStore.removePipeline(pipelineLocation);
    }

    public boolean isCoordinatorActive() {
        return coordinatorService.isCoordinatorActive();
    }

    /** Returns this member's bound HTTP port, or the configured port before Jetty is available. */
    public int getHttpPort() {
        JettyService service = jettyService;
        return service == null
                ? seaTunnelConfig.getEngineConfig().getHttpConfig().getPort()
                : service.getHttpPort();
    }

    public ConnectorPackageService getConnectorPackageService() {
        return getCoordinatorService().getConnectorPackageService();
    }

    public ThreadPoolStatus getThreadPoolStatusMetrics() {
        return coordinatorService.getThreadPoolStatusMetrics();
    }
}
