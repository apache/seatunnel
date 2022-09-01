package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.master.JobMaster;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * CoordinatorServer is instantiated on any node, but only runs on the master node
 */
public class CoordinatorServer {
    private static final ILogger LOGGER = Logger.getLogger(CoordinatorServer.class);

    private final NodeEngineImpl nodeEngine;
    private final ExecutorService executorService;
    private final SeaTunnelServer seaTunnelServer;

    public CoordinatorServer(
        @NonNull NodeEngineImpl nodeEngine,
        @NonNull ExecutorService executorService,
        @NonNull SeaTunnelServer seaTunnelServer
    ) {
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        this.seaTunnelServer = seaTunnelServer;
    }


    /**
     * key: job id;
     * <br> value: job master;
     */
    private Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    /**
     * call by client to submit job
     */
    public PassiveCompletableFuture<Void> submitJob(long jobId, Data jobImmutableInformation) {
        CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
        JobMaster jobMaster = new JobMaster(jobImmutableInformation, this.nodeEngine, executorService,
            seaTunnelServer.getResourceManager());
        executorService.submit(() -> {
            try {
                jobMaster.init();
                jobMaster.getPhysicalPlan().initStateFuture();
                runningJobMasterMap.put(jobId, jobMaster);
            } catch (Throwable e) {
                LOGGER.severe(String.format("submit job %s error %s ", jobId, ExceptionUtils.getMessage(e)));
                voidCompletableFuture.completeExceptionally(e);
            } finally {
                // We specify that when init is complete, the submitJob is complete
                voidCompletableFuture.complete(null);
            }

            try {
                jobMaster.run();
            } finally {
                runningJobMasterMap.remove(jobId);
            }
        });
        return new PassiveCompletableFuture(voidCompletableFuture);
    }

    public PassiveCompletableFuture<JobStatus> waitForJobComplete(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // TODO Get Job Status from JobHistoryStorage
            CompletableFuture<JobStatus> future = new CompletableFuture<>();
            future.complete(JobStatus.FINISHED);
            return new PassiveCompletableFuture<>(future);
        } else {
            return runningJobMaster.getJobMasterCompleteFuture();
        }
    }

    public PassiveCompletableFuture<Void> cancelJob(long jodId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jodId);
        if (runningJobMaster == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return new PassiveCompletableFuture<>(future);
        } else {
            return new PassiveCompletableFuture<>(CompletableFuture.supplyAsync(() -> {
                runningJobMaster.cancelJob();
                return null;
            }));
        }
    }

    public JobStatus getJobStatus(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // TODO Get Job Status from JobHistoryStorage
            return JobStatus.FINISHED;
        }
        return runningJobMaster.getJobStatus();
    }

    public JobMaster getJobMaster(Long jobId) {
        return runningJobMasterMap.get(jobId);
    }
}
