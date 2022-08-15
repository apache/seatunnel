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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.loader.SeatunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanUtils;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.SimpleResourceManager;
import org.apache.seatunnel.engine.server.scheduler.JobScheduler;
import org.apache.seatunnel.engine.server.scheduler.PipelineBaseScheduler;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class JobMaster implements Runnable {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private LogicalDag logicalDag;
    private PhysicalPlan physicalPlan;
    private final Data jobImmutableInformationData;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private FlakeIdGenerator flakeIdGenerator;

    private ResourceManager resourceManager;

    private CompletableFuture<JobStatus> jobMasterCompleteFuture = new CompletableFuture<>();

    private JobImmutableInformation jobImmutableInformation;

    public JobMaster(@NonNull Data jobImmutableInformationData,
                     @NonNull NodeEngine nodeEngine,
                     @NonNull ExecutorService executorService) {
        this.jobImmutableInformationData = jobImmutableInformationData;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
            this.nodeEngine.getHazelcastInstance().getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);

        this.resourceManager = new SimpleResourceManager();
    }

    public void init() throws Exception {
        jobImmutableInformation = nodeEngine.getSerializationService().toObject(
            jobImmutableInformationData);
        LOGGER.info("Job [" + jobImmutableInformation.getJobId() + "] submit");
        LOGGER.info(
            "Job [" + jobImmutableInformation.getJobId() + "] jar urls " + jobImmutableInformation.getPluginJarsUrls());

        if (!CollectionUtils.isEmpty(jobImmutableInformation.getPluginJarsUrls())) {
            this.logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(nodeEngine.getSerializationService(),
                    new SeatunnelChildFirstClassLoader(jobImmutableInformation.getPluginJarsUrls()),
                    jobImmutableInformation.getLogicalDag());
        } else {
            this.logicalDag = nodeEngine.getSerializationService().toObject(jobImmutableInformation.getLogicalDag());
        }
        physicalPlan = PhysicalPlanUtils.fromLogicalDAG(logicalDag,
            nodeEngine,
            jobImmutableInformation,
            System.currentTimeMillis(),
            executorService,
            flakeIdGenerator);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void run() {
        try {
            PassiveCompletableFuture<JobStatus> jobStatusPassiveCompletableFuture =
                physicalPlan.getJobEndCompletableFuture();

            jobStatusPassiveCompletableFuture.whenComplete((v, t) -> {
                // We need not handle t, Because we will not return t from physicalPlan
                if (JobStatus.FAILING.equals(v)) {
                    cleanJob();
                    physicalPlan.updateJobState(JobStatus.FAILING, JobStatus.FAILED);
                }
                jobMasterCompleteFuture.complete(physicalPlan.getJobStatus());
            });

            JobScheduler jobScheduler = new PipelineBaseScheduler(physicalPlan, this);
            jobScheduler.startScheduling();
        } catch (Throwable e) {
            LOGGER.severe(String.format("Job %s (%s) run error with: %s",
                physicalPlan.getJobImmutableInformation().getJobConfig().getName(),
                physicalPlan.getJobImmutableInformation().getJobId(),
                ExceptionUtils.getMessage(e)));
            // try to cancel job
            physicalPlan.cancelJob();
        } finally {
            jobMasterCompleteFuture.join();
        }
    }

    public void cleanJob() {
        // TODO clean something
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public PassiveCompletableFuture<JobStatus> getJobMasterCompleteFuture() {
        return new PassiveCompletableFuture<>(jobMasterCompleteFuture);
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }
}
