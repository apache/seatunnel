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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlanUtils;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;

import java.util.concurrent.ExecutorService;

public class JobMaster implements Runnable {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private LogicalDag logicalDag;
    private PhysicalPlan physicalPlan;
    private final Data jobImmutableInformation;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private FlakeIdGenerator flakeIdGenerator;

    public JobMaster(@NonNull Data jobImmutableInformation,
                     @NonNull NodeEngine nodeEngine,
                     @NonNull ExecutorService executorService) {
        this.jobImmutableInformation = jobImmutableInformation;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
            this.nodeEngine.getHazelcastInstance().getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);
    }

    public void init() throws Exception {
        JobImmutableInformation jobInformation = nodeEngine.getSerializationService().toObject(jobImmutableInformation);
        LOGGER.info("Job [" + jobInformation.getJobId() + "] submit");
        LOGGER.info("Job [" + jobInformation.getJobId() + "] jar urls " + jobInformation.getPluginJarsUrls());

        // TODO Use classloader load the connector jars and deserialize logicalDag
        this.logicalDag = new LogicalDag();
        physicalPlan = PhysicalPlanUtils.fromLogicalDAG(logicalDag,
                nodeEngine,
                jobInformation,
                System.currentTimeMillis(),
                executorService,
                flakeIdGenerator);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void run() {
        try {
            LOGGER.info("I will sleep 2000ms");
            Thread.sleep(2000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
