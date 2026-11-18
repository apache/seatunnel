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

package org.apache.seatunnel.resource.yarn.launch.factory;

import org.apache.seatunnel.engine.common.config.EngineConfig.ClusterRole;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.config.YarnContainerSettings;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResourceDescriptor;
import org.apache.seatunnel.resource.yarn.launch.YarnLocalResources;
import org.apache.seatunnel.resource.yarn.launch.decorators.CredentialsDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.DistributionDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.JvmOptionsDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.LoggingDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.RuntimeConfigDecorator;
import org.apache.seatunnel.resource.yarn.launch.decorators.YarnStepDecorator;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResourceRequirements;
import org.apache.seatunnel.resource.yarn.worker.SeatunnelYarnApplicationWorker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.client.api.AMRMClient;

import java.io.IOException;
import java.util.List;

/** Owns the allocation request and NodeManager launch context for a worker. */
public final class WorkerContainerFactory {
    private static final int WORKER_PRIORITY = 0;
    private final Configuration configuration;
    private final Path staging;
    private YarnLocalResourceDescriptor localized;

    public WorkerContainerFactory(Configuration configuration, Path staging) {
        this.configuration = configuration;
        this.staging = staging;
    }

    /**
     * Returns the ordered steps that declare worker-localized files and configure the worker JVM.
     */
    public static List<YarnStepDecorator<YarnLocalResource>> decorators(
            YarnContainerSettings settings) {
        return List.of(
                new DistributionDecorator(),
                new EnvironmentDecorator(),
                new LoggingDecorator(),
                new RuntimeConfigDecorator(settings),
                new JvmOptionsDecorator(),
                new CredentialsDecorator());
    }

    /**
     * Builds a worker launch spec from the application's staged files. The first launch reads file
     * metadata from staging. Each launch gets its own environment, command and current filesystem
     * credentials.
     *
     * @param clusterName Hazelcast cluster name for this application
     * @param masterAddress master endpoint the worker joins
     * @param worker resource and slot requirements for this allocation
     * @param user Hadoop user exported in simple authentication mode
     * @param masterDistributionHome AM's distribution path, used to resolve task jar paths on the
     *     worker
     * @return container spec ready to pass to the allocated NodeManager
     * @throws IOException if the staged manifest or file metadata cannot be read
     */
    public YarnContainerSpec create(
            String clusterName,
            String masterAddress,
            WorkerSpecification worker,
            String user,
            String masterDistributionHome)
            throws IOException {
        YarnLocalResourceDescriptor resources = resources();
        YarnLaunchParameters parameters =
                new YarnLaunchParameters(configuration, staging, user, resources);
        YarnContainerSpec container =
                new YarnContainerSpec(
                        ClusterRole.WORKER,
                        YarnResourceRequirements.forWorker(worker),
                        parameters,
                        SeatunnelYarnApplicationWorker.class.getName(),
                        List.of(
                                clusterName,
                                masterAddress,
                                String.valueOf(worker.getSlots()),
                                masterDistributionHome));
        return container.applyDecorators(decorators(resources.getSettings()));
    }

    private synchronized YarnLocalResourceDescriptor resources() throws IOException {
        if (localized == null) {
            localized = YarnLocalResources.resolve(configuration, staging, false);
        }
        return localized;
    }

    /**
     * Builds the request the driver queues with the RM. Worker slots are configured in the launched
     * JVM. The scheduler receives memory, virtual cores and the node-label expression.
     *
     * @param worker size of the worker container
     * @param nodeLabel resolved worker label, or {@code null} for default placement
     * @return one container request ready for the AM's next allocation heartbeat
     */
    public static AMRMClient.ContainerRequest createRequest(
            WorkerSpecification worker, String nodeLabel) {
        return AMRMClient.ContainerRequest.newBuilder()
                .capability(YarnResourceRequirements.forWorker(worker).toResource())
                .priority(Priority.newInstance(WORKER_PRIORITY))
                .nodeLabelsExpression(nodeLabel)
                .build();
    }
}
