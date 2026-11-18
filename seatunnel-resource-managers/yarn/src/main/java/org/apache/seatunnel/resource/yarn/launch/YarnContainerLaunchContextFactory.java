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

package org.apache.seatunnel.resource.yarn.launch;

import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.launch.factory.ApplicationMasterContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.factory.WorkerContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnResourceRequirements;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

/** Convenience entry points for constructing individual AM and worker launch contexts. */
public final class YarnContainerLaunchContextFactory {
    private YarnContainerLaunchContextFactory() {}

    /**
     * Resolves the AM files from shared staging and uses the supplied Hadoop configuration to
     * obtain delegation tokens on a Kerberos cluster.
     */
    public static ContainerLaunchContext master(
            Configuration configuration, Path staging, int memoryMb, String user) throws Exception {
        return ApplicationMasterContainerFactory.create(
                        configuration,
                        staging,
                        new YarnResourceRequirements(
                                memoryMb, ApplicationOptions.MASTER_CPU_CORES.defaultValue()),
                        YarnLocalResources.resolve(configuration, staging, true),
                        user)
                .getInternalResource();
    }

    /**
     * Builds a simple-authentication AM context from resource metadata already held by the caller.
     */
    public static ContainerLaunchContext master(
            Path staging, int memoryMb, YarnLocalResourceDescriptor localized, String user) {
        return ApplicationMasterContainerFactory.create(
                        new Configuration(false),
                        staging,
                        new YarnResourceRequirements(
                                memoryMb, ApplicationOptions.MASTER_CPU_CORES.defaultValue()),
                        localized,
                        user)
                .getInternalResource();
    }

    /**
     * Builds a single worker context under the AM's Hadoop identity. The running AM keeps a {@link
     * WorkerContainerFactory} to reuse staged metadata across worker launches.
     */
    public static ContainerLaunchContext worker(
            Configuration configuration,
            Path staging,
            String clusterName,
            String masterAddress,
            WorkerSpecification specification,
            String user,
            String masterDistributionHome)
            throws Exception {
        return new WorkerContainerFactory(configuration, staging)
                .create(clusterName, masterAddress, specification, user, masterDistributionHome)
                .getInternalResource();
    }
}
