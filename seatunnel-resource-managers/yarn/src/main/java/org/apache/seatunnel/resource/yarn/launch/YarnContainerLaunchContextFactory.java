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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnMasterCli;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnWorkerCli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Creates master and worker launch contexts from shared localization and command factories. */
public final class YarnContainerLaunchContextFactory {
    private YarnContainerLaunchContextFactory() {}

    /**
     * Creates the localized ApplicationMaster context for client-side submission.
     *
     * @param configuration Hadoop settings used to read shared staging storage
     * @param staging application-owned remote staging directory
     * @param memoryMb master container memory used to size its JVM heap
     * @return complete YARN master container launch context
     * @throws Exception if staged local resources cannot be resolved
     */
    public static ContainerLaunchContext master(
            Configuration configuration, Path staging, int memoryMb) throws Exception {
        return master(staging, memoryMb, YarnLocalResources.resolve(configuration, staging));
    }

    /** Creates the master context directly from resources registered during upload. */
    public static ContainerLaunchContext master(
            Path staging, int memoryMb, YarnLocalResourceDescriptor localized) {
        return create(
                localized,
                staging,
                memoryMb,
                SeatunnelYarnMasterCli.class.getName(),
                Collections.emptyList());
    }

    /**
     * Creates a worker context that joins one isolated application master.
     *
     * @param configuration localized Hadoop settings
     * @param staging application-owned remote staging directory
     * @param clusterName isolated Hazelcast cluster name
     * @param masterAddress advertised application master address
     * @param specification per-worker resources and fixed slots
     * @return complete YARN worker container launch context
     * @throws Exception if staged local resources cannot be resolved
     */
    public static ContainerLaunchContext worker(
            Configuration configuration,
            Path staging,
            String clusterName,
            String masterAddress,
            WorkerSpecification specification)
            throws Exception {
        return create(
                YarnLocalResources.resolve(configuration, staging),
                staging,
                specification.getMemoryMb(),
                SeatunnelYarnWorkerCli.class.getName(),
                Arrays.asList(
                        clusterName,
                        masterAddress,
                        String.valueOf(specification.getSlots()),
                        System.getProperty(YarnConstants.SEATUNNEL_HOME_PROPERTY)));
    }

    private static ContainerLaunchContext create(
            YarnLocalResourceDescriptor localized,
            Path staging,
            int memoryMb,
            String mainClass,
            List<String> arguments) {
        return ContainerLaunchContext.newInstance(
                localized.getResources(),
                YarnContainerCommand.environment(staging, localized.getHome()),
                Collections.singletonList(
                        YarnContainerCommand.command(
                                localized.getHome(), memoryMb, mainClass, arguments)),
                null,
                null,
                null);
    }
}
