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

package org.apache.seatunnel.resource.yarn.cluster;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriverFactory;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnStagingDirectory;

/** Creates the ApplicationMaster allocation driver through the resource-manager SPI. */
public final class YarnResourceManagerDriverFactory implements ResourceManagerDriverFactory {
    @Override
    public DeployType getDeployType() {
        return DeployType.YARN;
    }

    @Override
    public ResourceManagerDriver create(ApplicationSpecification specification) throws Exception {
        YarnApplicationConfiguration configuration =
                YarnApplicationConfiguration.forApplicationMaster(specification);
        return new YarnResourceManagerDriver(
                YarnConfigurationUtils.loadLocalized(YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME),
                YarnStagingDirectory.fromEnvironment(),
                configuration.getWorkerNodeLabel());
    }
}
