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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.core.starter.seatunnel.application.ApplicationRuntime;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.yarn.cluster.YarnContainerLaunch;
import org.apache.seatunnel.resource.yarn.cluster.YarnResourceManagerDriverFactory;
import org.apache.seatunnel.resource.yarn.cluster.YarnStagingDirectory;
import org.apache.seatunnel.resource.yarn.config.YarnConfigurationUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/** The YARN AM owns the native Zeta master and a single submitted job. */
public final class YarnApplicationMaster {
    private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMaster.class);

    private YarnApplicationMaster() {}

    /** Runs the native job and releases staged artifacts on normal or interrupted shutdown. */
    public static void main(String[] args) throws Exception {
        Configuration configuration =
                YarnConfigurationUtils.loadLocalized(
                        YarnContainerLaunch.LOCALIZED_HADOOP_CONFIG_NAME);
        Path staging = YarnStagingDirectory.fromEnvironment();
        Thread cleanup =
                new Thread(
                        () -> {
                            try {
                                YarnStagingDirectory.cleanup(configuration, staging);
                            } catch (Exception failure) {
                                LOG.warn(
                                        "Could not remove application staging directory {}",
                                        staging,
                                        failure);
                            }
                        },
                        "seatunnel-yarn-staging-cleanup");
        Runtime.getRuntime().addShutdownHook(cleanup);
        ApplicationResult result;
        try {
            String container = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
            ApplicationId id =
                    new ApplicationId(
                            DeployType.YARN,
                            ContainerId.fromString(container)
                                    .getApplicationAttemptId()
                                    .getApplicationId()
                                    .toString());
            ApplicationSpecification specification =
                    ApplicationSpecification.read(
                            Paths.get(YarnContainerLaunch.LOCALIZED_SPECIFICATION_NAME));
            result =
                    ApplicationRuntime.run(
                            id,
                            specification,
                            new YarnResourceManagerDriverFactory().create(specification));
        } finally {
            YarnStagingDirectory.cleanup(configuration, staging);
            try {
                Runtime.getRuntime().removeShutdownHook(cleanup);
            } catch (IllegalStateException ignored) {
                // The VM already started the hook, which performs the same idempotent cleanup.
            }
        }
        System.exit(result.getStatus() == ApplicationStatus.SUCCEEDED ? 0 : 1);
    }
}
