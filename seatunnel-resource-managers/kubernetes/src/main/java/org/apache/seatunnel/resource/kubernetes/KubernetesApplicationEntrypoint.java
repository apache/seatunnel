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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.core.starter.seatunnel.application.ApplicationRuntime;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationResult;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.ApplicationStatus;
import org.apache.seatunnel.resource.kubernetes.cluster.KubernetesResourceManagerDriverFactory;

import java.nio.file.Paths;

/** Kubernetes Job process: one native Zeta application and its fixed worker allocation. */
public final class KubernetesApplicationEntrypoint {
    private KubernetesApplicationEntrypoint() {}

    /**
     * Runs one job and maps its final outcome to the Kubernetes Job process exit code.
     *
     * @param args application ID followed by the mounted specification path
     * @throws Exception if arguments, localized configuration or driver initialization fail
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected application id and specification path");
        }
        ApplicationId id = new ApplicationId(DeployType.KUBERNETES, args[0]);
        ApplicationSpecification specification = ApplicationSpecification.read(Paths.get(args[1]));
        ResourceManagerDriver driver =
                new KubernetesResourceManagerDriverFactory().create(specification);
        ApplicationResult result = ApplicationRuntime.run(id, specification, driver);
        // Kubernetes observes the process code and persists Complete or Failed on the owner Job.
        if (result.getStatus() != ApplicationStatus.SUCCEEDED) {
            System.exit(1);
        }
    }
}
