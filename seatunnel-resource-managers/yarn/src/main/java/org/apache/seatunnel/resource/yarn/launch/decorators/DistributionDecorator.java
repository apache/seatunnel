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

package org.apache.seatunnel.resource.yarn.launch.decorators;

import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplicationStaging;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/** Publishes the inspected archive's manifest and sets the SeaTunnel home in each container. */
public final class DistributionDecorator extends AbstractYarnStepDecorator {

    @Override
    public List<YarnLocalResource> build() {
        return List.of(YarnLocalResource.distribution(this::writeManifest));
    }

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) {
        YarnLaunchParameters parameters = spec.getParameters();
        spec.getCommand()
                .addFileOption(
                        YarnConstants.SYSPROP_SEATUNNEL_HOME, parameters.getLocalized().getHome());
        return spec;
    }

    private void writeManifest(YarnApplicationStaging staging, Path target) throws IOException {
        Properties manifest = new Properties();
        staging.getDeployment()
                .getContainerSettings()
                .writeTo(manifest, staging.getDistribution()::containsFile);
        staging.getDistribution().writeManifest(staging.getFileSystem(), target, manifest);
    }
}
