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

import org.apache.seatunnel.resource.yarn.config.YarnContainerSettings;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/** Materializes the configured files and wires the engine and Hazelcast settings for each role. */
@RequiredArgsConstructor
public final class RuntimeConfigDecorator extends AbstractYarnStepDecorator {
    private final YarnContainerSettings settings;

    @Override
    public List<YarnLocalResource> build() {
        List<YarnLocalResource> files = new ArrayList<>();
        for (String name : settings.getShippedFiles()) {
            YarnLocalResource file =
                    YarnLocalResource.file(
                            name, (staging, target) -> staging.stageShippedFile(name, target));
            files.add(file);
        }
        return files;
    }

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) {
        YarnLaunchParameters parameters = spec.getParameters();
        spec.getCommand()
                .addFileOption(
                        YarnConstants.SYSPROP_SEATUNNEL_CONFIG,
                        parameters.path(YarnOptions.SEATUNNEL_CONFIG_FILE));
        String hazelcastConfigFile;
        if (spec.isMaster()) {
            hazelcastConfigFile = parameters.path(YarnOptions.MASTER_HAZELCAST_CONFIG);
        } else {
            hazelcastConfigFile = parameters.path(YarnOptions.WORKER_HAZELCAST_CONFIG);
        }
        spec.getCommand()
                .addFileOption(YarnConstants.SYSPROP_HAZELCAST_CONFIG, hazelcastConfigFile);
        return spec;
    }
}
