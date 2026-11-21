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

import org.apache.seatunnel.resource.yarn.config.YarnContainerSettings;
import org.apache.seatunnel.resource.yarn.launch.decorators.YarnStepDecorator;
import org.apache.seatunnel.resource.yarn.launch.factory.ApplicationMasterContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.factory.WorkerContainerFactory;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Resolves the files YARN must localize before starting a master or worker container. */
public final class YarnLocalResources {
    private YarnLocalResources() {}

    /**
     * Reads the staged manifest and the size and modification time of each file for one role. The
     * AM also receives its application specification and any separately staged job file.
     *
     * @param configuration Hadoop settings used to read shared staging storage
     * @param staging application-owned remote staging directory
     * @param forMaster whether to resolve the AM's resource set
     * @return localized resource map and distribution home relative to the container directory
     * @throws IOException if staged metadata or file status cannot be read
     */
    public static YarnLocalResourceDescriptor resolve(
            Configuration configuration, Path staging, boolean forMaster) throws IOException {
        try (FileSystem fileSystem = FileSystem.newInstance(staging.toUri(), configuration)) {
            return resolve(fileSystem, staging, forMaster);
        }
    }

    static YarnLocalResourceDescriptor resolve(
            FileSystem fileSystem, Path staging, boolean forMaster) throws IOException {
        Properties manifest = YarnDistribution.readManifest(fileSystem, staging);
        YarnDistribution distribution = YarnDistribution.fromManifest(staging, manifest);
        YarnContainerSettings settings = YarnContainerSettings.read(manifest);
        List<YarnStepDecorator<YarnLocalResource>> decorators;
        if (forMaster) {
            boolean hasJob =
                    fileSystem.exists(new Path(staging, YarnConstants.LOCALIZED_JOB_CONFIG_NAME));
            decorators = ApplicationMasterContainerFactory.decorators(settings, hasJob);
        } else {
            decorators = WorkerContainerFactory.decorators(settings);
        }
        return resolve(fileSystem, staging, distribution, settings, collect(decorators));
    }

    /**
     * Collects file declarations in the role's step order. Staging and metadata lookup perform the
     * I/O.
     */
    public static List<YarnLocalResource> collect(
            List<YarnStepDecorator<YarnLocalResource>> decorators) {
        List<YarnLocalResource> resources = new ArrayList<>();
        for (YarnStepDecorator<YarnLocalResource> decorator : decorators) {
            resources.addAll(decorator.build());
        }
        return resources;
    }

    private static YarnLocalResourceDescriptor resolve(
            FileSystem fileSystem,
            Path staging,
            YarnDistribution distribution,
            YarnContainerSettings settings,
            List<YarnLocalResource> files)
            throws IOException {
        Map<String, LocalResource> resources = new HashMap<>();
        for (YarnLocalResource resource : files) {
            resource.resolve(fileSystem, staging, distribution);
            resources.put(resource.getName(), resource.getInternalResource());
        }
        return new YarnLocalResourceDescriptor(resources, distribution.localizedHome(), settings);
    }
}
