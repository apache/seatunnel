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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.HashMap;
import java.util.Map;

/** Resolves the files YARN must localize before starting a master or worker container. */
final class YarnLocalResources {
    private YarnLocalResources() {}

    /**
     * Loads the staged distribution manifest and creates application-private local resources.
     *
     * @param configuration Hadoop settings used to read shared staging storage
     * @param staging application-owned remote staging directory
     * @return localized resource map and distribution home relative to the container directory
     * @throws Exception if staged metadata or file status cannot be read
     */
    static YarnLocalResourceDescriptor resolve(Configuration configuration, Path staging)
            throws Exception {
        Map<String, LocalResource> resources = new HashMap<>();
        String home;
        try (FileSystem fileSystem = FileSystem.newInstance(staging.toUri(), configuration)) {
            YarnDistribution distribution = YarnDistribution.read(fileSystem, staging);
            home = distribution.localizedHome();
            resources.put(
                    YarnConstants.LOCALIZED_DISTRIBUTION_NAME,
                    resource(fileSystem, distribution.archive(staging), LocalResourceType.ARCHIVE));
            resources.put(
                    YarnConstants.LOCALIZED_SPECIFICATION_NAME,
                    resource(
                            fileSystem,
                            new Path(staging, YarnConstants.LOCALIZED_SPECIFICATION_NAME),
                            LocalResourceType.FILE));
            resources.put(
                    YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME,
                    resource(
                            fileSystem,
                            new Path(staging, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME),
                            LocalResourceType.FILE));
        }
        return new YarnLocalResourceDescriptor(resources, home);
    }

    private static LocalResource resource(FileSystem fileSystem, Path path, LocalResourceType type)
            throws Exception {
        FileStatus status = fileSystem.getFileStatus(path);
        return LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(fileSystem.makeQualified(path)),
                type,
                LocalResourceVisibility.APPLICATION,
                status.getLen(),
                status.getModificationTime());
    }
}
