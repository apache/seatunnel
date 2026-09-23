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

import org.apache.seatunnel.core.starter.seatunnel.application.ApplicationWorker;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.YarnApplicationMaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Localizes the exact same distribution for the master and every worker. */
public final class YarnContainerLaunch {
    /** Directory name under which YARN expands the localized SeaTunnel distribution archive. */
    static final String LOCALIZED_DISTRIBUTION_NAME = "seatunnel";
    /** Localized job specification shared by the submitting client and ApplicationMaster. */
    public static final String LOCALIZED_SPECIFICATION_NAME = "application.properties";
    /** Merged Hadoop settings loaded from each container's private working directory. */
    public static final String LOCALIZED_HADOOP_CONFIG_NAME = "hadoop-conf.xml";
    /** Remote directory owned by this application and removed during terminal cleanup. */
    public static final String STAGING_DIRECTORY_ENV = "SEATUNNEL_YARN_STAGING";

    private YarnContainerLaunch() {}

    /** Creates the localized, quoted ApplicationMaster command for client-side submission. */
    public static ContainerLaunchContext master(
            Configuration configuration, Path staging, int memoryMb) throws Exception {
        return create(
                configuration,
                staging,
                memoryMb,
                YarnApplicationMaster.class.getName(),
                Collections.emptyList());
    }

    static ContainerLaunchContext worker(
            Configuration configuration,
            Path staging,
            String clusterName,
            String masterAddress,
            WorkerSpecification specification)
            throws Exception {
        return create(
                configuration,
                staging,
                specification.getMemoryMb(),
                ApplicationWorker.class.getName(),
                Arrays.asList(
                        clusterName,
                        masterAddress,
                        String.valueOf(specification.getSlots()),
                        System.getProperty("seatunnel.home")));
    }

    private static ContainerLaunchContext create(
            Configuration configuration,
            Path staging,
            int memoryMb,
            String mainClass,
            List<String> arguments)
            throws Exception {
        Map<String, LocalResource> resources = new HashMap<>();
        String home;
        try (FileSystem fileSystem = FileSystem.newInstance(staging.toUri(), configuration)) {
            YarnDistribution distribution = YarnDistribution.read(fileSystem, staging);
            home = distribution.localizedHome();
            resources.put(
                    LOCALIZED_DISTRIBUTION_NAME,
                    resource(fileSystem, distribution.archive(staging), LocalResourceType.ARCHIVE));
            resources.put(
                    LOCALIZED_SPECIFICATION_NAME,
                    resource(
                            fileSystem,
                            new Path(staging, LOCALIZED_SPECIFICATION_NAME),
                            LocalResourceType.FILE));
            resources.put(
                    LOCALIZED_HADOOP_CONFIG_NAME,
                    resource(
                            fileSystem,
                            new Path(staging, LOCALIZED_HADOOP_CONFIG_NAME),
                            LocalResourceType.FILE));
        }
        Map<String, String> environment = new HashMap<>();
        environment.put(STAGING_DIRECTORY_ENV, staging.toString());
        // ApplicationConstants expands on the NodeManager, never on the submitting host.
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        environment.put("SEATUNNEL_HOME", workingDirectory + "/" + home);
        environment.put("HADOOP_CONF_DIR", workingDirectory);
        String classpath =
                ".:"
                        + home
                        + "config:"
                        + home
                        + "starter/seatunnel-starter.jar:"
                        + home
                        + "starter/logging/*:"
                        + home
                        + "lib/*:"
                        + home
                        + "resource-managers/yarn/*";
        StringBuilder command =
                new StringBuilder("\"")
                        .append(ApplicationConstants.Environment.JAVA_HOME.$$())
                        .append("/bin/java\" -Xmx")
                        .append(Math.max(64L, memoryMb * 3L / 4))
                        .append("m -Dseatunnel.home=")
                        .append("\"")
                        .append(workingDirectory)
                        .append("\"/")
                        .append(quote(home))
                        .append(" -Dhazelcast.logging.type=log4j2 -Dlog4j2.configurationFile=")
                        .append(quote(home + "config/log4j2_client.properties"))
                        .append(" -cp ")
                        .append(quote(classpath))
                        .append(' ')
                        .append(quote(mainClass));
        for (String argument : arguments) {
            command.append(' ').append(quote(argument));
        }
        command.append(" 1>")
                .append(quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"))
                .append(" 2>")
                .append(quote(ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        return ContainerLaunchContext.newInstance(
                resources,
                environment,
                Collections.singletonList(command.toString()),
                null,
                null,
                null);
    }

    static LocalResource resource(FileSystem fileSystem, Path path, LocalResourceType type)
            throws Exception {
        FileStatus status = fileSystem.getFileStatus(path);
        return LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(fileSystem.makeQualified(path)),
                type,
                LocalResourceVisibility.APPLICATION,
                status.getLen(),
                status.getModificationTime());
    }

    static String quote(String value) {
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}
