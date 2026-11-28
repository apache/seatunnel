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
import org.apache.seatunnel.resource.yarn.launch.YarnContainerCommand;
import org.apache.seatunnel.resource.yarn.launch.YarnLaunchParameters;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplicationStaging;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnContainerSpec;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Ships the Hadoop client configuration and supplies SeaTunnel's bootstrap and user environment.
 */
public final class EnvironmentDecorator extends AbstractYarnStepDecorator {
    private static final FsPermission HADOOP_CONFIG_PERMISSION = new FsPermission((short) 0600);

    @Override
    public List<YarnLocalResource> build() {
        YarnLocalResource file =
                YarnLocalResource.file(
                        YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME, this::writeHadoopConfiguration);
        return List.of(file);
    }

    @Override
    public YarnContainerSpec decorate(YarnContainerSpec spec) {
        YarnLaunchParameters parameters = spec.getParameters();
        String workingDirectory = ApplicationConstants.Environment.PWD.$$();
        Map<String, String> environment = spec.getInternalResource().getEnvironment();
        parameters
                .getLocalized()
                .getSettings()
                .getEnvironment()
                .forEach(
                        (name, value) ->
                                environment.put(
                                        name, YarnContainerCommand.escapeDoubleQuoted(value)));
        environment.put(
                YarnConstants.STAGING_DIRECTORY_ENV,
                YarnContainerCommand.escapeDoubleQuoted(parameters.getStaging().toString()));
        environment.put(
                YarnConstants.SEATUNNEL_HOME_ENV,
                workingDirectory
                        + Path.SEPARATOR
                        + YarnContainerCommand.escapeDoubleQuoted(
                                parameters.getLocalized().getHome()));
        environment.put(YarnConstants.HADOOP_CONF_DIR_ENV, workingDirectory);
        environment.put(
                YarnConstants.HADOOP_USER_NAME_ENV,
                YarnContainerCommand.escapeDoubleQuoted(parameters.getUser()));
        return spec;
    }

    private void writeHadoopConfiguration(YarnApplicationStaging staging, Path path)
            throws IOException {
        try (FSDataOutputStream output =
                staging.getFileSystem()
                        .createFile(path)
                        .permission(HADOOP_CONFIG_PERMISSION)
                        .overwrite(false)
                        .build()) {
            staging.getHadoopConfiguration().writeXml(output);
        }
    }
}
