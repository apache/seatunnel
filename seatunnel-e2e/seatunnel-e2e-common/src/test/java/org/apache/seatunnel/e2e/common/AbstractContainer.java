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

package org.apache.seatunnel.e2e.common;

import static org.apache.seatunnel.e2e.common.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.ContainerUtil.adaptPathForWin;
import static org.apache.seatunnel.e2e.common.ContainerUtil.copyConfigFileToContainer;
import static org.apache.seatunnel.e2e.common.ContainerUtil.copyConnectorJarToContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractContainer {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractContainer.class);
    protected static final String START_ROOT_MODULE_NAME = "seatunnel-core";

    protected final String startModuleName;

    protected final String startModuleFullPath;

    public AbstractContainer() {
        this.startModuleName = getStartModuleName();
        this.startModuleFullPath = PROJECT_ROOT_PATH + File.separator + START_ROOT_MODULE_NAME + File.separator + this.startModuleName;
        ContainerUtil.checkPathExist(startModuleFullPath);
    }

    protected abstract String getDockerImage();

    protected abstract String getStartModuleName();

    protected abstract String getStartShellName();

    protected abstract String getConnectorModulePath();

    protected abstract String getConnectorType();

    protected abstract String getConnectorNamePrefix();

    protected abstract String getSeaTunnelHomeInContainer();

    protected abstract List<String> getExtraStartShellCommands();

    protected void copySeaTunnelStarter(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarter(container,
            this.startModuleName,
            this.startModuleFullPath,
            getSeaTunnelHomeInContainer(),
            getStartShellName());
    }

    protected Container.ExecResult executeJob(GenericContainer<?> container, String confFile) throws IOException, InterruptedException {
        final String confInContainerPath = copyConfigFileToContainer(container, confFile);
        // copy connectors
        copyConnectorJarToContainer(container,
            confFile,
            getConnectorModulePath(),
            getConnectorNamePrefix(),
            getConnectorType(),
            getSeaTunnelHomeInContainer());
        return executeCommand(container, confInContainerPath);
    }

    protected Container.ExecResult executeCommand(GenericContainer<?> container, String configPath) throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(getSeaTunnelHomeInContainer(), "bin", getStartShellName()).toString();
        // base command
        command.add(adaptPathForWin(binPath));
        command.add("--config");
        command.add(adaptPathForWin(configPath));
        command.addAll(getExtraStartShellCommands());

        Container.ExecResult execResult = container.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        return execResult;
    }
}
