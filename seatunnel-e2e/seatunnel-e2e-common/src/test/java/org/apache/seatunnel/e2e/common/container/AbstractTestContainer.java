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

package org.apache.seatunnel.e2e.common.container;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.adaptPathForWin;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.copyConfigFileToContainer;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.copyConnectorJarToContainer;

public abstract class AbstractTestContainer implements TestContainer {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractTestContainer.class);
    protected static final String START_ROOT_MODULE_NAME = "seatunnel-core";

    public static final String SEATUNNEL_HOME = "/tmp/seatunnel/";
    protected final String startModuleName;

    protected final String startModuleFullPath;

    public AbstractTestContainer() {
        this.startModuleName = getStartModuleName();
        this.startModuleFullPath =
                PROJECT_ROOT_PATH
                        + File.separator
                        + START_ROOT_MODULE_NAME
                        + File.separator
                        + this.startModuleName;
        ContainerUtil.checkPathExist(startModuleFullPath);
    }

    protected abstract String getDockerImage();

    protected abstract String getStartModuleName();

    protected abstract String getStartShellName();

    protected abstract String getConnectorModulePath();

    protected abstract String getConnectorType();

    protected abstract String getSavePointCommand();

    protected abstract String getCancelJobCommand();

    protected abstract String getRestoreCommand();

    protected abstract String getConnectorNamePrefix();

    protected abstract List<String> getExtraStartShellCommands();

    /**
     * TODO: issue #2733, Reimplement all modules that override the method, remove this method & use
     * {@link ContainerExtendedFactory}.
     */
    protected void executeExtraCommands(GenericContainer<?> container)
            throws IOException, InterruptedException {
        // do nothing
    }

    protected void copySeaTunnelStarterToContainer(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarterToContainer(
                container, this.startModuleName, this.startModuleFullPath, SEATUNNEL_HOME);
    }

    protected void copySeaTunnelStarterLoggingToContainer(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarterLoggingToContainer(
                container, this.startModuleFullPath, SEATUNNEL_HOME);
    }

    protected Container.ExecResult executeJob(GenericContainer<?> container, String confFile)
            throws IOException, InterruptedException {
        return executeJob(container, confFile, null, null);
    }

    protected Container.ExecResult executeJob(
            GenericContainer<?> container, String confFile, String jobId, List<String> variables)
            throws IOException, InterruptedException {
        final String confInContainerPath = copyConfigFileToContainer(container, confFile);
        // copy connectors
        copyConnectorJarToContainer(
                container,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(adaptPathForWin(binPath));
        command.add("--config");
        command.add(adaptPathForWin(confInContainerPath));
        command.add("--name");
        command.add(new File(confInContainerPath).getName());
        if (StringUtils.isNoneEmpty(jobId)) {
            command.add("--set-job-id");
            command.add(jobId);
        }
        List<String> extraStartShellCommands = new ArrayList<>(getExtraStartShellCommands());
        if (variables != null && !variables.isEmpty()) {
            variables.forEach(
                    v -> {
                        extraStartShellCommands.add("-i");
                        extraStartShellCommands.add(v);
                    });
        }
        command.addAll(extraStartShellCommands);
        return executeCommand(container, command);
    }

    protected Container.ExecResult savepointJob(GenericContainer<?> container, String jobId)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(adaptPathForWin(binPath));
        command.add(getSavePointCommand());
        command.add(jobId);
        command.addAll(getExtraStartShellCommands());
        return executeCommand(container, command);
    }

    protected Container.ExecResult cancelJob(GenericContainer<?> container, String jobId)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(adaptPathForWin(binPath));
        command.add(getCancelJobCommand());
        command.add(jobId);
        command.addAll(getExtraStartShellCommands());
        return executeCommand(container, command);
    }

    protected Container.ExecResult restoreJob(
            GenericContainer<?> container, String confFile, String jobId, List<String> variables)
            throws IOException, InterruptedException {
        final String confInContainerPath = copyConfigFileToContainer(container, confFile);
        // copy connectors
        copyConnectorJarToContainer(
                container,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(adaptPathForWin(binPath));
        command.add("--config");
        command.add(adaptPathForWin(confInContainerPath));
        command.add(getRestoreCommand());
        command.add(jobId);
        List<String> extraStartShellCommands = new ArrayList<>(getExtraStartShellCommands());
        if (variables != null && !variables.isEmpty()) {
            variables.forEach(
                    v -> {
                        extraStartShellCommands.add("-i");
                        extraStartShellCommands.add(v);
                    });
        }
        command.addAll(extraStartShellCommands);
        return executeCommand(container, command);
    }

    protected Container.ExecResult executeCommand(
            GenericContainer<?> container, List<String> command)
            throws IOException, InterruptedException {
        String commandStr = String.join(" ", command);
        LOG.info(
                "Execute command in container[{}] "
                        + "\n==================== Shell Command start ====================\n"
                        + "{}"
                        + "\n==================== Shell Command end   ====================",
                container.getDockerImageName(),
                commandStr);
        Container.ExecResult execResult = container.execInContainer("bash", "-c", commandStr);

        if (execResult.getStdout() != null && !execResult.getStdout().isEmpty()) {
            LOG.info(
                    "Container[{}] command {} STDOUT:"
                            + "\n==================== STDOUT start ====================\n"
                            + "{}"
                            + "\n==================== STDOUT end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    execResult.getStdout());
        }
        if (execResult.getStderr() != null && !execResult.getStderr().isEmpty()) {
            LOG.error(
                    "Container[{}] command {} STDERR:"
                            + "\n==================== STDERR start ====================\n"
                            + "{}"
                            + "\n==================== STDERR end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    execResult.getStderr());
        }

        if (execResult.getExitCode() != 0) {
            LOG.info(
                    "Container[{}] command {} Server Log:"
                            + "\n==================== Server Log start ====================\n"
                            + "{}"
                            + "\n==================== Server Log end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    container.getLogs());
        }

        return execResult;
    }
}
