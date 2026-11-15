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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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

    protected static final boolean isWindows =
            System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS");

    protected static String hostName = System.getProperty("user.name");
    protected Integer hostUid = Integer.parseInt(System.getProperty("user.id", "1000"));
    protected Integer hostGid = Integer.parseInt(System.getProperty("user.gid", "1000"));

    protected static final String CONTAINER_VOLUME_MOUNT_PATH = "/tmp/seatunnel_mnt";
    protected static final Path CONTAINER_HADOOP_JAR_PATH =
            Paths.get(
                    SEATUNNEL_HOME, String.format("lib/%s", MavenJarUtil.getHadoop3UberJarName()));

    public static final String HOST_VOLUME_MOUNT_PATH =
            isWindows
                    ? String.format("C:/Users/%s/tmp/seatunnel_mnt", hostName)
                    : CONTAINER_VOLUME_MOUNT_PATH;

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
        // Set execute permissions for scripts to prevent "Permission denied" errors
        setScriptExecutePermissions(container);
    }

    /** Set execute permissions for SeaTunnel scripts in the container. */
    protected void setScriptExecutePermissions(GenericContainer<?> container) {
        try {
            LOG.info("Setting execute permissions for SeaTunnel scripts...");

            // Set execute permissions for all shell scripts in the bin directory
            container.execInContainer("sh", "-c", "chmod +x /tmp/seatunnel/bin/*.sh || true");

            // Specifically ensure the starter script has execute permissions
            String startShellName = getStartShellName();
            if (startShellName != null && !startShellName.isEmpty()) {
                container.execInContainer(
                        "sh", "-c", "chmod +x /tmp/seatunnel/bin/" + startShellName + " || true");
            }

            LOG.info("Script execute permissions set successfully");

        } catch (Exception e) {
            LOG.warn("Warning: Failed to set script execute permissions: " + e.getMessage());
            // Don't fail the test for permission issues, just log the warning
        }
    }

    protected void copySeaTunnelStarterToContainer(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarterToContainer(
                container, this.startModuleName, this.startModuleFullPath, SEATUNNEL_HOME);
    }

    protected void copySeaTunnelStarterLoggingToContainer(GenericContainer<?> container) {
        ContainerUtil.copySeaTunnelStarterLoggingToContainer(
                container, this.startModuleFullPath, SEATUNNEL_HOME);
    }

    /**
     * Stops the given engine containers and then deletes {@link #HOST_VOLUME_MOUNT_PATH}. Every
     * step runs even if an earlier one fails: a container left running keeps its network alias on
     * the shared network, and the next test case can then talk to it instead of its own container.
     * The first failure is rethrown after all steps have run; later ones are added to it as
     * suppressed. If an interrupt is only recorded as suppressed, the thread's interrupt flag is
     * set again before rethrowing.
     *
     * @param containers containers to stop in order; {@code null} entries are skipped
     * @throws Exception the first failure to stop a container or to delete the host path
     */
    protected void stopContainersAndDeleteVolume(GenericContainer<?>... containers)
            throws Exception {
        Exception failure = null;
        for (GenericContainer<?> container : containers) {
            try {
                stopContainer(container);
            } catch (Exception e) {
                failure = addFailure(failure, e);
            }
        }
        try {
            deleteHostVolumeMountPath();
        } catch (Exception e) {
            failure = addFailure(failure, e);
        }
        if (failure != null) {
            restoreInterruptIfSuppressed(failure);
            throw failure;
        }
    }

    /**
     * Removes {@link #CONTAINER_VOLUME_MOUNT_PATH} inside the container, then stops it. The removal
     * is best effort: if it fails or is skipped, a warning is logged and the container is stopped
     * anyway.
     */
    static void stopContainer(GenericContainer<?> container) throws Exception {
        if (container == null) {
            return;
        }
        Exception failure = null;
        try {
            removeContainerVolumeMountPath(container);
        } catch (InterruptedException e) {
            failure = e;
        }
        try {
            container.stop();
        } catch (Exception e) {
            failure = addFailure(failure, e);
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Files the engine writes to the bind-mounted volume can be owned by root, so they are removed
     * from inside the container before it stops. The exit code is not checked: {@code rm} always
     * fails on the mount point itself ("Device or resource busy") after removing its contents.
     * Anything left behind is reported by {@link #deleteHostPath(String)}.
     *
     * @return {@code true} if the removal ran, {@code false} if it was skipped or failed; both
     *     cases are logged
     */
    static boolean removeContainerVolumeMountPath(GenericContainer<?> container)
            throws InterruptedException {
        try {
            if (!container.isRunning()) {
                LOG.warn(
                        "Container{} {} is not running, skipping the removal of {} inside it",
                        container.getNetworkAliases(),
                        container.getContainerId(),
                        CONTAINER_VOLUME_MOUNT_PATH);
                return false;
            }
            container.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            return true;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            // The container can stop between isRunning() and the exec.
            LOG.warn(
                    "Failed to remove {} inside container{} {}",
                    CONTAINER_VOLUME_MOUNT_PATH,
                    container.getNetworkAliases(),
                    container.getContainerId(),
                    e);
        }
        return false;
    }

    /** Deletes {@link #HOST_VOLUME_MOUNT_PATH} and logs a warning if it could not be deleted. */
    protected void deleteHostVolumeMountPath() {
        deleteHostPath(HOST_VOLUME_MOUNT_PATH);
    }

    /** @return {@code true} if the path no longer exists */
    static boolean deleteHostPath(String path) {
        FileUtils.deleteFile(path);
        if (!new File(path).exists()) {
            return true;
        }
        LOG.warn(
                "Could not delete {} on the host, the next test case that mounts it will see the"
                        + " files left there",
                path);
        return false;
    }

    /**
     * Sets the interrupt flag again if an {@link InterruptedException} was only recorded as
     * suppressed. Called after every container was stopped, because a set flag can make the
     * remaining {@code stop()} calls fail.
     */
    private static void restoreInterruptIfSuppressed(Exception failure) {
        if (failure instanceof InterruptedException) {
            return;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            if (suppressed instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static Exception addFailure(Exception first, Exception next) {
        if (first == null) {
            return next;
        }
        first.addSuppressed(next);
        return first;
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
        return restoreJob(container, confFile, jobId, variables, getRestoreCommand());
    }

    protected Container.ExecResult restoreJob(
            GenericContainer<?> container,
            String confFile,
            String sourceJobId,
            String restoreJobId,
            List<String> variables,
            String restoreCommand)
            throws IOException, InterruptedException {
        final String confInContainerPath = copyConfigFileToContainer(container, confFile);
        copyConnectorJarToContainer(
                container,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        command.add(adaptPathForWin(binPath));
        command.add("--config");
        command.add(adaptPathForWin(confInContainerPath));
        command.add(restoreCommand);
        command.add(sourceJobId);
        if (StringUtils.isNoneEmpty(restoreJobId)) {
            command.add("--set-job-id");
            command.add(restoreJobId);
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

    protected Container.ExecResult restoreJob(
            GenericContainer<?> container,
            String confFile,
            String jobId,
            List<String> variables,
            String restoreCommand)
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
        command.add(restoreCommand);
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
