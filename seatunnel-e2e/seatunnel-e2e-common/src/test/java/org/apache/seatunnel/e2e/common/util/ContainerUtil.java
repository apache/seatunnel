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

package org.apache.seatunnel.e2e.common.util;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.factory.FactoryException;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public final class ContainerUtil {

    public static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";

    /** An error occurs when the user is not a submodule of seatunnel-e2e. */
    public static final String PROJECT_ROOT_PATH = getProjectRootPath();

    private static String getProjectRootPath() {
        String e2eRootModuleDir = "seatunnel-e2e";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(e2eRootModuleDir))) {
            path = path.getParent();
        }
        return path.getParent().toString();
    }

    public static void copyConnectorJarToContainer(
            GenericContainer<?> container,
            String confFile,
            String connectorsRootPath,
            String connectorPrefix,
            String connectorType,
            String seatunnelHome) {
        Config jobConfig = getConfig(getResourcesFile(confFile));
        Config connectorsMapping =
                getConfig(new File(PROJECT_ROOT_PATH + File.separator + PLUGIN_MAPPING_FILE));
        if (!connectorsMapping.hasPath(connectorType)
                || connectorsMapping.getConfig(connectorType).isEmpty()) {
            return;
        }
        Config connectors = connectorsMapping.getConfig(connectorType);
        Set<String> connectorNames = getConnectors(jobConfig, connectors, "source");
        connectorNames.addAll(getConnectors(jobConfig, connectors, "sink"));
        File module = new File(PROJECT_ROOT_PATH + File.separator + connectorsRootPath);

        List<File> connectorFiles = getConnectorFiles(module, connectorNames, connectorPrefix);
        connectorFiles.forEach(
                jar ->
                        container.copyFileToContainer(
                                MountableFile.forHostPath(jar.getAbsolutePath()),
                                Paths.get(seatunnelHome, "connectors", connectorType, jar.getName())
                                        .toString()));
    }

    public static String copyConfigFileToContainer(GenericContainer<?> container, String confFile) {
        final String targetConfInContainer = Paths.get("/tmp", confFile).toString();
        container.copyFileToContainer(
                MountableFile.forHostPath(getResourcesFile(confFile).getAbsolutePath()),
                targetConfInContainer);
        return targetConfInContainer;
    }

    public static void copySeaTunnelStarterLoggingToContainer(
            GenericContainer<?> container,
            String startModulePath,
            String seatunnelHomeInContainer) {
        // copy logging lib
        final String loggingLibPath =
                startModulePath
                        + File.separator
                        + "target"
                        + File.separator
                        + "logging-e2e"
                        + File.separator;
        checkPathExist(loggingLibPath);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(loggingLibPath),
                Paths.get(seatunnelHomeInContainer, "starter", "logging").toString());
    }

    public static void copySeaTunnelStarterToContainer(
            GenericContainer<?> container,
            String startModuleName,
            String startModulePath,
            String seatunnelHomeInContainer) {
        // solve the problem of multi modules such as
        // seatunnel-flink-starter/seatunnel-flink-13-starter
        final String[] splits = StringUtils.split(startModuleName, File.separator);
        final String startJarName = splits[splits.length - 1] + ".jar";
        // copy starter
        final String startJarPath =
                startModulePath + File.separator + "target" + File.separator + startJarName;
        checkPathExist(startJarPath);
        // don't use container#withFileSystemBind, this isn't supported in Windows.
        container.withCopyFileToContainer(
                MountableFile.forHostPath(startJarPath),
                Paths.get(seatunnelHomeInContainer, "starter", startJarName).toString());

        // copy lib
        String transformJar = "seatunnel-transforms-v2.jar";
        Path transformJarPath =
                Paths.get(PROJECT_ROOT_PATH, "seatunnel-transforms-v2", "target", transformJar);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(transformJarPath),
                Paths.get(seatunnelHomeInContainer, "lib", transformJar).toString());

        // copy bin
        final String startBinPath = startModulePath + File.separator + "src/main/bin/";
        checkPathExist(startBinPath);
        container.withCopyFileToContainer(
                MountableFile.forHostPath(startBinPath),
                Paths.get(seatunnelHomeInContainer, "bin").toString());

        // copy plugin-mapping.properties
        container.withCopyFileToContainer(
                MountableFile.forHostPath(PROJECT_ROOT_PATH + "/plugin-mapping.properties"),
                Paths.get(seatunnelHomeInContainer, "connectors", PLUGIN_MAPPING_FILE).toString());
    }

    public static String adaptPathForWin(String path) {
        // Running IT use cases under Windows requires replacing \ with /
        return path == null ? "" : path.replaceAll("\\\\", "/");
    }

    private static List<File> getConnectorFiles(
            File currentModule, Set<String> connectorNames, String connectorPrefix) {
        List<File> connectorFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(currentModule.listFiles())) {
            getConnectorFiles(file, connectorNames, connectorPrefix, connectorFiles);
        }
        return connectorFiles;
    }

    private static void getConnectorFiles(
            File currentModule,
            Set<String> connectorNames,
            String connectorPrefix,
            List<File> connectors) {
        if (currentModule.isFile() || connectorNames.size() == connectors.size()) {
            return;
        }
        if (connectorNames.contains(currentModule.getName())) {
            File targetPath = new File(currentModule.getAbsolutePath() + File.separator + "target");
            for (File file : Objects.requireNonNull(targetPath.listFiles())) {
                if (file.getName().startsWith(currentModule.getName())
                        && !file.getName().endsWith("javadoc.jar")
                        && !file.getName().endsWith("tests.jar")) {
                    connectors.add(file);
                    return;
                }
            }
        }

        if (currentModule.getName().startsWith(connectorPrefix)) {
            for (File file : Objects.requireNonNull(currentModule.listFiles())) {
                getConnectorFiles(file, connectorNames, connectorPrefix, connectors);
            }
        }
    }

    private static Set<String> getConnectors(
            Config jobConfig, Config connectorsMap, String pluginType) {
        List<? extends Config> connectorConfigList = jobConfig.getConfigList(pluginType);
        Map<String, String> connectors = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ReadonlyConfig.fromConfig(connectorsMap.getConfig(pluginType)).toMap(connectors);
        return connectorConfigList.stream()
                .map(config -> config.getString("plugin_name"))
                .filter(connectors::containsKey)
                .map(connectors::get)
                .collect(Collectors.toSet());
    }

    public static Path getCurrentModulePath() {
        return Paths.get(System.getProperty("user.dir"));
    }

    public static File getResourcesFile(String confFile) {
        File file = new File(getCurrentModulePath() + "/src/test/resources" + confFile);
        if (file.exists()) {
            return file;
        }
        throw new IllegalArgumentException(confFile + " doesn't exist");
    }

    private static Config getConfig(File file) {
        return ConfigFactory.parseFile(file)
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(
                        ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    public static void checkPathExist(String path) {
        Assertions.assertTrue(new File(path).exists(), path + " must exist");
    }

    public static List<TestContainer> discoverTestContainers() {
        try {
            final List<TestContainer> result = new LinkedList<>();
            ServiceLoader.load(TestContainer.class, Thread.currentThread().getContextClassLoader())
                    .iterator()
                    .forEachRemaining(result::add);
            return result;
        } catch (ServiceConfigurationError e) {
            log.error("Could not load service provider for containers.", e);
            throw new FactoryException("Could not load service provider for containers.", e);
        }
    }

    public static void copyFileIntoContainers(
            String fileName, String targetPath, GenericContainer<?> container) {
        Path path = getResourcesFile(fileName).toPath();
        container.copyFileToContainer(MountableFile.forHostPath(path), targetPath);
    }
}
