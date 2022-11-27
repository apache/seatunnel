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

package org.apache.seatunnel.common.config;

import static java.nio.file.FileVisitOption.FOLLOW_LINKS;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Common {

    private Common() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * Used to set the size when create a new collection(just to pass the checkstyle).
     */
    public static final int COLLECTION_SIZE = 16;

    private static final int APP_LIB_DIR_DEPTH = 2;

    private static final int PLUGIN_LIB_DIR_DEPTH = 3;

    private static DeployMode MODE;

    private static boolean STARTER = false;

    /**
     * Set mode. return false in case of failure
     */
    public static void setDeployMode(DeployMode mode) {
        MODE = mode;
    }

    public static void setStarter(boolean inStarter) {
        STARTER = inStarter;
    }

    public static DeployMode getDeployMode() {
        return MODE;
    }

    /**
     * Root dir varies between different spark master and deploy mode,
     * it also varies between relative and absolute path.
     * When running seatunnel in --master local, you can put plugins related files in $project_dir/plugins,
     * then these files will be automatically copied to $project_dir/seatunnel-core/target and token in effect if you start seatunnel in IDE tools such as IDEA.
     * When running seatunnel in --master yarn or --master mesos, you can put plugins related files in plugins dir.
     */
    public static Path appRootDir() {
        if (DeployMode.CLIENT == MODE || STARTER) {
            try {
                String path = Common.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                path = new File(path).getPath();
                return Paths.get(path).getParent().getParent();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else if (DeployMode.CLUSTER == MODE) {
            return Paths.get("");
        } else {
            throw new IllegalStateException("deploy mode not support : " + MODE);
        }
    }

    public static Path appStarterDir() {
        return appRootDir().resolve("starter");
    }

    /**
     * Plugin Root Dir
     */
    public static Path pluginRootDir() {
        return Paths.get(appRootDir().toString(), "plugins");
    }

    /**
     * Plugin Root Dir
     */
    public static Path connectorRootDir(String engine) {
        return Paths.get(appRootDir().toString(), "connectors", engine.toLowerCase());
    }

    /**
     * Plugin Connector Jar Dir
     */
    public static Path connectorJarDir(String engine) {
        String seatunnelHome = System.getProperty("SEATUNNEL_HOME");
        if (StringUtils.isBlank(seatunnelHome)) {
            return Paths.get(appRootDir().toString(), "connectors", engine.toLowerCase());
        } else {
            return Paths.get(seatunnelHome, "connectors", engine.toLowerCase());
        }
    }

    /**
     * Plugin Connector Dir
     */
    public static Path connectorDir() {
        String seatunnelHome = System.getProperty("SEATUNNEL_HOME");
        if (StringUtils.isBlank(seatunnelHome)) {
            return Paths.get(appRootDir().toString(), "connectors");
        } else {
            return Paths.get(seatunnelHome, "connectors");
        }
    }

    /**
     * lib Dir
     */
    public static Path libDir() {
        String seatunnelHome = System.getProperty("SEATUNNEL_HOME");
        if (StringUtils.isBlank(seatunnelHome)) {
            seatunnelHome = appRootDir().toString();
        }
        return Paths.get(seatunnelHome, "lib");
    }

    /**
     * return lib jars, which located in 'lib/*' or 'lib/{dir}/*'.
     */
    public static List<Path> getLibJars() {
        Path libRootDir = Common.libDir();
        if (!Files.exists(libRootDir) || !Files.isDirectory(libRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(libRootDir, APP_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream
                .filter(it -> !it.toFile().isDirectory())
                .filter(it -> it.getFileName().toString().endsWith(".jar"))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * return the jar package configured in env jars
     */
    public static Set<Path> getThirdPartyJars(String paths) {

        return Arrays.stream(paths.split(";"))
            .filter(s -> !"".equals(s))
            .filter(it -> it.endsWith(".jar"))
            .map(path -> Paths.get(URI.create(path))).collect(Collectors.toSet());
    }

    public static Path pluginTarball() {
        return appRootDir().resolve("plugins.tar.gz");
    }

    /**
     * Get specific plugin dir
     */
    public static Path pluginDir(String pluginName) {
        return Paths.get(pluginRootDir().toString(), pluginName);
    }

    /**
     * Get files dir of specific plugin
     */
    public static Path pluginFilesDir(String pluginName) {
        return Paths.get(pluginDir(pluginName).toString(), "files");
    }

    /**
     * Get lib dir of specific plugin
     */
    public static Path pluginLibDir(String pluginName) {
        return Paths.get(pluginDir(pluginName).toString(), "lib");
    }

    /**
     * return plugin's dependent jars, which located in 'plugins/${pluginName}/lib/*'.
     */
    public static List<Path> getPluginsJarDependencies() {
        Path pluginRootDir = Common.pluginRootDir();
        if (!Files.exists(pluginRootDir) || !Files.isDirectory(pluginRootDir)) {
            return Collections.emptyList();
        }
        try (Stream<Path> stream = Files.walk(pluginRootDir, PLUGIN_LIB_DIR_DEPTH, FOLLOW_LINKS)) {
            return stream
                .filter(it -> pluginRootDir.relativize(it).getNameCount() == PLUGIN_LIB_DIR_DEPTH)
                .filter(it -> it.getParent().endsWith("lib"))
                .filter(it -> it.getFileName().toString().endsWith(".jar"))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
