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

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Common {

    private static final List<String> ALLOWED_MODES = Arrays.stream(DeployMode.values())
        .map(DeployMode::getName).collect(Collectors.toList());

    private static Optional<String> MODE = Optional.empty();

    public static boolean isModeAllowed(String mode) {
        return ALLOWED_MODES.contains(mode.toLowerCase());
    }

    /**
     * Set mode. return false in case of failure
     */
    public static Boolean setDeployMode(String m) {
        if (isModeAllowed(m)) {
            MODE = Optional.of(m);
            return true;
        } else {
            return false;
        }
    }

    public static Optional<String> getDeployMode() {
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
        if (MODE.equals(Optional.of(DeployMode.CLIENT.getName()))) {
            try {
                String path = Common.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
                return Paths.get(path).getParent().getParent().getParent();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else if (MODE.equals(Optional.of(DeployMode.CLUSTER.getName()))) {
            return Paths.get("");
        } else {
            throw new IllegalStateException("MODE not support : " + MODE.orElse("null"));
        }
    }

    /**
     * Plugin Root Dir
     */
    public static Path pluginRootDir() {
        return Paths.get(appRootDir().toString(), "plugins");
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

}
