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

package org.apache.seatunnel.core.starter.flink.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.enums.MasterType;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ResourceUtils {

    private static final String PLUGIN_MAPPING_FILE_NAME = "plugin-mapping.properties";

    private static final String YARN_JAVA_OPTS =
            "env.java.opts=-Djava.protocol.handler.pkgs=org.apache.seatunnel.core.starter.flink.protocol";

    public static void attachLocalResource(
            MasterType type, List<String> resources, String connectors, List<String> command) {
        switch (type) {
            case YARN_APPLICATION:
                command.add("-D");
                command.add("\"yarn.ship-files=" + StringUtils.join(resources, ';') + "\"");
                if (StringUtils.isNoneEmpty(connectors)) {
                    command.add("-D");
                    command.add(YARN_JAVA_OPTS);
                }
                break;
            case KUBERNETES_SESSION:
            default:
                break;
        }
    }

    public static String getPluginMappingConfigPath() {
        return Common.connectorDir().resolve(PLUGIN_MAPPING_FILE_NAME).toFile().getAbsolutePath();
    }

    public static Config getPluginMappingConfigFromClasspath() {
        try {
            URL configUrl = ResourceUtils.class.getResource("/" + PLUGIN_MAPPING_FILE_NAME);
            return ConfigFactory.parseFile(Paths.get(configUrl.toURI()).toFile())
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                    .resolveWith(
                            ConfigFactory.systemProperties(),
                            ConfigResolveOptions.defaults().setAllowUnresolved(true));
        } catch (URISyntaxException e) {
            throw CommonError.fileNotExistFailed("SeaTunnel", "read", PLUGIN_MAPPING_FILE_NAME);
        }
    }

    public static Path getConfigFile(MasterType type, String configFile) {
        switch (type) {
            case REMOTE:
            case YARN_PER_JOB:
            case YARN_SESSION:
            case KUBERNETES_SESSION:
                return Paths.get(configFile);
            case YARN_APPLICATION:
                return getConfigFileFromClasspath(configFile);
            case KUBERNETES_APPLICATION:
                return Paths.get(getFileName(configFile));
            default:
                throw new IllegalArgumentException("Unsupported deploy mode: " + type);
        }
    }

    public static URL of(String spec) {
        try {
            return new URL(spec);
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("Illegal provide lib address: " + spec, e);
        }
    }

    private static Path getConfigFileFromClasspath(String configFile) {
        try {
            String filename = getFileName(configFile);
            return Paths.get(ResourceUtils.class.getResource("/" + filename).toURI());
        } catch (URISyntaxException e) {
            throw CommonError.fileNotExistFailed("SeaTunnel", "read", configFile);
        }
    }

    private static String getFileName(String configFile) {
        return configFile.substring(configFile.lastIndexOf(File.separatorChar) + 1);
    }
}
