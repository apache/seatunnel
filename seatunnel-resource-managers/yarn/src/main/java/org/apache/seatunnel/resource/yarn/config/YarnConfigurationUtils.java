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

package org.apache.seatunnel.resource.yarn.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/** Loads platform configuration and bounds Hadoop RPC retries for lifecycle cleanup. */
public final class YarnConfigurationUtils {
    /** Standard environment variable used to discover client-side Hadoop XML files. */
    private static final String HADOOP_CONF_DIR_ENV = "HADOOP_CONF_DIR";

    /** Hadoop configuration files copied into the merged application configuration. */
    private static final String[] HADOOP_CONFIGURATION_FILES =
            new String[] {"core-site.xml", "hdfs-site.xml", "yarn-site.xml"};

    /** Authentication mode supported by the MVP container localization flow. */
    private static final String SIMPLE_AUTHENTICATION = "simple";

    /** Hadoop settings without public constants that bound application lifecycle RPCs. */
    private static final String IPC_CONNECT_TIMEOUT = "ipc.client.connect.timeout";

    private static final String IPC_CONNECT_MAX_RETRIES = "ipc.client.connect.max.retries";
    private static final String IPC_CONNECT_TIMEOUT_RETRIES =
            "ipc.client.connect.max.retries.on.timeouts";
    private static final String IPC_RPC_TIMEOUT = "ipc.client.rpc-timeout.ms";
    private static final String DFS_SOCKET_TIMEOUT = "dfs.client.socket-timeout";
    private static final String HADOOP_AUTHENTICATION = "hadoop.security.authentication";

    /** Bounds one Hadoop or YARN RPC attempt so cleanup fits the runtime shutdown deadline. */
    private static final int RPC_TIMEOUT_MILLIS = 10000;

    private YarnConfigurationUtils() {}

    /** Loads the submitting user's Hadoop XML files and validates supported authentication. */
    public static YarnConfiguration load(Map<String, String> options) {
        YarnConfiguration configuration = new YarnConfiguration();
        String directory =
                ReadonlyConfig.fromMap(new HashMap<String, Object>(options))
                        .get(YarnOptions.CONFIG_DIRECTORY);
        if (directory.isEmpty()) {
            directory = System.getenv(HADOOP_CONF_DIR_ENV);
        }
        if (directory != null && !directory.isEmpty()) {
            if (!Files.isDirectory(Paths.get(directory))) {
                throw new IllegalArgumentException(
                        "Hadoop configuration directory does not exist: " + directory);
            }
            for (String name : HADOOP_CONFIGURATION_FILES) {
                File file = new File(directory, name);
                if (Files.isRegularFile(file.toPath())) {
                    configuration.addResource(new Path(file.toURI()));
                }
            }
        }
        requireSimpleAuthentication(configuration);
        return configuration;
    }

    /** Loads the merged Hadoop configuration localized into the container working directory. */
    public static Configuration loadLocalized(String fileName) {
        YarnConfiguration configuration = new YarnConfiguration();
        configuration.addResource(new Path(Paths.get(fileName).toAbsolutePath().toUri()));
        requireSimpleAuthentication(configuration);
        return configuration;
    }

    /**
     * Keep Hadoop's multi-minute default retries inside the application's bounded cleanup window.
     */
    public static Configuration withBoundedRpc(Configuration original) {
        Configuration configuration = new Configuration(original);
        configuration.setLong(
                YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RPC_TIMEOUT_MILLIS);
        configuration.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1000);
        configuration.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, RPC_TIMEOUT_MILLIS);
        configuration.setLong(YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, 1000);
        configuration.setLong(
                YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS, 20000);
        configuration.setInt(IPC_CONNECT_TIMEOUT, RPC_TIMEOUT_MILLIS);
        configuration.setInt(IPC_CONNECT_MAX_RETRIES, 1);
        configuration.setInt(IPC_CONNECT_TIMEOUT_RETRIES, 1);
        configuration.setInt(IPC_RPC_TIMEOUT, RPC_TIMEOUT_MILLIS);
        configuration.setInt(DFS_SOCKET_TIMEOUT, RPC_TIMEOUT_MILLIS);
        return configuration;
    }

    /** Rejects credentials that the application launcher cannot propagate to containers. */
    public static void requireSimpleAuthentication(Configuration configuration) {
        if (!SIMPLE_AUTHENTICATION.equalsIgnoreCase(
                configuration.get(HADOOP_AUTHENTICATION, SIMPLE_AUTHENTICATION))) {
            throw new IllegalArgumentException(
                    "YARN application mode supports simple authentication only; Kerberos is not supported.");
        }
    }
}
