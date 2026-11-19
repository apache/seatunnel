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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.config.XmlConfigBuilder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Loads platform configuration and bounds Hadoop RPC retries for lifecycle cleanup. */
public final class YarnConfigurationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(YarnConfigurationUtils.class);

    /** Hadoop configuration files copied into the merged application configuration. */
    private static final String[] HADOOP_CONFIGURATION_FILES =
            new String[] {"core-site.xml", "hdfs-site.xml", "yarn-site.xml"};

    /** Hadoop settings without public constants that bound application lifecycle RPCs. */
    private static final String IPC_CONNECT_TIMEOUT = "ipc.client.connect.timeout";

    private static final String IPC_CONNECT_MAX_RETRIES = "ipc.client.connect.max.retries";
    private static final String IPC_CONNECT_TIMEOUT_RETRIES =
            "ipc.client.connect.max.retries.on.timeouts";
    private static final String IPC_RPC_TIMEOUT = "ipc.client.rpc-timeout.ms";
    private static final String DFS_SOCKET_TIMEOUT = "dfs.client.socket-timeout";

    /** Bounds one Hadoop or YARN RPC attempt so cleanup fits the runtime shutdown deadline. */
    private static final int RPC_TIMEOUT_MILLIS = 10000;

    private static final int RPC_RETRY_INTERVAL_MILLIS = 1000;
    private static final int APPLICATION_POLL_TIMEOUT_MILLIS = 20000;

    private YarnConfigurationUtils() {}

    /** Loads the engine and role-specific member configuration before applying YARN discovery. */
    public static SeaTunnelConfig loadEngineConfig() {
        SeaTunnelConfig engine = ConfigProvider.locateAndGetSeaTunnelConfig();
        String memberConfig = System.getProperty(YarnConstants.HAZELCAST_CONFIG_PROPERTY, "");
        if (memberConfig.toLowerCase(Locale.ROOT).endsWith(".xml")) {
            // The shared engine configuration loader handles YAML member files.
            engine.setHazelcastConfig(new XmlConfigBuilder().build());
        }
        return engine;
    }

    /**
     * Loads the submitter's Hadoop configuration. An explicit {@code yarn.config-dir} takes
     * precedence over {@code HADOOP_CONF_DIR}. With neither set, no site directory is loaded.
     *
     * @param options deployment options supplied to the descriptor
     * @return Hadoop configuration used by the submission clients
     * @throws IllegalArgumentException if the selected directory does not exist
     */
    public static YarnConfiguration load(Map<String, String> options) {
        YarnConfiguration configuration = new YarnConfiguration();
        String directory =
                ReadonlyConfig.fromMap(new HashMap<String, Object>(options))
                        .get(YarnOptions.CONFIG_DIRECTORY);
        if (directory.isEmpty()) {
            directory = System.getenv(YarnConstants.HADOOP_CONF_DIR_ENV);
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
        } else {
            LOG.warn(
                    "No Hadoop site directory configured. Set yarn.config-dir or HADOOP_CONF_DIR "
                            + "to load your cluster settings.");
        }
        return configuration;
    }

    /** Loads the merged Hadoop configuration localized into the container working directory. */
    public static Configuration loadLocalized(String fileName) {
        YarnConfiguration configuration = new YarnConfiguration();
        configuration.addResource(new Path(Paths.get(fileName).toAbsolutePath().toUri()));
        if (YarnSecurityContext.isKerberos(configuration)) {
            UserGroupInformation.setConfiguration(configuration);
        }
        return configuration;
    }

    /**
     * Keep Hadoop's multi-minute default retries inside the application's bounded cleanup window.
     */
    public static Configuration withBoundedRpc(Configuration original) {
        Configuration configuration = new Configuration(original);
        configuration.setLong(
                YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, RPC_TIMEOUT_MILLIS);
        configuration.setLong(
                YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS,
                RPC_RETRY_INTERVAL_MILLIS);
        configuration.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS, RPC_TIMEOUT_MILLIS);
        configuration.setLong(
                YarnConfiguration.CLIENT_NM_CONNECT_RETRY_INTERVAL_MS, RPC_RETRY_INTERVAL_MILLIS);
        configuration.setLong(
                YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS,
                APPLICATION_POLL_TIMEOUT_MILLIS);
        configuration.setInt(IPC_CONNECT_TIMEOUT, RPC_TIMEOUT_MILLIS);
        configuration.setInt(IPC_CONNECT_MAX_RETRIES, 1);
        configuration.setInt(IPC_CONNECT_TIMEOUT_RETRIES, 1);
        configuration.setInt(IPC_RPC_TIMEOUT, RPC_TIMEOUT_MILLIS);
        configuration.setInt(DFS_SOCKET_TIMEOUT, RPC_TIMEOUT_MILLIS);
        return configuration;
    }
}
