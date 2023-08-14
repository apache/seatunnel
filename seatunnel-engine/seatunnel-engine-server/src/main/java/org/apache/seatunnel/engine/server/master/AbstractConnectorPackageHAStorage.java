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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.AbstractConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.FileConfiguration;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarHAStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.util.Map;

public abstract class AbstractConnectorPackageHAStorage {

    protected static final ILogger LOGGER =
            Logger.getLogger(AbstractConnectorPackageHAStorage.class);

    protected FileSystem fileSystem;

    /** The name of the configuration property that specifies the name of the file system. */
    public static final String USER_DEFINED_BASE_PATH = "basePath";

    /** The base path of the connector jar store. */
    protected String basePath;

    protected static final String COMMON_PLUGIN_JAR_STORAGE_PATH = "/plugins";

    protected static final String CONNECTOR__PLUGIN_JAR_STORAGE_PATH = "/connectors/seatunnel";

    private static final String STORAGE_TYPE_KEY = "storage.type";

    public static final String DEFAULT_CONNECTOR_JAR_FILE_PATH_SPLIT = "/";

    private ConnectorJarStorageMode connectorJarStorageMode;

    private final ConnectorJarHAStorageConfig connectorJarHAStorageConfig;

    private static final String DEFAULT_WINDOWS_OS_NAME_SPACE =
            "C:\\ProgramData\\seatunnel\\connectorJars\\";

    private static final String DEFAULT_LINUX_OS_NAME_SPACE = "/seatunnel/connectorJars/";

    public AbstractConnectorPackageHAStorage(ConnectorJarStorageConfig connectorJarStorageConfig) {
        this.connectorJarStorageMode = connectorJarStorageConfig.getStorageMode();
        this.connectorJarHAStorageConfig =
                connectorJarStorageConfig.getConnectorJarHAStorageConfig();
        Map<String, String> configuration = connectorJarHAStorageConfig.getStoragePluginConfig();
        try {
            Configuration hadoopConf = getConfiguration(configuration);
            fileSystem = FileSystem.get(hadoopConf);
        } catch (Exception e) {
            LOGGER.warning("Failed to get file system");
        }
        setDefaultStorageSpaceByOSName();
        if (StringUtils.isNotBlank(configuration.get(USER_DEFINED_BASE_PATH))) {
            setBaseStoragePath(configuration.get(USER_DEFINED_BASE_PATH));
        }
    }

    /** set default storage root directory */
    private void setDefaultStorageSpaceByOSName() {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            setBaseStoragePath(DEFAULT_WINDOWS_OS_NAME_SPACE);
        } else {
            setBaseStoragePath(DEFAULT_LINUX_OS_NAME_SPACE);
        }
    }

    private void setBaseStoragePath(String baseStoragePath) {
        if (baseStoragePath != null) {
            if (!baseStoragePath.endsWith(DEFAULT_CONNECTOR_JAR_FILE_PATH_SPLIT)) {
                baseStoragePath = baseStoragePath + DEFAULT_CONNECTOR_JAR_FILE_PATH_SPLIT;
            }
            this.basePath = baseStoragePath;
        }
    }

    private Configuration getConfiguration(Map<String, String> config)
            throws CheckpointStorageException {
        String storageType =
                config.getOrDefault(STORAGE_TYPE_KEY, FileConfiguration.LOCAL.toString());
        config.remove(STORAGE_TYPE_KEY);
        AbstractConfiguration configuration =
                FileConfiguration.valueOf(storageType.toUpperCase()).getConfiguration(storageType);
        return configuration.buildConfiguration(config);
    }

    public abstract void uploadConnectorJar(
            long jobId, File localFile, ConnectorJarIdentifier connectorJarIdentifier);

    public abstract void downloadConnectorJar(
            long jobId, ConnectorJarIdentifier connectorJarIdentifier);

    public abstract void deleteConnectorJar(
            long jobId, ConnectorJarIdentifier connectorJarIdentifier);

    protected String getStorageLocationPath(
            long jobId, ConnectorJarIdentifier connectorJarIdentifier) {
        switch (connectorJarStorageMode) {
            case SHARED:
                return getSharedStorageLocationPath(jobId, connectorJarIdentifier);
            case ISOLATED:
                return getIsolatedStorageLocationPath(jobId, connectorJarIdentifier);
            default:
                throw new IllegalArgumentException(
                        ServerConfigOptions.CONNECTOR_JAR_STORAGE_MODE
                                + " must in [SHARED, ISOLATED]");
        }
    }

    private String getSharedStorageLocationPath(
            long jobId, ConnectorJarIdentifier connectorJarIdentifier) {
        if (connectorJarIdentifier.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return String.format(
                    "%s/%s/%s/%s/%s",
                    basePath,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    connectorJarIdentifier.getPluginName(),
                    "lib",
                    connectorJarIdentifier.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s",
                    basePath,
                    CONNECTOR__PLUGIN_JAR_STORAGE_PATH,
                    connectorJarIdentifier.getFileName());
        }
    }

    private String getIsolatedStorageLocationPath(
            long jobId, ConnectorJarIdentifier connectorJarIdentifier) {
        Preconditions.checkNotNull(jobId);
        if (connectorJarIdentifier.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return String.format(
                    "%s/%s/%s/%s/%s/%s",
                    basePath,
                    jobId,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    connectorJarIdentifier.getPluginName(),
                    "lib",
                    connectorJarIdentifier.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s/%s",
                    basePath,
                    jobId,
                    CONNECTOR__PLUGIN_JAR_STORAGE_PATH,
                    connectorJarIdentifier.getFileName());
        }
    }
}
