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

import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.core.job.CommonPluginJar;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;

import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class IsolatedConnectorJarStorageStrategy extends AbstractConnectorJarStorageStrategy {

    public IsolatedConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig) {
        super(connectorJarStorageConfig);
    }

    @Override
    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        File storageFile = getStorageLocation(jobId, connectorJar);
        if (storageFile.exists()) {
            return ConnectorJarIdentifier.of(connectorJar, storageFile.toString());
        }
        String storagePath = storageConnectorJarFileInternal(connectorJar, storageFile).toString();
        return ConnectorJarIdentifier.of(connectorJar, storagePath);
    }

    @Override
    public void cleanUpWhenJobFinished(List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarIdentifierList.forEach(
                connectorJarIdentifier -> {
                    deleteConnectorJar(connectorJarIdentifier);
                });
    }

    @Override
    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        deleteConnectorJarInternal(new File(connectorJarIdentifier.getStoragePath()));
    }

    @Override
    public String getStorageLocationPath(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        CommonPluginJar commonPluginJar = (CommonPluginJar) connectorJar;
        if (connectorJar.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return String.format(
                    "%s/%s/%s/%s/%s/%s",
                    storageDir,
                    jobId,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    commonPluginJar.getPluginName(),
                    "lib",
                    connectorJar.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s/%s",
                    storageDir,
                    jobId,
                    CONNECTOR__PLUGIN_JAR_STORAGE_PATH,
                    connectorJar.getFileName());
        }
    }

    @Override
    public byte[] readConnectorJarByteData(File connectorJarFile) {
        return readConnectorJarByteDataInternal(connectorJarFile);
    }
}
