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
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class IsolatedConnectorJarStorageStrategy extends AbstractConnectorJarStorageStrategy {

    public IsolatedConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig, SeaTunnelServer seaTunnelServer) {
        super(connectorJarStorageConfig, seaTunnelServer);
    }

    @Override
    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        File storageFile = getStorageLocation(jobId, connectorJar);
        if (storageFile.exists()) {
            return ConnectorJarIdentifier.of(connectorJar, storageFile.toString());
        }
        Optional<Path> optional = storageConnectorJarFileInternal(connectorJar, storageFile);
        ConnectorJarIdentifier connectorJarIdentifier =
                optional.isPresent()
                        ? ConnectorJarIdentifier.of(connectorJar, optional.get().toString())
                        : ConnectorJarIdentifier.of(connectorJar, "");
        return connectorJarIdentifier;
    }

    @Override
    public boolean checkConnectorJarExisted(long jobId, ConnectorJar connectorJar) {
        File storageFile = getStorageLocation(jobId, connectorJar);
        if (storageFile.exists()) {
            return true;
        }
        return false;
    }

    @Override
    public void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarIdentifierList.forEach(
                connectorJarIdentifier -> {
                    deleteConnectorJar(connectorJarIdentifier);
                });
    }

    @Override
    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        deleteConnectorJarInternal(new File(connectorJarIdentifier.getStoragePath()));
        deleteConnectorJarInExecutionNode(connectorJarIdentifier);
    }

    @Override
    public String getStorageLocationPath(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        if (connectorJar.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            CommonPluginJar commonPluginJar = (CommonPluginJar) connectorJar;
            return String.format(
                    "%s/%s/%s/%s",
                    storageDir,
                    jobId,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    commonPluginJar.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s/%s",
                    storageDir,
                    CONNECTOR_PLUGIN_JAR_STORAGE_PATH,
                    jobId,
                    connectorJar.getFileName());
        }
    }

    @Override
    public byte[] readConnectorJarByteData(File connectorJarFile) {
        return readConnectorJarByteDataInternal(connectorJarFile);
    }
}
