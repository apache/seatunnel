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
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;

import java.io.File;
import java.io.IOException;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class IsolatedConnectorJarStorageStrategy extends AbstractConnectorJarStorageStrategy {

    public IsolatedConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig) {
        super(connectorJarStorageConfig);
    }

    @Override
    public String storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        File storageFile = getStorageLocation(jobId, connectorJar);
        if (storageFile.exists()) {
            return storageFile.toString();
        }
        return storageConnectorJarFileInternal(connectorJar, storageFile).toString();
    }

    @Override
    public void deleteConnectorJar(long jobId, String connectorJarFileName) throws IOException {
        File storageLocationByMetaInfo = new File("");
        deleteConnectorJarInternal(storageLocationByMetaInfo);
    }

    @Override
    public String getStorageLocationPath(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        if (connectorJar.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return String.format(
                    "%s/%s/%s/%s/%s/%s",
                    storageDir,
                    jobId,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    connectorJar.getPluginName(),
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
    public String getStoragePathFromJarName(String connectorJarName) {
        return null;
    }

    @Override
    public byte[] readConnectorJarByteData(File connectorJarFile) {
        return new byte[0];
    }
}
