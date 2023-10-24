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

import org.apache.seatunnel.engine.common.config.SeaTunnelProperties;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.task.operation.DeleteConnectorJarInExecutionNode;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.StringUtils;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractConnectorJarStorageStrategy implements ConnectorJarStorageStrategy {

    protected static final ILogger LOGGER =
            Logger.getLogger(AbstractConnectorJarStorageStrategy.class);

    protected static final String COMMON_PLUGIN_JAR_STORAGE_PATH = "/plugins";

    protected static final String CONNECTOR_PLUGIN_JAR_STORAGE_PATH = "/connectors/seatunnel";

    protected String storageDir;

    protected final ConnectorJarStorageConfig connectorJarStorageConfig;

    protected final SeaTunnelServer seaTunnelServer;

    protected final NodeEngineImpl nodeEngine;

    public AbstractConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig, SeaTunnelServer seaTunnelServer) {
        this.seaTunnelServer = seaTunnelServer;
        this.nodeEngine = seaTunnelServer.getNodeEngine();
        checkNotNull(connectorJarStorageConfig);
        this.connectorJarStorageConfig = connectorJarStorageConfig;
        this.storageDir = getConnectorJarStorageDir();
    }

    @Override
    public File getStorageLocation(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        File file = new File(getStorageLocationPath(jobId, connectorJar));
        try {
            Files.createDirectories(file.getParentFile().toPath());
        } catch (IOException e) {
            LOGGER.warning(
                    String.format(
                            "The creation of directories : %s for the connector jar storage path has failed.",
                            file.getParentFile().toPath().toString()));
        }
        return file;
    }

    @Override
    public ConnectorJarIdentifier getConnectorJarIdentifier(long jobId, ConnectorJar connectorJar) {
        return ConnectorJarIdentifier.of(connectorJar, getStorageLocationPath(jobId, connectorJar));
    }

    @Override
    public Optional<Path> storageConnectorJarFileInternal(
            ConnectorJar connectorJar, File storageFile) {
        boolean success = false;
        try {
            if (!storageFile.exists()) {
                FileOutputStream fos = new FileOutputStream(storageFile);
                fos.write(connectorJar.getData());
            } else {
                LOGGER.warning(
                        String.format(
                                "File storage for an existing file %s. This may indicate a duplicate upload. Ignoring newest upload.",
                                storageFile));
            }
            success = true;
        } catch (IOException ioe) {
            LOGGER.warning(
                    String.format(
                            "The connector jar package file %s storage failed.", storageFile));
        } finally {
            if (!success) {
                // delete storageFile from a failed download
                if (!storageFile.delete() && storageFile.exists()) {
                    // An exception occurred and the file that failed to write needs to be cleared.
                    LOGGER.warning(
                            String.format(
                                    "Could not delete the corrupted connector jar package file %s.",
                                    storageFile));
                }
            }
        }
        return success ? Optional.of(storageFile.toPath()) : Optional.empty();
    }

    private String getConnectorJarStorageDir() {
        String userDefinedStoragePath = connectorJarStorageConfig.getStoragePath();
        if (StringUtils.isNotBlank(userDefinedStoragePath)) {
            return new File(userDefinedStoragePath).getAbsolutePath();
        } else {
            // get SeatunnelHome
            return new File(
                            System.getProperty(
                                    SeaTunnelProperties.SEATUNNEL_HOME.getName(),
                                    SeaTunnelProperties.SEATUNNEL_HOME.getDefaultValue()))
                    .getAbsolutePath();
        }
    }

    @Override
    public void deleteConnectorJarInternal(File storageFile) {
        if (!storageFile.delete() && storageFile.exists()) {
            LOGGER.warning(String.format("Failed to delete connector jar file %s", storageFile));
        }
    }

    @Override
    public void deleteConnectorJarInExecutionNode(ConnectorJarIdentifier connectorJarIdentifier) {
        Address masterNodeAddress = nodeEngine.getMasterAddress();
        Collection<Member> memberList = nodeEngine.getClusterService().getMembers();
        memberList.forEach(
                member -> {
                    if (!member.getAddress().equals(masterNodeAddress)) {
                        NodeEngineUtil.sendOperationToMemberNode(
                                nodeEngine,
                                new DeleteConnectorJarInExecutionNode(connectorJarIdentifier),
                                member.getAddress());
                    }
                });
    }

    @Override
    public byte[] readConnectorJarByteDataInternal(File connectorJarFile) {
        try {
            // Read file data and convert it to a byte array.
            FileInputStream inputStream = new FileInputStream(connectorJarFile);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            LOGGER.warning(
                    String.format(
                            "Failed to read the connector jar package file : { %s } , the file to be read may not exist",
                            connectorJarFile));
            return new byte[0];
        }
    }
}
