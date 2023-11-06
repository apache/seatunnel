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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ServerConnectorPackageClient {

    private static final ILogger LOGGER = Logger.getLogger(ServerConnectorPackageClient.class);

    private final NodeEngineImpl nodeEngine;

    private final ReadWriteLock readWriteLock;

    public ServerConnectorPackageClient(
            NodeEngineImpl nodeEngine, SeaTunnelConfig seaTunnelConfig) {
        this.nodeEngine = nodeEngine;
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public Set<URL> getConnectorJarFromLocal(Set<ConnectorJarIdentifier> connectorJarIdentifiers) {
        return connectorJarIdentifiers.stream()
                .map(
                        connectorJarIdentifier -> {
                            String connectorJarStoragePath =
                                    connectorJarIdentifier.getStoragePath();
                            File storageFile = new File(connectorJarStoragePath);
                            try {
                                if (storageFile.exists()) {
                                    return Optional.of(storageFile.toURI().toURL());
                                } else {
                                    return Optional.empty();
                                }
                            } catch (MalformedURLException e) {
                                LOGGER.warning(
                                        String.format("Cannot get plugin URL: {%s}", storageFile));
                                return Optional.empty();
                            }
                        })
                .filter(Optional::isPresent)
                .map(
                        optional -> {
                            return (URL) optional.get();
                        })
                .collect(Collectors.toSet());
    }

    public void storageConnectorJarFile(
            byte[] connectorJarByteData, ConnectorJarIdentifier connectorJarIdentifier) {
        readWriteLock.writeLock().lock();
        storageConnectorJarFile(
                connectorJarByteData, new File(connectorJarIdentifier.getStoragePath()));
        readWriteLock.writeLock().unlock();
    }

    private void storageConnectorJarFile(byte[] connectorJarByteData, File storageFile) {
        boolean success = false;
        try {
            if (!storageFile.exists()) {
                FileOutputStream fos = new FileOutputStream(storageFile);
                fos.write(connectorJarByteData);
            } else {
                LOGGER.warning(
                        String.format(
                                "File storage for an existing file %s. "
                                        + "This may indicate a duplicate download. Ignoring newest download.",
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
    }

    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        try {
            File storageLocation = new File(connectorJarIdentifier.getStoragePath());
            readWriteLock.writeLock().lock();
            deleteConnectorJarInternal(storageLocation);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    private void deleteConnectorJarInternal(File storageFile) {
        if (!storageFile.delete() && storageFile.exists()) {
            LOGGER.warning(String.format("Failed to delete connector jar file %s", storageFile));
        }
    }
}
