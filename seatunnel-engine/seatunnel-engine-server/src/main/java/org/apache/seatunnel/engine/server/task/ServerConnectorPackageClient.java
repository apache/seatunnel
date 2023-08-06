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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.task.operation.DownloadConnectorJarOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class ServerConnectorPackageClient {

    private static final ILogger LOGGER = Logger.getLogger(ServerConnectorPackageClient.class);

    private final NodeEngineImpl nodeEngine;

    private final ConnectorJarStorageConfig connectorJarStorageConfig;

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    private final long connectorJarExpiryTime;

    private final ReadWriteLock readWriteLock;

    /** a time-to-live (TTL) and storage location for connector jar package. */
    static class ExpiryTime {
        /** expiry time (no cleanup for non-positive values). */
        public long keepUntil = -1;

        public String storagePath = "";

        public ExpiryTime(long keepUntil, String storagePath) {
            this.keepUntil = keepUntil;
            this.storagePath = storagePath;
        }
    }

    /** Map to store the TTL of each connector jar package stored in the local storage. */
    private final ConcurrentHashMap<String, ExpiryTime> connectorJarExpiryTimes =
            new ConcurrentHashMap<>();

    public ServerConnectorPackageClient(
            NodeEngineImpl nodeEngine, SeaTunnelConfig seaTunnelConfig) {
        this.nodeEngine = nodeEngine;
        this.connectorJarStorageConfig =
                seaTunnelConfig.getEngineConfig().getConnectorJarStorageConfig();
        this.connectorJarExpiryTime = connectorJarStorageConfig.getConnectorJarExpiryTime();
        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);
        this.cleanupInterval = connectorJarStorageConfig.getCleanupTaskInterval() * 1000;
        this.cleanupTimer.schedule(
                new ServerConnectorJarCleanupTask(
                        LOGGER, this::deleteConnectorJar, connectorJarExpiryTimes),
                cleanupInterval,
                cleanupInterval);
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public Set<URL> getConnectorJarPath(Set<URL> connectorJars) {
        return connectorJars.stream()
                .map(
                        connectorJar -> {
                            String connectorJarStoragePath =
                                    getConnectorJarFileLocallyFirst(connectorJar.getFile());
                            File storageFile = new File(connectorJarStoragePath);
                            try {
                                return Optional.of(storageFile.toURI().toURL());
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

    public String getConnectorJarFileLocallyFirst(String connectorJarName) {
        ExpiryTime expiryTime = connectorJarExpiryTimes.get(connectorJarName);
        if (expiryTime != null) {
            String storagePath = expiryTime.storagePath;
            // update TTL for connector jar package in connectorJarExpiryTimes
            expiryTime.keepUntil = System.currentTimeMillis() + connectorJarExpiryTime;
            connectorJarExpiryTimes.put(connectorJarName, expiryTime);
            return storagePath;
        } else {
            String storagePath = downloadFromMasterNode(connectorJarName);
            connectorJarExpiryTimes.put(
                    connectorJarName,
                    new ExpiryTime(
                            System.currentTimeMillis() + connectorJarExpiryTime, storagePath));
            return storagePath;
        }
    }

    public String downloadFromMasterNode(String connectorJarName) {
        ImmutablePair<byte[], String> immutablePair = null;
        InvocationFuture<ImmutablePair<byte[], String>> invocationFuture =
                NodeEngineUtil.sendOperationToMasterNode(
                        nodeEngine, new DownloadConnectorJarOperation(connectorJarName));
        try {
            immutablePair = invocationFuture.get();
        }
        // HazelcastInstanceNotActiveException. It means that the node is
        // offline, so waiting for restoration can be successful.
        catch (HazelcastInstanceNotActiveException e) {
            LOGGER.warning(
                    String.format(
                            "Download connector jar package from master node with exception: %s.",
                            ExceptionUtils.getMessage(e)));
        } catch (Exception e) {
            throw new SeaTunnelEngineException(ExceptionUtils.getMessage(e));
        }
        if (immutablePair == null) return "";
        byte[] connectorJarByteData = immutablePair.getLeft();
        String storagePath = immutablePair.getRight();
        readWriteLock.writeLock().lock();
        storageConnectorJarFile(connectorJarByteData, new File(storagePath));
        readWriteLock.writeLock().unlock();
        return storagePath;
    }

    private void storageConnectorJarFile(byte[] connectorJarByteData, File storageFile) {
        boolean success = false;
        try {
            if (!storageFile.exists()) {
                FileOutputStream fos = new FileOutputStream(storageFile);
                // md.update(value);
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

    public void deleteConnectorJar(String connectorJarFileName) {
        ExpiryTime expiryTime = connectorJarExpiryTimes.get(connectorJarFileName);
        if (expiryTime != null) {
            try {
                File storageLocation = new File(expiryTime.storagePath);
                readWriteLock.writeLock().lock();
                deleteConnectorJarInternal(storageLocation);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    private void deleteConnectorJarInternal(File storageFile) {
        if (!storageFile.delete() && storageFile.exists()) {
            LOGGER.warning(String.format("Failed to delete connector jar file %s", storageFile));
        }
    }
}
