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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.job.RefCount;

import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.File;
import java.nio.file.Path;
import java.util.Timer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class SharedConnectorJarStorageStrategy extends AbstractConnectorJarStorageStrategy {

    /** Lock guarding concurrent file accesses. */
    private final ReadWriteLock readWriteLock;

    private final IMap<String, RefCount> connectorJarRefCounters;

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    public SharedConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig, NodeEngineImpl nodeEngine) {
        super(connectorJarStorageConfig);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.connectorJarRefCounters =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_CONNECTOR_JAR_REF_COUNTERS);
        // Initializing the clean up task
        this.cleanupTimer = new Timer(true);
        this.cleanupInterval = connectorJarStorageConfig.getCleanupTaskInterval() * 1000;
        this.cleanupTimer.schedule(
                new SharedConnectorJarCleanupTask(
                        LOGGER, this::deleteConnectorJar, connectorJarRefCounters),
                cleanupInterval,
                cleanupInterval);
    }

    @Override
    public String storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        String connectorJarFileName = connectorJar.getFileName();
        RefCount refCount = connectorJarRefCounters.get(connectorJarFileName);
        if (refCount == null) {
            refCount = new RefCount();
            File storageLocation = getStorageLocation(jobId, connectorJar);
            try {
                readWriteLock.writeLock().lock();
                Path path = storageConnectorJarFileInternal(connectorJar, storageLocation);
                refCount.setStoragePath(path.toString());
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
        // increment reference counts for connector jar
        Long references = refCount.getReferences();
        refCount.setReferences(++references);
        connectorJarRefCounters.put(connectorJarFileName, refCount);
        return refCount.getStoragePath();
    }

    @Override
    public void deleteConnectorJar(long jobId, String connectorJarFileName) {
        RefCount refCount = connectorJarRefCounters.get(connectorJarFileName);
        if (refCount != null) {
            try {
                File storageLocation = new File(refCount.getStoragePath());
                readWriteLock.writeLock().lock();
                deleteConnectorJarInternal(storageLocation);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    @Override
    public String getStorageLocationPath(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        if (connectorJar.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            return String.format(
                    "%s/%s/%s/%s/%s",
                    storageDir,
                    COMMON_PLUGIN_JAR_STORAGE_PATH,
                    connectorJar.getPluginName(),
                    "lib",
                    connectorJar.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s",
                    storageDir, CONNECTOR__PLUGIN_JAR_STORAGE_PATH, connectorJar.getFileName());
        }
    }

    public void decreaseConnectorJarRefCount(String connectorJarFileName) {
        connectorJarRefCounters.compute(
                connectorJarFileName,
                new BiFunction<String, RefCount, RefCount>() {
                    @Override
                    public RefCount apply(String connectorJarFileName, RefCount refCount) {
                        if (refCount != null) {
                            Long references = refCount.getReferences();
                            refCount.setReferences(--references);
                        }
                        return refCount;
                    }
                });
    }

    @Override
    public String getStoragePathFromJarName(String connectorJarName) {
        RefCount refCount = connectorJarRefCounters.get(connectorJarName);
        if (refCount == null) {
            LOGGER.warning(String.format(
                    "Failed to obtain the storage path of the jar package" +
                            " on the master node through the jar package name : { %s }," +
                            " because the current jar package file does not exist on the master node.",
                    connectorJarName
            ));
            return "";
        }
        return refCount.getStoragePath();
    }

    @Override
    public byte[] readConnectorJarByteData(File connectorJarFile) {
        readWriteLock.readLock().lock();
        try {
            return readConnectorJarByteDataInternal(connectorJarFile);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }
}
