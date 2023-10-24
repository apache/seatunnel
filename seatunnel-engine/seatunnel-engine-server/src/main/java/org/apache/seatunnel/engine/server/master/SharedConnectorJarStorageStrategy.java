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
import org.apache.seatunnel.engine.core.job.CommonPluginJar;
import org.apache.seatunnel.engine.core.job.ConnectorJar;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ConnectorJarType;
import org.apache.seatunnel.engine.core.job.RefCount;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

import com.hazelcast.map.IMap;

import java.io.File;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class SharedConnectorJarStorageStrategy extends AbstractConnectorJarStorageStrategy {

    /** Lock guarding concurrent file accesses. */
    private final ReadWriteLock readWriteLock;

    private final IMap<ConnectorJarIdentifier, RefCount> connectorJarRefCounters;

    /** Time interval (ms) to run the cleanup task; also used as the default TTL. */
    private final long cleanupInterval;

    /** Timer task to execute the cleanup at regular intervals. */
    private final Timer cleanupTimer;

    public SharedConnectorJarStorageStrategy(
            ConnectorJarStorageConfig connectorJarStorageConfig, SeaTunnelServer seaTunnelServer) {
        super(connectorJarStorageConfig, seaTunnelServer);
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
    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        ConnectorJarIdentifier connectorJarIdentifier =
                ConnectorJarIdentifier.of(
                        connectorJar, getStorageLocationPath(jobId, connectorJar));
        RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
        if (refCount == null) {
            refCount = new RefCount();
            File storageLocation = getStorageLocation(jobId, connectorJar);
            try {
                readWriteLock.writeLock().lock();
                storageConnectorJarFileInternal(connectorJar, storageLocation);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
        // increment reference counts for connector jar
        Long references = refCount.getReferences();
        refCount.setReferences(++references);
        connectorJarRefCounters.put(connectorJarIdentifier, refCount);
        return connectorJarIdentifier;
    }

    @Override
    public boolean checkConnectorJarExisted(long jobId, ConnectorJar connectorJar) {
        ConnectorJarIdentifier connectorJarIdentifier =
                ConnectorJarIdentifier.of(
                        connectorJar, getStorageLocationPath(jobId, connectorJar));
        RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
        if (refCount != null) {
            return true;
        }
        return false;
    }

    public void increaseRefCountForConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
        if (refCount != null) {
            // increment reference counts for connector jar
            Long references = refCount.getReferences();
            refCount.setReferences(++references);
            connectorJarRefCounters.put(connectorJarIdentifier, refCount);
        }
    }

    @Override
    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
        if (refCount != null) {
            try {
                File storageLocation = new File(connectorJarIdentifier.getStoragePath());
                readWriteLock.writeLock().lock();
                deleteConnectorJarInternal(storageLocation);
                deleteConnectorJarInExecutionNode(connectorJarIdentifier);
                connectorJarRefCounters.remove(connectorJarIdentifier);
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    @Override
    public String getStorageLocationPath(long jobId, ConnectorJar connectorJar) {
        checkNotNull(jobId);
        if (connectorJar.getType() == ConnectorJarType.COMMON_PLUGIN_JAR) {
            CommonPluginJar commonPluginJar = (CommonPluginJar) connectorJar;
            return String.format(
                    "%s/%s/%s",
                    storageDir, COMMON_PLUGIN_JAR_STORAGE_PATH, commonPluginJar.getFileName());
        } else {
            return String.format(
                    "%s/%s/%s",
                    storageDir, CONNECTOR_PLUGIN_JAR_STORAGE_PATH, connectorJar.getFileName());
        }
    }

    @Override
    public void cleanUpWhenJobFinished(
            long jobId, List<ConnectorJarIdentifier> connectorJarIdentifierList) {
        connectorJarIdentifierList.forEach(
                connectorJarIdentifier -> {
                    decreaseConnectorJarRefCount(connectorJarIdentifier);
                });
    }

    public void decreaseConnectorJarRefCount(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.compute(
                connectorJarIdentifier,
                new BiFunction<ConnectorJarIdentifier, RefCount, RefCount>() {
                    @Override
                    public RefCount apply(
                            ConnectorJarIdentifier connectorJarIdentifier, RefCount refCount) {
                        if (refCount != null) {
                            Long references = refCount.getReferences();
                            refCount.setReferences(--references);
                        }
                        return refCount;
                    }
                });
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
