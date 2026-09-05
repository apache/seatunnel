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

package org.apache.seatunnel.engine.server.service.jar;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
        // Initializing the cleanup task
        this.cleanupTimer = new Timer(true);
        this.cleanupInterval = connectorJarStorageConfig.getCleanupTaskInterval() * 1000;
        this.cleanupTimer.schedule(
                new SharedConnectorJarCleanupTask(
                        this::deleteConnectorJar, connectorJarRefCounters),
                cleanupInterval,
                cleanupInterval);
    }

    /**
     * Stores a shared connector jar and reserves its first reference.
     *
     * <p>The distributed identifier lock fences stale coordinator cleanup while the local file and
     * shared reference are rebuilt.
     *
     * @param jobId job identifier
     * @param connectorJar connector jar payload
     * @return connector jar storage identifier
     */
    @Override
    public ConnectorJarIdentifier storageConnectorJarFile(long jobId, ConnectorJar connectorJar) {
        ConnectorJarIdentifier connectorJarIdentifier =
                ConnectorJarIdentifier.of(
                        connectorJar, getStorageLocationPath(jobId, connectorJar));
        connectorJarRefCounters.lock(connectorJarIdentifier);
        try {
            readWriteLock.writeLock().lock();
            RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
            if (refCount == null) {
                refCount = new RefCount();
            }
            File storageLocation = getStorageLocation(jobId, connectorJar);
            if (!storageLocation.exists()) {
                if (!storageConnectorJarFileInternal(connectorJar, storageLocation).isPresent()) {
                    throw new SeaTunnelEngineException(
                            String.format(
                                    "Failed to store shared connector jar %s.", storageLocation));
                }
            }
            Long references = refCount.getReferences();
            refCount.setReferences(++references);
            connectorJarRefCounters.put(connectorJarIdentifier, refCount);
            return connectorJarIdentifier;
        } finally {
            readWriteLock.writeLock().unlock();
            connectorJarRefCounters.unlock(connectorJarIdentifier);
        }
    }

    @Override
    public boolean checkConnectorJarExisted(long jobId, ConnectorJar connectorJar) {
        ConnectorJarIdentifier connectorJarIdentifier =
                ConnectorJarIdentifier.of(
                        connectorJar, getStorageLocationPath(jobId, connectorJar));
        RefCount refCount = connectorJarRefCounters.get(connectorJarIdentifier);
        return refCount != null && new File(connectorJarIdentifier.getStoragePath()).exists();
    }

    /**
     * Atomically reserves one reference before cluster replication starts.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     * @return {@code true} when the existing reference was reserved, or {@code false} when cleanup
     *     already removed the record
     */
    public boolean increaseRefCountForConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.lock(connectorJarIdentifier);
        try {
            readWriteLock.writeLock().lock();
            AtomicBoolean referenceReserved = new AtomicBoolean();
            connectorJarRefCounters.compute(
                    connectorJarIdentifier,
                    (identifier, refCount) -> {
                        if (refCount == null) {
                            return null;
                        }
                        if (!new File(connectorJarIdentifier.getStoragePath()).exists()) {
                            return refCount;
                        }
                        refCount.setReferences(refCount.getReferences() + 1);
                        referenceReserved.set(true);
                        return refCount;
                    });
            return referenceReserved.get();
        } finally {
            readWriteLock.writeLock().unlock();
            connectorJarRefCounters.unlock(connectorJarIdentifier);
        }
    }

    /**
     * Atomically claims and deletes an unreferenced shared connector jar.
     *
     * <p>If an upload reserves a provisional reference before the claim, cleanup leaves the map
     * entry and files untouched.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     */
    @Override
    public void deleteConnectorJar(ConnectorJarIdentifier connectorJarIdentifier) {
        AtomicBoolean cleanupClaimed = new AtomicBoolean();
        connectorJarRefCounters.lock(connectorJarIdentifier);
        try {
            readWriteLock.writeLock().lock();
            connectorJarRefCounters.compute(
                    connectorJarIdentifier,
                    (identifier, refCount) -> {
                        if (refCount != null && refCount.getReferences() <= 0) {
                            cleanupClaimed.set(true);
                            return null;
                        }
                        return refCount;
                    });
            if (!cleanupClaimed.get()) {
                return;
            }
            File storageLocation = new File(connectorJarIdentifier.getStoragePath());
            deleteConnectorJarInternal(storageLocation);
            deleteConnectorJarInExecutionNode(connectorJarIdentifier);
        } catch (RuntimeException e) {
            if (cleanupClaimed.get()) {
                // Keep a zero-reference tombstone so the next cleanup cycle retries any local or
                // remote deletion that did not acknowledge.
                RefCount retryCleanup = new RefCount();
                connectorJarRefCounters.put(connectorJarIdentifier, retryCleanup);
            }
            throw e;
        } finally {
            readWriteLock.writeLock().unlock();
            connectorJarRefCounters.unlock(connectorJarIdentifier);
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
        connectorJarIdentifierList.forEach(this::decreaseConnectorJarRefCount);
    }

    /**
     * Decreases one committed shared connector jar reference.
     *
     * <p>The distributed identifier lock serializes the update with upload and cleanup across
     * coordinator generations.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     */
    public void decreaseConnectorJarRefCount(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.lock(connectorJarIdentifier);
        try {
            readWriteLock.writeLock().lock();
            connectorJarRefCounters.compute(
                    connectorJarIdentifier,
                    (connectorJarIdentifier1, refCount) -> {
                        if (refCount != null) {
                            Long references = refCount.getReferences();
                            refCount.setReferences(--references);
                        }
                        return refCount;
                    });
        } finally {
            readWriteLock.writeLock().unlock();
            connectorJarRefCounters.unlock(connectorJarIdentifier);
        }
    }

    /**
     * Rolls back one reference after cluster replication fails.
     *
     * <p>When no committed reference remains, a zero-reference tombstone keeps partially replicated
     * remote copies visible to the cleanup timer. A retry can reserve the tombstone before cleanup
     * claims it and idempotently repair any missing replicas.
     *
     * @param connectorJarIdentifier connector jar storage identifier
     */
    public void rollbackConnectorJarRefCount(ConnectorJarIdentifier connectorJarIdentifier) {
        connectorJarRefCounters.lock(connectorJarIdentifier);
        try {
            readWriteLock.writeLock().lock();
            connectorJarRefCounters.compute(
                    connectorJarIdentifier,
                    (identifier, refCount) -> {
                        if (refCount == null) {
                            return null;
                        }
                        long references = refCount.getReferences() - 1;
                        if (references <= 0) {
                            refCount.setReferences(0L);
                            return refCount;
                        }
                        refCount.setReferences(references);
                        return refCount;
                    });
        } finally {
            readWriteLock.writeLock().unlock();
            connectorJarRefCounters.unlock(connectorJarIdentifier);
        }
    }
}
