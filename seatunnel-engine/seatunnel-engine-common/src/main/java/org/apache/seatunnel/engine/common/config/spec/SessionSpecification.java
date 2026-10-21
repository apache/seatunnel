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

package org.apache.seatunnel.engine.common.config.spec;

import lombok.Getter;

/**
 * Immutable runtime settings for one Kubernetes session cluster.
 *
 * <p>The worker specification is shared by every worker in the session. The MVP intentionally
 * supports one homogeneous worker template per session; changing the template requires creating a
 * new session.
 */
@Getter
public final class SessionSpecification {

    private final String name;
    private final String clusterName;
    private final int minWorkers;
    private final int maxWorkers;
    private final WorkerSpecification workerSpecification;
    private final boolean dynamicWorkerEnabled;
    private final long startupTimeoutMillis;
    private final long retryIntervalMillis;

    /**
     * Creates one validated session specification.
     *
     * @param name session name used in Kubernetes labels and log messages
     * @param clusterName Hazelcast cluster name shared by all session members
     * @param minWorkers number of workers created at session startup
     * @param maxWorkers upper bound for automatic worker provisioning
     * @param workerSpecification homogeneous worker resource envelope
     * @param dynamicWorkerEnabled whether pending jobs may provision additional workers
     * @param startupTimeoutMillis maximum wait for requested workers to register
     * @param retryIntervalMillis interval used while waiting for worker registration
     */
    public SessionSpecification(
            String name,
            String clusterName,
            int minWorkers,
            int maxWorkers,
            WorkerSpecification workerSpecification,
            boolean dynamicWorkerEnabled,
            long startupTimeoutMillis,
            long retryIntervalMillis) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("session.name must not be empty");
        }
        if (clusterName == null || clusterName.trim().isEmpty()) {
            throw new IllegalArgumentException("session.cluster-name must not be empty");
        }
        if (minWorkers < 1) {
            throw new IllegalArgumentException("session.worker.min-workers must be >= 1");
        }
        if (maxWorkers < minWorkers) {
            throw new IllegalArgumentException(
                    "session.worker.max-workers must be >= session.worker.min-workers");
        }
        if (workerSpecification == null) {
            throw new IllegalArgumentException("session.worker specification must not be null");
        }
        if (startupTimeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "session.dynamic-worker.startup-timeout-millis must be > 0");
        }
        if (retryIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                    "session.dynamic-worker.retry-interval-millis must be > 0");
        }
        this.name = name.trim();
        this.clusterName = clusterName.trim();
        this.minWorkers = minWorkers;
        this.maxWorkers = maxWorkers;
        this.workerSpecification = workerSpecification;
        this.dynamicWorkerEnabled = dynamicWorkerEnabled;
        this.startupTimeoutMillis = startupTimeoutMillis;
        this.retryIntervalMillis = retryIntervalMillis;
    }
}
