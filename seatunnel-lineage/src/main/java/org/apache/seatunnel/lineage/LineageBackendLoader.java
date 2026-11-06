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

package org.apache.seatunnel.lineage;

import java.lang.ref.WeakReference;
import java.util.ServiceLoader;

/** Loads the backend selected by the immutable lineage configuration. */
public final class LineageBackendLoader {

    /**
     * The most recent successful resolution.
     *
     * <p>Resolution scans the whole classpath, and every emitted event resolves the backend again.
     * The two callers that matter are on latency-sensitive engine paths, where that scan is pure
     * overhead: a process reports lineage through one transport loaded by one class loader, so the
     * same answer comes back every time. A single entry is enough to remove the repeat scan while
     * keeping the loader identity part of the answer.
     */
    private static volatile Resolution resolution;

    private LineageBackendLoader() {}

    /** Loads the backend whose name matches the configured transport. */
    public static LineageBackend load(LineageConfig config) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = LineageBackendLoader.class.getClassLoader();
        }
        Resolution cached = resolution;
        if (cached != null && cached.matches(loader, config.transport())) {
            return cached.backend;
        }
        for (LineageBackend candidate : ServiceLoader.load(LineageBackend.class, loader)) {
            if (config.transport().equalsIgnoreCase(candidate.getName())) {
                if (isLoadedWithThisClass(candidate)) {
                    resolution = new Resolution(loader, config.transport(), candidate);
                }
                return candidate;
            }
        }
        throw new IllegalArgumentException(
                "No LineageBackend is registered for transport " + config.transport());
    }

    /**
     * Returns whether caching the backend can extend nothing's lifetime.
     *
     * <p>A cached backend keeps its class, and therefore its class loader, reachable from a static
     * field. That is free when the backend comes from the same place as this class, which is the
     * case for both deployments that matter: the engine server jar on Zeta, and the shaded artifact
     * in the Flink cluster's lib/. It is not free when the context class loader resolves a copy
     * shipped inside a job's own jar, where caching would pin that job's class loader for the life
     * of the process. Those resolutions are simply not cached, which is the behaviour that existed
     * before the cache.
     */
    private static boolean isLoadedWithThisClass(LineageBackend backend) {
        return backend.getClass().getClassLoader() == LineageBackendLoader.class.getClassLoader();
    }

    private static final class Resolution {
        /**
         * Weak so that remembering which loader produced this answer cannot, on its own, keep that
         * loader alive; a collected reference simply misses and the lookup runs again.
         */
        private final WeakReference<ClassLoader> loader;

        private final String transport;
        private final LineageBackend backend;

        private Resolution(ClassLoader loader, String transport, LineageBackend backend) {
            this.loader = new WeakReference<>(loader);
            this.transport = transport;
            this.backend = backend;
        }

        private boolean matches(ClassLoader candidateLoader, String candidateTransport) {
            return loader.get() == candidateLoader
                    && transport.equalsIgnoreCase(candidateTransport);
        }
    }
}
