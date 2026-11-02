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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The single seam through which every engine emits a lineage event.
 *
 * <p>The event carries its own {@link LineageEventType}, so there is deliberately no per-lifecycle
 * method here: a reporter-style API would need one method per type and would still leave the caller
 * to route the types it does not cover.
 */
public final class LineageRuntime {
    private static final Logger LOGGER = Logger.getLogger(LineageRuntime.class.getName());

    private LineageRuntime() {}

    /**
     * Emits an event through the configured backend, or returns immediately when disabled.
     *
     * <p>Never throws. A backend that cannot be resolved (an unknown {@code openlineage_transport},
     * or a deployment missing the backend's jar/provider file) or one that violates its own
     * no-throw contract only produces a WARNING log line here. Lineage delivery must not be able to
     * change the outcome of the job it is describing, and that guarantee has to hold at this shared
     * entry point rather than depend on every engine call site independently remembering to guard
     * it.
     */
    public static void emit(LineageConfig config, LineageEvent event) {
        if (!config.enabled()) {
            return;
        }
        try {
            LineageBackendLoader.load(config).emit(config, event);
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "Failed to emit a lineage event", e);
        }
    }
}
