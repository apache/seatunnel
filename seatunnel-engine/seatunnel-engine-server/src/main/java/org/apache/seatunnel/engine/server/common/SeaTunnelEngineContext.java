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

package org.apache.seatunnel.engine.server.common;

import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;

import java.util.Objects;

/**
 * Shared engine context propagated inside the engine.
 *
 * <p>This context is intended to reduce direct propagation of infrastructure-specific runtime
 * objects into engine services. At the current stage, it only exposes state store bundles and
 * serves as a small entry point for state-related abstractions.
 */
public final class SeaTunnelEngineContext implements AutoCloseable {

    private final EngineStateStores stateStores;

    private SeaTunnelEngineContext(Builder builder) {
        this.stateStores = builder.stateStores;
    }

    public static Builder builder(EngineStateStores stateStores) {
        return new Builder(stateStores);
    }

    /**
     * Returns the grouped engine state stores.
     *
     * @return engine state stores
     */
    public EngineStateStores getStateStores() {
        return stateStores;
    }

    @Override
    public void close() {
        stateStores.close();
    }

    /** Builder for {@link SeaTunnelEngineContext}. */
    public static final class Builder {
        private final EngineStateStores stateStores;

        private Builder(EngineStateStores stateStores) {
            this.stateStores = Objects.requireNonNull(stateStores, "stateStores");
        }

        public SeaTunnelEngineContext build() {
            return new SeaTunnelEngineContext(this);
        }
    }
}
