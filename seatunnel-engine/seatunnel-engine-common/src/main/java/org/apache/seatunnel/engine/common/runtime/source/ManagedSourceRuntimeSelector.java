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

package org.apache.seatunnel.engine.common.runtime.source;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.managed.ManagedSourceCapability;
import org.apache.seatunnel.engine.common.config.server.ManagedSourceRuntimeConfig;

import java.util.List;
import java.util.Locale;

/** Selects the Source execution lane once, before task deployment. */
public final class ManagedSourceRuntimeSelector {

    private ManagedSourceRuntimeSelector() {}

    public static ManagedSourceRuntimeSelection select(
            SeaTunnelSource<?, ?, ?> source, ManagedSourceRuntimeConfig config) {
        config.validate();
        List<String> allowlist = config.normalizedConnectorAllowlist();
        if (!config.isEnabled() || allowlist.isEmpty()) {
            return ManagedSourceRuntimeSelection.legacy();
        }
        String pluginName = source.getPluginName();
        if (pluginName == null || pluginName.trim().isEmpty()) {
            throw new IllegalStateException(
                    "Managed Source connector plugin name must not be blank");
        }
        String connectorName = pluginName.trim().toLowerCase(Locale.ROOT);
        if (!allowlist.contains(connectorName)) {
            return ManagedSourceRuntimeSelection.legacy();
        }

        ManagedSourceCapability capability = source.getManagedSourceCapability();
        if (capability == null || capability.isLegacy()) {
            throw new IllegalStateException(
                    "Connector "
                            + source.getPluginName()
                            + " is allowlisted for the managed Source runtime but declares no managed capability");
        }
        if (capability.getRuntimeProtocolVersion() != config.getRuntimeProtocolVersion()) {
            throw new IllegalStateException(
                    "Managed Source protocol mismatch for connector "
                            + source.getPluginName()
                            + ": engine="
                            + config.getRuntimeProtocolVersion()
                            + ", connector="
                            + capability.getRuntimeProtocolVersion());
        }
        if (capability.usesSourceEvents()) {
            throw new IllegalStateException(
                    "Connector "
                            + source.getPluginName()
                            + " uses SourceEvents, which are not supported by managed Source protocol version 1");
        }
        if (capability.supportsManagedCoordinator() && !capability.supportsAsyncEnumerator()) {
            throw new IllegalStateException(
                    "Connector "
                            + source.getPluginName()
                            + " declares a managed coordinator but does not opt into the event-loop-safe enumerator contract");
        }

        ManagedSourceRuntimeMode mode;
        if (capability.supportsManagedReader() && capability.supportsManagedCoordinator()) {
            mode = ManagedSourceRuntimeMode.MANAGED_READER_AND_COORDINATOR;
        } else if (capability.supportsManagedReader()) {
            mode = ManagedSourceRuntimeMode.MANAGED_READER;
        } else if (capability.supportsManagedCoordinator()) {
            mode = ManagedSourceRuntimeMode.MANAGED_COORDINATOR;
        } else {
            throw new IllegalStateException(
                    "Managed Source capability for "
                            + source.getPluginName()
                            + " does not enable a runtime component");
        }
        return new ManagedSourceRuntimeSelection(
                mode,
                capability.getRuntimeProtocolVersion(),
                capability.getConnectorStateVersion(),
                capability.getCapabilityDigest());
    }
}
