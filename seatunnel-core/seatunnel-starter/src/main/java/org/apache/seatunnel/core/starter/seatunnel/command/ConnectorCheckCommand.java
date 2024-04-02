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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ConnectorCheckCommandArgs;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConnectorCheckCommand implements Command<ConnectorCheckCommandArgs> {

    private static final Map<PluginType, PluginDiscovery> DISCOVERY_MAP = new HashMap();
    private ConnectorCheckCommandArgs connectorCheckCommandArgs;

    public ConnectorCheckCommand(ConnectorCheckCommandArgs connectorCheckCommandArgs) {
        this.connectorCheckCommandArgs = connectorCheckCommandArgs;
        this.DISCOVERY_MAP.put(PluginType.SOURCE, new SeaTunnelSourcePluginDiscovery());
        this.DISCOVERY_MAP.put(PluginType.SINK, new SeaTunnelSinkPluginDiscovery());
        this.DISCOVERY_MAP.put(PluginType.TRANSFORM, new SeaTunnelTransformPluginDiscovery());
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        PluginType pluginType = connectorCheckCommandArgs.getPluginType();

        // Print plugins(connectors and transforms)
        if (connectorCheckCommandArgs.isListConnectors()) {
            if (Objects.isNull(pluginType)) {
                DISCOVERY_MAP
                        .values()
                        .forEach(pluginDiscovery -> pluginDiscovery.printSupportedPlugins());
            } else {
                DISCOVERY_MAP.get(pluginType).printSupportedPlugins();
            }
        }

        String pluginIdentifier = connectorCheckCommandArgs.getPluginIdentifier();
        // print option rule of the connector
        if (StringUtils.isNoneBlank(pluginIdentifier)) {
            if (Objects.isNull(pluginType)) {
                DISCOVERY_MAP
                        .values()
                        .forEach(
                                pluginDiscovery ->
                                        pluginDiscovery.printOptionRules(pluginIdentifier));
            } else {
                DISCOVERY_MAP.get(pluginType).printOptionRules(pluginIdentifier);
            }
        }
    }
}
