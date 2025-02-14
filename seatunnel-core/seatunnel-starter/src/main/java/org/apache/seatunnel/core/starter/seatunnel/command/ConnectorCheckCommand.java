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

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
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
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ConnectorCheckCommand implements Command<ConnectorCheckCommandArgs> {
    private static final String OPTION_DESCRIPTION_FORMAT = ", Description: '%s'";

    private static final String REQUIRED_OPTION_FORMAT = "Required Options: \n %s";

    private static final String OPTIONAL_OPTION_FORMAT = "Optional Options: \n %s";

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
                        .entrySet()
                        .forEach(
                                pluginTypePluginDiscoveryEntry ->
                                        printSupportedPlugins(
                                                pluginTypePluginDiscoveryEntry.getKey(),
                                                pluginTypePluginDiscoveryEntry
                                                        .getValue()
                                                        .getPlugins()));
            } else {
                printSupportedPlugins(pluginType, DISCOVERY_MAP.get(pluginType).getPlugins());
            }
        }

        String pluginIdentifier = connectorCheckCommandArgs.getPluginIdentifier();
        // print option rule of the connector
        if (StringUtils.isNoneBlank(pluginIdentifier)) {
            if (Objects.isNull(pluginType)) {
                DISCOVERY_MAP
                        .entrySet()
                        .forEach(
                                pluginTypePluginDiscoveryEntry -> {
                                    printOptionRulesByPluginTypeAndIdentifier(
                                            pluginTypePluginDiscoveryEntry.getValue(),
                                            pluginIdentifier);
                                });
            } else {
                printOptionRulesByPluginTypeAndIdentifier(
                        DISCOVERY_MAP.get(pluginType), pluginIdentifier);
            }
        }
    }

    private void printOptionRulesByPluginTypeAndIdentifier(
            PluginDiscovery DISCOVERY_MAP, String pluginIdentifier) {
        ImmutableTriple<PluginIdentifier, List<Option<?>>, List<Option<?>>> triple =
                DISCOVERY_MAP.getOptionRules(pluginIdentifier);
        if (Objects.nonNull(triple.getLeft())) {
            printOptionRules(triple.getLeft(), triple.getMiddle(), triple.getRight());
        }
    }

    private void printSupportedPlugins(
            PluginType pluginType, LinkedHashMap<PluginIdentifier, OptionRule> plugins) {
        System.out.println(StringUtils.LF + StringUtils.capitalize(pluginType.getType()));
        String supportedPlugins =
                plugins.keySet().stream()
                        .map(pluginIdentifier -> pluginIdentifier.getPluginName())
                        .collect(Collectors.joining(StringUtils.SPACE));
        System.out.println(supportedPlugins + StringUtils.LF);
    }

    private void printOptionRules(
            PluginIdentifier pluginIdentifier,
            List<Option<?>> requiredOptions,
            List<Option<?>> optionOptions) {
        System.out.println(
                StringUtils.LF
                        + pluginIdentifier.getPluginName()
                        + StringUtils.SPACE
                        + pluginIdentifier.getPluginType());
        System.out.println(
                String.format(REQUIRED_OPTION_FORMAT, getOptionRulesString(requiredOptions)));
        if (optionOptions.size() > 0) {
            System.out.println(
                    String.format(OPTIONAL_OPTION_FORMAT, getOptionRulesString(optionOptions)));
        }
    }

    private static String getOptionRulesString(List<Option<?>> requiredOptions) {
        String requiredOptionsString =
                requiredOptions.stream()
                        .map(
                                option ->
                                        String.format(
                                                        option.toString()
                                                                + OPTION_DESCRIPTION_FORMAT,
                                                        option.getDescription())
                                                + StringUtils.LF)
                        .collect(Collectors.joining(StringUtils.SPACE));
        return requiredOptionsString;
    }
}
