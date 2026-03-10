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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.Condition;
import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.server.rest.response.OptionRuleResponse;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class OptionRulesService extends BaseService {

    private static final String PARAM_TYPE = "type";
    private static final String PARAM_PLUGIN = "plugin";

    private final Map<PluginType, PluginDiscovery<?>> pluginDiscoveries;
    private final ConcurrentMap<PluginType, LinkedHashMap<PluginIdentifier, OptionRule>>
            discoveredPluginsCache;
    private final ConcurrentMap<PluginType, ConcurrentMap<String, OptionRuleResponse>>
            responseCache;

    public OptionRulesService(NodeEngineImpl nodeEngine) {
        super(nodeEngine);
        Map<PluginType, PluginDiscovery<?>> discoveries = new EnumMap<>(PluginType.class);
        discoveries.put(PluginType.SOURCE, new SeaTunnelSourcePluginDiscovery());
        discoveries.put(PluginType.SINK, new SeaTunnelSinkPluginDiscovery());
        this.pluginDiscoveries = Collections.unmodifiableMap(discoveries);
        this.discoveredPluginsCache = new ConcurrentHashMap<>();
        this.responseCache = new ConcurrentHashMap<>();
    }

    public OptionRuleResponse getOptionRules(String pluginTypeText, String pluginName) {
        PluginType pluginType = parseSupportedPluginType(pluginTypeText);
        String normalizedPluginName = normalizePluginName(pluginName);
        return responseCache
                .computeIfAbsent(pluginType, key -> new ConcurrentHashMap<>())
                .computeIfAbsent(
                        normalizedPluginName,
                        key -> {
                            Map.Entry<PluginIdentifier, OptionRule> entry =
                                    getDiscoveredPlugins(pluginType).entrySet().stream()
                                            .filter(
                                                    pluginEntry ->
                                                            pluginEntry
                                                                    .getKey()
                                                                    .getPluginName()
                                                                    .equalsIgnoreCase(
                                                                            normalizedPluginName))
                                            .findFirst()
                                            .orElseThrow(
                                                    () ->
                                                            new NoSuchElementException(
                                                                    String.format(
                                                                            "Plugin '%s' not found for type '%s'.",
                                                                            pluginName.trim(),
                                                                            pluginType.getType())));
                            return buildResponse(entry.getKey(), entry.getValue());
                        });
    }

    OptionRuleResponse buildResponse(PluginIdentifier pluginIdentifier, OptionRule optionRule) {
        List<OptionRuleResponse.OptionMetadata> optionalOptions =
                optionRule.getOptionalOptions().stream()
                        .map(this::toOptionMetadata)
                        .collect(Collectors.toList());
        List<OptionRuleResponse.RequiredOptionMetadata> requiredOptions =
                optionRule.getRequiredOptions().stream()
                        .map(this::toRequiredOptionMetadata)
                        .collect(Collectors.toList());
        return new OptionRuleResponse(
                pluginIdentifier.getEngineType(),
                pluginIdentifier.getPluginType(),
                pluginIdentifier.getPluginName(),
                new OptionRuleResponse.OptionRuleMetadata(optionalOptions, requiredOptions));
    }

    private LinkedHashMap<PluginIdentifier, OptionRule> getDiscoveredPlugins(
            PluginType pluginType) {
        return discoveredPluginsCache.computeIfAbsent(
                pluginType,
                key -> {
                    PluginDiscovery<?> pluginDiscovery = pluginDiscoveries.get(key);
                    if (pluginDiscovery == null) {
                        throw new IllegalArgumentException(
                                String.format("Unsupported plugin type: %s", pluginType.getType()));
                    }
                    return pluginDiscovery.getPlugins();
                });
    }

    private PluginType parseSupportedPluginType(String pluginTypeText) {
        if (StringUtils.isBlank(pluginTypeText)) {
            throw new IllegalArgumentException(
                    String.format("Parameter '%s' cannot be empty.", PARAM_TYPE));
        }
        String normalizedPluginType = pluginTypeText.trim();
        if (StringUtils.equalsIgnoreCase(normalizedPluginType, PluginType.SOURCE.getType())) {
            return PluginType.SOURCE;
        }
        if (StringUtils.equalsIgnoreCase(normalizedPluginType, PluginType.SINK.getType())) {
            return PluginType.SINK;
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported plugin type '%s'. Only '%s' and '%s' are supported.",
                        normalizedPluginType,
                        PluginType.SOURCE.getType(),
                        PluginType.SINK.getType()));
    }

    private String normalizePluginName(String pluginName) {
        if (StringUtils.isBlank(pluginName)) {
            throw new IllegalArgumentException(
                    String.format("Parameter '%s' cannot be empty.", PARAM_PLUGIN));
        }
        return pluginName.trim().toLowerCase(Locale.ROOT);
    }

    private OptionRuleResponse.RequiredOptionMetadata toRequiredOptionMetadata(
            RequiredOption requiredOption) {
        List<OptionRuleResponse.OptionMetadata> options =
                requiredOption.getOptions().stream()
                        .map(this::toOptionMetadata)
                        .collect(Collectors.toList());
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            return new OptionRuleResponse.RequiredOptionMetadata(
                    OptionRuleResponse.RuleType.ABSOLUTELY_REQUIRED, options, null, null);
        }
        if (requiredOption instanceof RequiredOption.ExclusiveRequiredOptions) {
            return new OptionRuleResponse.RequiredOptionMetadata(
                    OptionRuleResponse.RuleType.EXCLUSIVE, options, null, null);
        }
        if (requiredOption instanceof RequiredOption.BundledRequiredOptions) {
            return new OptionRuleResponse.RequiredOptionMetadata(
                    OptionRuleResponse.RuleType.BUNDLED, options, null, null);
        }
        if (requiredOption instanceof RequiredOption.ConditionalRequiredOptions) {
            Expression expression =
                    ((RequiredOption.ConditionalRequiredOptions) requiredOption).getExpression();
            return new OptionRuleResponse.RequiredOptionMetadata(
                    OptionRuleResponse.RuleType.CONDITIONAL,
                    options,
                    expression.toString(),
                    toExpressionNode(expression));
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported required option type: %s",
                        requiredOption.getClass().getName()));
    }

    private OptionRuleResponse.ExpressionNode toExpressionNode(Expression expression) {
        if (expression == null) {
            return null;
        }
        return new OptionRuleResponse.ExpressionNode(
                toConditionNode(expression.getCondition()),
                toLogicalOperator(expression.and()),
                toExpressionNode(expression.getNext()));
    }

    private OptionRuleResponse.ConditionNode toConditionNode(Condition<?> condition) {
        if (condition == null) {
            return null;
        }
        return new OptionRuleResponse.ConditionNode(
                toOptionMetadata(condition.getOption()),
                condition.getExpectValue(),
                toLogicalOperator(condition.and()),
                toConditionNode(condition.getNext()));
    }

    private OptionRuleResponse.LogicalOperator toLogicalOperator(Boolean and) {
        if (and == null) {
            return null;
        }
        return and ? OptionRuleResponse.LogicalOperator.AND : OptionRuleResponse.LogicalOperator.OR;
    }

    private OptionRuleResponse.OptionMetadata toOptionMetadata(Option<?> option) {
        List<Object> optionValues = null;
        if (option instanceof SingleChoiceOption) {
            optionValues = new ArrayList<>(((SingleChoiceOption<?>) option).getOptionValues());
        }
        return new OptionRuleResponse.OptionMetadata(
                option.key(),
                option.typeReference().getType().getTypeName(),
                option.defaultValue(),
                option.getDescription(),
                new ArrayList<>(option.getFallbackKeys()),
                optionValues);
    }
}
