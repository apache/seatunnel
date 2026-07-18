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
import org.apache.seatunnel.api.configuration.util.ConditionOperator;
import org.apache.seatunnel.api.configuration.util.ConditionRule;
import org.apache.seatunnel.api.configuration.util.Expression;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.server.rest.response.OptionRuleResponse;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Service for exposing runtime connector option rules via REST APIs.
 *
 * <p>The returned metadata is loaded from runtime plugin discovery and cached for the lifetime of
 * the service instance. The cache can be cleared explicitly if plugin metadata needs to be
 * refreshed.
 */
@Slf4j
public class OptionRulesService extends BaseService {

    private static final String PARAM_TYPE = "type";
    private static final String PARAM_PLUGIN = "plugin";
    private static final Map<Class<? extends RequiredOption>, OptionRuleResponse.RuleType>
            REQUIRED_OPTION_RULE_TYPES = createRequiredOptionRuleTypes();

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
        discoveries.put(PluginType.TRANSFORM, new SeaTunnelTransformPluginDiscovery());
        this.pluginDiscoveries = Collections.unmodifiableMap(discoveries);
        this.discoveredPluginsCache = new ConcurrentHashMap<>();
        this.responseCache = new ConcurrentHashMap<>();
    }

    /**
     * Returns the full option rule metadata of a runtime connector.
     *
     * @param pluginTypeText connector type text, currently supports {@code source} and {@code sink}
     * @param pluginName connector factory identifier
     * @return connector option rule metadata
     * @throws IllegalArgumentException if the request parameters are blank or unsupported
     * @throws NoSuchElementException if the target plugin cannot be found from runtime discovery
     */
    public OptionRuleResponse getOptionRules(String pluginTypeText, String pluginName) {
        PluginType pluginType = parseSupportedPluginType(pluginTypeText);
        String normalizedPluginName = normalizePluginName(pluginName);
        String displayPluginName = pluginName.trim();
        ConcurrentMap<String, OptionRuleResponse> pluginTypeCache = getPluginTypeCache(pluginType);
        return pluginTypeCache.computeIfAbsent(
                normalizedPluginName,
                key ->
                        buildOptionRuleResponse(
                                pluginType, normalizedPluginName, displayPluginName));
    }

    /**
     * Clears all cached discovery and response metadata.
     *
     * <p>This is primarily intended for future plugin reload scenarios. Current Zeta deployments
     * still expect service restart after plugin installation or upgrade.
     */
    public void clearCache() {
        discoveredPluginsCache.clear();
        responseCache.clear();
        log.info("Cleared option rules cache");
    }

    OptionRuleResponse buildResponse(PluginIdentifier pluginIdentifier, OptionRule optionRule) {
        return new OptionRuleResponse(
                pluginIdentifier.getEngineType(),
                pluginIdentifier.getPluginType(),
                pluginIdentifier.getPluginName(),
                toOptionRuleMetadata(optionRule));
    }

    private OptionRuleResponse.OptionRuleMetadata toOptionRuleMetadata(OptionRule optionRule) {
        List<OptionRuleResponse.OptionMetadata> optionalOptions =
                optionRule.getOptionalOptions().stream()
                        .map(this::toOptionMetadata)
                        .collect(Collectors.toList());
        List<OptionRuleResponse.RequiredOptionMetadata> requiredOptions =
                optionRule.getRequiredOptions().stream()
                        .map(this::toRequiredOptionMetadata)
                        .collect(Collectors.toList());
        List<OptionRuleResponse.ConditionRuleMetadata> conditionRules =
                optionRule.getConditionRules().stream()
                        .map(this::toConditionRuleMetadata)
                        .collect(Collectors.toList());
        List<OptionRuleResponse.ValueConstraintMetadata> valueConstraints =
                optionRule.getValueConstraints().stream()
                        .map(this::toValueConstraintMetadata)
                        .collect(Collectors.toList());
        return new OptionRuleResponse.OptionRuleMetadata(
                optionalOptions, requiredOptions, conditionRules, valueConstraints);
    }

    private ConcurrentMap<String, OptionRuleResponse> getPluginTypeCache(PluginType pluginType) {
        return responseCache.computeIfAbsent(pluginType, key -> new ConcurrentHashMap<>());
    }

    private OptionRuleResponse buildOptionRuleResponse(
            PluginType pluginType, String normalizedPluginName, String displayPluginName) {
        Map.Entry<PluginIdentifier, OptionRule> pluginEntry =
                findPlugin(pluginType, normalizedPluginName, displayPluginName);
        return buildResponse(pluginEntry.getKey(), pluginEntry.getValue());
    }

    private Map.Entry<PluginIdentifier, OptionRule> findPlugin(
            PluginType pluginType, String normalizedPluginName, String displayPluginName) {
        return getDiscoveredPlugins(pluginType).entrySet().stream()
                .filter(
                        pluginEntry ->
                                pluginEntry
                                        .getKey()
                                        .getPluginName()
                                        .equalsIgnoreCase(normalizedPluginName))
                .findFirst()
                .orElseThrow(
                        () ->
                                new NoSuchElementException(
                                        String.format(
                                                "Plugin '%s' not found for type '%s'.",
                                                displayPluginName, pluginType.getType())));
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
        if (StringUtils.equalsIgnoreCase(normalizedPluginType, PluginType.TRANSFORM.getType())) {
            return PluginType.TRANSFORM;
        }
        throw new IllegalArgumentException(
                String.format(
                        "Unsupported plugin type '%s'. Only '%s', '%s' and '%s' are supported.",
                        normalizedPluginType,
                        PluginType.SOURCE.getType(),
                        PluginType.SINK.getType(),
                        PluginType.TRANSFORM.getType()));
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
        OptionRuleResponse.RuleType ruleType = resolveRuleType(requiredOption);
        if (ruleType == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported required option type: %s",
                            requiredOption.getClass().getName()));
        }
        if (requiredOption instanceof RequiredOption.ConditionalRequiredOptions) {
            Expression expression =
                    ((RequiredOption.ConditionalRequiredOptions) requiredOption).getExpression();
            return new OptionRuleResponse.RequiredOptionMetadata(
                    ruleType, options, expression.toString(), toExpressionNode(expression));
        }
        return new OptionRuleResponse.RequiredOptionMetadata(ruleType, options, null, null);
    }

    private OptionRuleResponse.ConditionRuleMetadata toConditionRuleMetadata(
            ConditionRule conditionRule) {
        Expression expression = conditionRule.getExpression();
        return new OptionRuleResponse.ConditionRuleMetadata(
                expression.toString(),
                toExpressionNode(expression),
                toOptionRuleMetadata(conditionRule.getOptionRule()));
    }

    private OptionRuleResponse.RuleType resolveRuleType(RequiredOption requiredOption) {
        return REQUIRED_OPTION_RULE_TYPES.entrySet().stream()
                .filter(entry -> entry.getKey().isInstance(requiredOption))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
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
        ConditionOperator op = condition.getOperator();
        String compareOperatorSymbol =
                (op != null && op != ConditionOperator.EQUAL) ? op.getSymbol() : null;
        OptionRuleResponse.OptionMetadata compareOptionMeta =
                condition.getCompareOption() != null
                        ? toOptionMetadata(condition.getCompareOption())
                        : null;
        String conditionOperator = (op != null) ? op.name() : null;
        String conditionOperatorCategory = (op != null) ? op.getCategory().name() : null;
        Object expectValue = condition.getExpectValue();
        if (op == ConditionOperator.EXTENSION && condition.getExtension() != null) {
            expectValue = condition.getExtension().description();
        }
        return new OptionRuleResponse.ConditionNode(
                toOptionMetadata(condition.getOption()),
                expectValue,
                compareOperatorSymbol,
                compareOptionMeta,
                conditionOperator,
                conditionOperatorCategory,
                toLogicalOperator(condition.and()),
                toConditionNode(condition.getNext()));
    }

    private OptionRuleResponse.ValueConstraintMetadata toValueConstraintMetadata(
            Condition<?> condition) {
        return new OptionRuleResponse.ValueConstraintMetadata(
                condition.toString(), toConditionNode(condition));
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

    private static Map<Class<? extends RequiredOption>, OptionRuleResponse.RuleType>
            createRequiredOptionRuleTypes() {
        Map<Class<? extends RequiredOption>, OptionRuleResponse.RuleType> ruleTypes =
                new HashMap<>();
        ruleTypes.put(
                RequiredOption.AbsolutelyRequiredOptions.class,
                OptionRuleResponse.RuleType.ABSOLUTELY_REQUIRED);
        ruleTypes.put(
                RequiredOption.ExclusiveRequiredOptions.class,
                OptionRuleResponse.RuleType.EXCLUSIVE);
        ruleTypes.put(
                RequiredOption.BundledRequiredOptions.class, OptionRuleResponse.RuleType.BUNDLED);
        ruleTypes.put(
                RequiredOption.ConditionalRequiredOptions.class,
                OptionRuleResponse.RuleType.CONDITIONAL);
        return Collections.unmodifiableMap(ruleTypes);
    }
}
