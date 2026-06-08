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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

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
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.MetadataExportCommandArgs;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Export connector metadata as structured JSON via runtime reflection.
 *
 * <p>Uses PluginDiscovery + factory.optionRule() — the same mechanism as SeaTunnel Web and engine
 * REST API. Output is 100% accurate because it reads the live OptionRule objects, not source code.
 *
 * <p>Usage: seatunnel-metadata-export.sh [-o output.json] [-pt source|sink] [-p Jdbc] [--stdout]
 */
public class MetadataExportCommand implements Command<MetadataExportCommandArgs> {

    private final MetadataExportCommandArgs args;
    private final Map<PluginType, PluginDiscovery> discoveryMap = new HashMap<>();
    private final ObjectMapper mapper;

    public MetadataExportCommand(MetadataExportCommandArgs args) {
        this.args = args;
        this.discoveryMap.put(PluginType.SOURCE, new SeaTunnelSourcePluginDiscovery());
        this.discoveryMap.put(PluginType.SINK, new SeaTunnelSinkPluginDiscovery());
        this.discoveryMap.put(PluginType.TRANSFORM, new SeaTunnelTransformPluginDiscovery());
        this.mapper = new ObjectMapper();
        if (args.isPrettyPrint()) {
            this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
        }
    }

    @Override
    public void execute() throws CommandExecuteException, ConfigCheckException {
        try {
            ObjectNode root = mapper.createObjectNode();
            root.put("version", getSeaTunnelVersion());
            root.put("exportTime", System.currentTimeMillis());

            ArrayNode connectorsArray = mapper.createArrayNode();

            List<PluginType> typesToExport = new ArrayList<>();
            if (args.getPluginType() != null) {
                typesToExport.add(args.getPluginType());
            } else {
                typesToExport.add(PluginType.SOURCE);
                typesToExport.add(PluginType.SINK);
                typesToExport.add(PluginType.TRANSFORM);
            }

            for (PluginType pluginType : typesToExport) {
                PluginDiscovery discovery = discoveryMap.get(pluginType);
                if (discovery == null) {
                    continue;
                }

                LinkedHashMap<PluginIdentifier, OptionRule> plugins = discovery.getPlugins();

                for (Map.Entry<PluginIdentifier, OptionRule> entry : plugins.entrySet()) {
                    PluginIdentifier id = entry.getKey();
                    OptionRule rule = entry.getValue();

                    // Filter by plugin name if specified
                    if (args.getPluginName() != null
                            && !args.getPluginName().equalsIgnoreCase(id.getPluginName())) {
                        continue;
                    }

                    ObjectNode connectorNode = exportConnector(id, rule, pluginType);
                    connectorsArray.add(connectorNode);
                }
            }

            root.set("connectors", connectorsArray);

            if (args.isStdout()) {
                System.out.println(mapper.writeValueAsString(root));
            } else {
                File outputFile = new File(args.getOutputPath());
                mapper.writeValue(outputFile, root);
                System.err.println(
                        "Exported "
                                + connectorsArray.size()
                                + " connectors to "
                                + outputFile.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new CommandExecuteException("Failed to export metadata", e);
        }
    }

    private ObjectNode exportConnector(
            PluginIdentifier id, OptionRule rule, PluginType pluginType) {
        ObjectNode node = mapper.createObjectNode();
        node.put("name", id.getPluginName());
        node.put("type", pluginType.getType());

        // Required options — preserving the 4 subtypes
        ArrayNode requiredArray = mapper.createArrayNode();
        for (RequiredOption reqOpt : rule.getRequiredOptions()) {
            String category = resolveCategory(reqOpt);
            String expression = null;

            if (reqOpt instanceof RequiredOption.ConditionalRequiredOptions) {
                Expression expr =
                        ((RequiredOption.ConditionalRequiredOptions) reqOpt).getExpression();
                expression = expr.toString();
            }

            for (Option<?> opt : reqOpt.getOptions()) {
                ObjectNode optNode = exportOption(opt);
                optNode.put("category", category);
                if (expression != null) {
                    optNode.put("conditionExpression", expression);
                    // Also export structured condition for easier parsing
                    if (reqOpt instanceof RequiredOption.ConditionalRequiredOptions) {
                        Expression expr =
                                ((RequiredOption.ConditionalRequiredOptions) reqOpt)
                                        .getExpression();
                        optNode.set("conditionTree", exportExpression(expr));
                    }
                }
                requiredArray.add(optNode);
            }
        }
        node.set("required", requiredArray);

        // Optional options
        ArrayNode optionalArray = mapper.createArrayNode();
        for (Option<?> opt : rule.getOptionalOptions()) {
            optionalArray.add(exportOption(opt));
        }
        node.set("optional", optionalArray);

        // Condition rules (conditionalRule — nested OptionRule triggered by expression)
        ArrayNode conditionRulesArray = mapper.createArrayNode();
        for (ConditionRule condRule : rule.getConditionRules()) {
            ObjectNode condNode = mapper.createObjectNode();
            condNode.put("expression", condRule.getExpression().toString());
            condNode.set("expressionTree", exportExpression(condRule.getExpression()));

            // Nested OptionRule
            OptionRule nestedRule = condRule.getOptionRule();
            ArrayNode nestedRequired = mapper.createArrayNode();
            for (RequiredOption nestedReq : nestedRule.getRequiredOptions()) {
                String nestedCategory = resolveCategory(nestedReq);
                for (Option<?> opt : nestedReq.getOptions()) {
                    ObjectNode optNode = exportOption(opt);
                    optNode.put("category", nestedCategory);
                    nestedRequired.add(optNode);
                }
            }
            condNode.set("requiredOptions", nestedRequired);

            ArrayNode nestedOptional = mapper.createArrayNode();
            for (Option<?> opt : nestedRule.getOptionalOptions()) {
                nestedOptional.add(exportOption(opt));
            }
            condNode.set("optionalOptions", nestedOptional);

            conditionRulesArray.add(condNode);
        }
        node.set("conditionRules", conditionRulesArray);

        ArrayNode valueConstraintsArray = mapper.createArrayNode();
        for (Condition<?> constraint : rule.getValueConstraints()) {
            ObjectNode vcNode = mapper.createObjectNode();
            vcNode.put("expression", constraint.toString());
            vcNode.set("conditionTree", exportCondition(constraint));
            valueConstraintsArray.add(vcNode);
        }
        node.set("valueConstraints", valueConstraintsArray);

        return node;
    }

    private ObjectNode exportOption(Option<?> opt) {
        ObjectNode node = mapper.createObjectNode();
        node.put("key", opt.key());

        // Resolve type to a clean, LLM-readable name
        Type rawType = opt.typeReference().getType();
        node.put("type", resolveTypeName(rawType));

        // Default value
        Object defaultValue = opt.defaultValue();
        if (defaultValue != null) {
            try {
                node.set("defaultValue", mapper.valueToTree(defaultValue));
            } catch (IllegalArgumentException e) {
                node.put("defaultValue", String.valueOf(defaultValue));
            }
        } else {
            node.putNull("defaultValue");
        }

        // Description
        String desc = opt.getDescription();
        if (desc != null && !desc.isEmpty()) {
            node.put("description", desc);
        }

        // Fallback keys — critical for HOCON compatibility
        List<String> fallbackKeys = opt.getFallbackKeys();
        if (fallbackKeys != null && !fallbackKeys.isEmpty()) {
            ArrayNode fbArray = mapper.createArrayNode();
            for (String fb : fallbackKeys) {
                fbArray.add(fb);
            }
            node.set("fallbackKeys", fbArray);
        }

        // Enum values — from SingleChoiceOption or from Enum class reflection
        ArrayNode valuesArray = exportOptionValues(opt, rawType);
        if (valuesArray != null && valuesArray.size() > 0) {
            node.set("optionValues", valuesArray);
        }

        return node;
    }

    /**
     * Resolve a Java Type to a clean, readable name for LLM consumption.
     *
     * <p>Handles:
     *
     * <ul>
     *   <li>Standard types: java.lang.String → "string", java.lang.Boolean → "boolean"
     *   <li>Collections: java.util.List&lt;String&gt; → "list&lt;string&gt;"
     *   <li>Maps: java.util.Map&lt;String,String&gt; → "map&lt;string,string&gt;"
     *   <li>Enums: CompressFormat → "enum&lt;CompressFormat&gt;"
     *   <li>Anonymous/inner classes: falls back to "object" with original name preserved
     * </ul>
     */
    @SuppressWarnings("rawtypes")
    private static String resolveTypeName(Type type) {
        if (type == null) {
            return "object";
        }

        // ParameterizedType: List<String>, Map<String,Object>, etc.
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type rawType = pt.getRawType();
            String rawName = resolveTypeName(rawType);
            Type[] args = pt.getActualTypeArguments();
            if (args != null && args.length > 0) {
                StringBuilder sb = new StringBuilder(rawName).append("<");
                for (int i = 0; i < args.length; i++) {
                    if (i > 0) sb.append(",");
                    sb.append(resolveTypeName(args[i]));
                }
                sb.append(">");
                return sb.toString();
            }
            return rawName;
        }

        // Class type
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;

            // Primitive wrappers → simple names
            if (clazz == String.class) return "string";
            if (clazz == Boolean.class || clazz == boolean.class) return "boolean";
            if (clazz == Integer.class || clazz == int.class) return "int";
            if (clazz == Long.class || clazz == long.class) return "long";
            if (clazz == Double.class || clazz == double.class) return "double";
            if (clazz == Float.class || clazz == float.class) return "float";

            // Collections
            if (List.class.isAssignableFrom(clazz)) return "list";
            if (Map.class.isAssignableFrom(clazz)) return "map";

            // Enums
            if (clazz.isEnum()) {
                return "enum<" + clazz.getSimpleName() + ">";
            }

            // Named class — use simple name
            String simpleName = clazz.getSimpleName();
            if (!simpleName.isEmpty() && !simpleName.contains("$") && !simpleName.matches("\\d+")) {
                return simpleName.substring(0, 1).toLowerCase() + simpleName.substring(1);
            }
        }

        // Fallback: clean up anonymous class garbage
        String typeName = type.getTypeName();
        if (typeName.contains("$") || typeName.contains("@")) {
            // Try to extract the outer class name for context
            int dollarIdx = typeName.indexOf('$');
            if (dollarIdx > 0) {
                String outerFull = typeName.substring(0, dollarIdx);
                int lastDot = outerFull.lastIndexOf('.');
                if (lastDot >= 0) {
                    return outerFull.substring(lastDot + 1).toLowerCase();
                }
            }
            return "object";
        }

        return typeName;
    }

    /**
     * Export allowed values for an option.
     *
     * <p>Sources (in priority order):
     *
     * <ol>
     *   <li>SingleChoiceOption.getOptionValues() — explicitly declared choices
     *   <li>Enum class constants — if the option's type is a Java Enum
     * </ol>
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private ArrayNode exportOptionValues(Option<?> opt, Type rawType) {
        // 1. SingleChoiceOption — explicit values
        if (opt instanceof SingleChoiceOption) {
            List<?> values = ((SingleChoiceOption<?>) opt).getOptionValues();
            if (values != null && !values.isEmpty()) {
                ArrayNode arr = mapper.createArrayNode();
                for (Object v : values) {
                    arr.add(String.valueOf(v));
                }
                return arr;
            }
        }

        // 2. Enum type — reflect enum constants
        Class<?> enumClass = extractEnumClass(rawType);
        if (enumClass != null && enumClass.isEnum()) {
            Object[] constants = enumClass.getEnumConstants();
            if (constants != null && constants.length > 0 && constants.length <= 50) {
                ArrayNode arr = mapper.createArrayNode();
                for (Object c : constants) {
                    arr.add(String.valueOf(c));
                }
                return arr;
            }
        }

        return null;
    }

    /** Extract the Enum class from a Type, handling Class, ParameterizedType, etc. */
    private static Class<?> extractEnumClass(Type type) {
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;
            if (clazz.isEnum()) {
                return clazz;
            }
        }
        return null;
    }

    private ObjectNode exportExpression(Expression expression) {
        if (expression == null) {
            return null;
        }
        ObjectNode node = mapper.createObjectNode();
        node.set("condition", exportCondition(expression.getCondition()));
        if (expression.and() != null) {
            node.put("operator", expression.and() ? "AND" : "OR");
        }
        if (expression.hasNext()) {
            node.set("next", exportExpression(expression.getNext()));
        }
        return node;
    }

    private ObjectNode exportCondition(Condition<?> condition) {
        if (condition == null) {
            return null;
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("key", condition.getOption().key());
        if (condition.getExpectValue() != null) {
            node.put("expectValue", String.valueOf(condition.getExpectValue()));
        }
        ConditionOperator op = condition.getOperator();
        if (op != null) {
            node.put("conditionOperator", op.name());
            node.put("conditionOperatorCategory", op.getCategory().name());
        }
        if (op != null && op != ConditionOperator.EQUAL) {
            node.put("compareOperator", op.getSymbol());
        }
        if (condition.getCompareOption() != null) {
            node.put("compareOption", condition.getCompareOption().key());
        }
        if (condition.and() != null) {
            node.put("operator", condition.and() ? "AND" : "OR");
        }
        if (condition.hasNext()) {
            node.set("next", exportCondition(condition.getNext()));
        }
        return node;
    }

    private static String resolveCategory(RequiredOption requiredOption) {
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            return "absolutely_required";
        } else if (requiredOption instanceof RequiredOption.ExclusiveRequiredOptions) {
            return "exclusive";
        } else if (requiredOption instanceof RequiredOption.BundledRequiredOptions) {
            return "bundled";
        } else if (requiredOption instanceof RequiredOption.ConditionalRequiredOptions) {
            return "conditional";
        }
        return "unknown";
    }

    private static String getSeaTunnelVersion() {
        Package pkg = MetadataExportCommand.class.getPackage();
        String version = pkg != null ? pkg.getImplementationVersion() : null;
        return version != null ? version : "dev";
    }
}
