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

package org.apache.seatunnel.tools.x2seatunnel.template;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 基于 Typesafe Config (HOCON) 的模板分析器 用于解析 SeaTunnel 配置模板，自动推断字段路径，替换手动缩进解析 */
@Slf4j
public class HoconTemplateAnalyzer {

    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

    /**
     * 解析模板字符串，提取所有配置字段和对应的变量引用
     *
     * @param templateContent 模板内容
     * @param templateType 模板类型 (source/sink)
     * @return 字段路径到变量引用的映射
     */
    public Map<String, List<String>> extractFieldVariables(
            String templateContent, String templateType) {
        Map<String, List<String>> fieldVariables = new HashMap<>();

        try {
            // 使用 Typesafe Config 解析模板
            Config config =
                    ConfigFactory.parseString(
                            templateContent,
                            ConfigParseOptions.defaults()
                                    .setSyntax(ConfigSyntax.CONF)
                                    .setAllowMissing(true));

            // 递归遍历配置树，提取字段路径和变量
            extractVariablesFromConfig(config, templateType, "", fieldVariables);

        } catch (Exception e) {
            log.error("HOCON 模板解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板格式不符合HOCON语法标准: " + e.getMessage(), e);
        }

        return fieldVariables;
    }

    /** 递归遍历配置对象，提取字段路径和变量引用 */
    private void extractVariablesFromConfig(
            Config config,
            String templateType,
            String currentPath,
            Map<String, List<String>> fieldVariables) {
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();

            // 构建完整的字段路径
            String fieldPath = buildFieldPath(templateType, currentPath, key);

            if (value.valueType() == ConfigValueType.OBJECT) {
                // 如果是对象，递归处理
                Config subConfig = config.getConfig(key);
                extractVariablesFromConfig(subConfig, templateType, fieldPath, fieldVariables);
            } else if (value.valueType() == ConfigValueType.STRING) {
                // 如果是字符串，提取变量引用
                String stringValue = value.unwrapped().toString();
                List<String> variables = extractVariablesFromString(stringValue);
                if (!variables.isEmpty()) {
                    fieldVariables.put(fieldPath, variables);
                }
            } else if (value.valueType() == ConfigValueType.LIST) {
                // 处理列表中的字符串值
                @SuppressWarnings("unchecked")
                List<Object> listValue = (List<Object>) value.unwrapped();
                for (int i = 0; i < listValue.size(); i++) {
                    if (listValue.get(i) instanceof String) {
                        String stringValue = (String) listValue.get(i);
                        List<String> variables = extractVariablesFromString(stringValue);
                        if (!variables.isEmpty()) {
                            String listFieldPath = fieldPath + "[" + i + "]";
                            fieldVariables.put(listFieldPath, variables);
                        }
                    }
                }
            }
        }
    }

    /** 构建完整的字段路径 */
    private String buildFieldPath(String templateType, String currentPath, String key) {
        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(templateType);

        if (!currentPath.isEmpty()) {
            pathBuilder.append(".").append(currentPath);
        }
        pathBuilder.append(".").append(key);

        return pathBuilder.toString();
    }

    /** 从字符串中提取所有变量引用 */
    private List<String> extractVariablesFromString(String value) {
        List<String> variables = new ArrayList<>();
        Matcher matcher = VARIABLE_PATTERN.matcher(value);

        while (matcher.find()) {
            String variable = matcher.group(1);
            variables.add(variable);
        }

        return variables;
    }

    /** 验证模板语法是否有效 */
    public boolean validateTemplate(String templateContent) {
        try {
            ConfigFactory.parseString(
                    templateContent,
                    ConfigParseOptions.defaults()
                            .setSyntax(ConfigSyntax.CONF)
                            .setAllowMissing(true));
            return true;
        } catch (Exception e) {
            log.warn("Template validation failed: {}", e.getMessage());
            return false;
        }
    }

    /** 获取模板的根键名（如 Jdbc, Kafka 等） */
    public String extractRootKey(String templateContent) {
        try {
            Config config =
                    ConfigFactory.parseString(
                            templateContent,
                            ConfigParseOptions.defaults()
                                    .setSyntax(ConfigSyntax.CONF)
                                    .setAllowMissing(true));

            // 通常模板的根键就是第一个顶级键
            for (String key : config.root().keySet()) {
                return key;
            }
        } catch (Exception e) {
            log.warn("Failed to extract root key from template: {}", e.getMessage());
        }
        return "Unknown";
    }
}
