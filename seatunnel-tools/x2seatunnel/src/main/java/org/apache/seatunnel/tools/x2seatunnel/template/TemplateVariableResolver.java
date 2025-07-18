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

import org.apache.seatunnel.tools.x2seatunnel.model.DataXConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 模板变量解析器 - 支持基础变量、默认值、条件映射和转换器调用 */
public class TemplateVariableResolver {

    private static final Logger logger = LoggerFactory.getLogger(TemplateVariableResolver.class);

    // Jinja2 风格变量模式：{{ datax.path.to.value }}
    private static final Pattern JINJA2_VARIABLE_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\}\\}");

    // Jinja2 风格过滤器模式：{{ datax.path.to.value | filter }}
    private static final Pattern JINJA2_FILTER_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\|\\s*([^}]+)\\s*\\}\\}");

    private final ObjectMapper objectMapper;
    private final TemplateMappingManager templateMappingManager;

    public TemplateVariableResolver(TemplateMappingManager templateMappingManager) {
        this.objectMapper = new ObjectMapper();
        this.templateMappingManager = templateMappingManager;
    }

    public TemplateVariableResolver() {
        this.objectMapper = new ObjectMapper();
        this.templateMappingManager = null;
    }
    /**
     * 解析模板变量
     *
     * @param templateContent 模板内容
     * @param dataXConfig DataX配置
     * @return 解析后的内容
     */
    public String resolve(String templateContent, DataXConfig dataXConfig) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return templateContent;
        }

        logger.debug("开始解析模板变量");

        try {
            // 将DataXConfig转换为JsonNode以便路径查询
            JsonNode rootNode = objectMapper.valueToTree(dataXConfig);

            String result = templateContent;

            // 0. 处理 {% set var = expr %} 语法（仅支持简单表达式）
            Map<String, String> localVars = new HashMap<>();
            Pattern setPattern = Pattern.compile("\\{%\\s*set\\s+(\\w+)\\s*=\\s*(.*?)\\s*%\\}");
            Matcher setMatcher = setPattern.matcher(result);
            while (setMatcher.find()) {
                String varName = setMatcher.group(1);
                String expr = setMatcher.group(2);
                String exprTemplate = "{{ " + expr + " }}";
                String value =
                        resolveJinja2FilterVariables(
                                resolveJinja2Variables(exprTemplate, rootNode), rootNode);
                localVars.put(varName, value);
                logger.debug("设置局部变量: {} = {}", varName, value);
            }
            result = setMatcher.replaceAll("");

            // 简单的字符串替换处理局部变量
            for (Map.Entry<String, String> entry : localVars.entrySet()) {
                result = result.replace("{{ " + entry.getKey() + " }}", entry.getValue());
            }

            // 1. 处理 Jinja2 风格的过滤器变量
            result = resolveJinja2FilterVariables(result, rootNode);

            // 2. 处理 Jinja2 风格的基础变量
            result = resolveJinja2Variables(result, rootNode);

            logger.debug("模板变量解析完成");
            return result;

        } catch (Exception e) {
            logger.error("模板变量解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板变量解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 解析模板变量（使用原始JSON字符串）
     *
     * @param templateContent 模板内容
     * @param dataXJsonContent DataX JSON配置内容
     * @return 解析后的内容
     */
    public String resolve(String templateContent, String dataXJsonContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return templateContent;
        }

        logger.debug("开始解析模板变量");

        try {
            // 直接解析JSON字符串为JsonNode
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);

            String result = templateContent;

            // 1. 处理 Jinja2 风格的过滤器变量
            result = resolveJinja2FilterVariables(result, rootNode);

            // 2. 处理 Jinja2 风格的基础变量
            result = resolveJinja2Variables(result, rootNode);

            logger.debug("模板变量解析完成");
            return result;

        } catch (Exception e) {
            logger.error("模板变量解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板变量解析失败: " + e.getMessage(), e);
        }
    }

    /** 解析 Jinja2 风格的基础变量：{{ datax.path.to.value }} */
    private String resolveJinja2Variables(String content, JsonNode rootNode) {
        Matcher matcher = JINJA2_VARIABLE_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String value = extractValueFromJinja2Path(rootNode, path);
            String resolvedValue = (value != null) ? value : "";

            matcher.appendReplacement(sb, Matcher.quoteReplacement(resolvedValue));
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /** 解析 Jinja2 风格的过滤器变量：{{ datax.path.to.value | filter }} */
    private String resolveJinja2FilterVariables(String content, JsonNode rootNode) {
        Matcher matcher = JINJA2_FILTER_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String filterExpression = matcher.group(2).trim();

            String value = extractValueFromJinja2Path(rootNode, path);

            // 处理过滤器链：filter1 | filter2 | filter3
            String[] filters = parseFilterChain(filterExpression);
            Object resolvedValue = value;

            for (String filter : filters) {
                // 添加空值检查，防止空指针异常
                if (resolvedValue == null) {
                    resolvedValue = "";
                }

                // 统一应用过滤器
                resolvedValue = applyFilter(resolvedValue, filter.trim());
            }

            String finalValue =
                    resolvedValue instanceof String
                            ? (String) resolvedValue
                            : (resolvedValue != null ? resolvedValue.toString() : "");
            matcher.appendReplacement(sb, Matcher.quoteReplacement(finalValue));
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /** 智能解析过滤器链，正确处理括号内的管道符 */
    private String[] parseFilterChain(String filterExpression) {
        List<String> filters = new ArrayList<>();
        StringBuilder currentFilter = new StringBuilder();
        int depth = 0;
        boolean inQuotes = false;
        char quoteChar = '\0';

        for (int i = 0; i < filterExpression.length(); i++) {
            char c = filterExpression.charAt(i);

            if (!inQuotes && (c == '\'' || c == '"')) {
                inQuotes = true;
                quoteChar = c;
                currentFilter.append(c);
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
                quoteChar = '\0';
                currentFilter.append(c);
            } else if (!inQuotes && c == '(') {
                depth++;
                currentFilter.append(c);
            } else if (!inQuotes && c == ')') {
                depth--;
                currentFilter.append(c);
            } else if (!inQuotes && c == '|' && depth == 0) {
                filters.add(currentFilter.toString().trim());
                currentFilter.setLength(0);
            } else {
                currentFilter.append(c);
            }
        }

        if (currentFilter.length() > 0) {
            filters.add(currentFilter.toString().trim());
        }

        return filters.toArray(new String[0]);
    }

    /** 从 Jinja2 风格的路径提取值：datax.job.content[0].reader.parameter.column */
    private String extractValueFromJinja2Path(JsonNode rootNode, String path) {
        try {
            JsonNode currentNode = rootNode;

            // 将 datax.job.content[0] 转换为 job.content[0] (移除 datax 前缀)
            if (path.startsWith("datax.")) {
                path = path.substring(6);
            }

            String[] pathParts = path.split("\\.");

            for (String part : pathParts) {
                if (currentNode == null) {
                    return null;
                }

                // 处理数组索引，如 content[0]
                if (part.contains("[") && part.contains("]")) {
                    String arrayName = part.substring(0, part.indexOf("["));
                    String indexStr = part.substring(part.indexOf("[") + 1, part.indexOf("]"));

                    currentNode = currentNode.get(arrayName);
                    if (currentNode != null && currentNode.isArray()) {
                        try {
                            int index = Integer.parseInt(indexStr);
                            currentNode = currentNode.get(index);
                        } catch (NumberFormatException e) {
                            logger.warn("无效的数组索引: {}", indexStr);
                            return null;
                        }
                    }
                } else {
                    currentNode = currentNode.get(part);
                }
            }

            if (currentNode != null && !currentNode.isNull()) {
                if (currentNode.isArray()) {
                    // 如果是数组，返回数组的所有元素
                    StringBuilder result = new StringBuilder();
                    for (int i = 0; i < currentNode.size(); i++) {
                        if (i > 0) result.append(",");
                        result.append(currentNode.get(i).asText());
                    }
                    return result.toString();
                } else {
                    return currentNode.asText();
                }
            }

        } catch (Exception e) {
            logger.warn("提取 Jinja2 路径值失败: {}", path, e);
        }

        return null;
    }

    /** 找到匹配的右括号位置，处理嵌套括号 */
    private int findMatchingCloseParen(String text, int openParenPos) {
        int depth = 1;
        for (int i = openParenPos + 1; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1; // 没有找到匹配的右括号
    }

    /** 统一的过滤器应用方法 - 支持字符串和数组 */
    private Object applyFilter(Object value, String filterExpression) {
        if (value == null) {
            value = "";
        }

        // 解析过滤器：join(',') 或 join(', ') 或 default('SELECT * FROM table')
        String filterName;
        String filterArgs = "";

        if (filterExpression.contains("(") && filterExpression.contains(")")) {
            filterName = filterExpression.substring(0, filterExpression.indexOf("(")).trim();

            // 找到正确的右括号位置（处理嵌套括号）
            int openParenPos = filterExpression.indexOf("(");
            int closeParenPos = findMatchingCloseParen(filterExpression, openParenPos);

            if (closeParenPos != -1) {
                filterArgs = filterExpression.substring(openParenPos + 1, closeParenPos).trim();
                // 移除引号
                if (filterArgs.startsWith("'") && filterArgs.endsWith("'")) {
                    filterArgs = filterArgs.substring(1, filterArgs.length() - 1);
                } else if (filterArgs.startsWith("\"") && filterArgs.endsWith("\"")) {
                    filterArgs = filterArgs.substring(1, filterArgs.length() - 1);
                }
            } else {
                logger.warn("无法找到匹配的右括号: {}", filterExpression);
            }
        } else {
            filterName = filterExpression.trim();
        }

        // 应用过滤器
        switch (filterName) {
            case "join":
                if (value instanceof String[]) {
                    return applyJoinFilterOnArray(
                            (String[]) value, filterArgs.isEmpty() ? "," : filterArgs);
                } else {
                    return applyJoinFilter(
                            value.toString(), filterArgs.isEmpty() ? "," : filterArgs);
                }
            case "default":
                String stringValue = value.toString();
                return stringValue.isEmpty() ? filterArgs : stringValue;
            case "upper":
                return value.toString().toUpperCase();
            case "lower":
                return value.toString().toLowerCase();
            case "regex_extract":
                return applyRegexExtract(value.toString(), filterArgs);
            case "jdbc_driver_mapper":
                return applyTransformer(value.toString(), "jdbc_driver_mapper");
            case "split":
                return applySplit(value.toString(), filterArgs);
            case "get":
                return applyGet(value, filterArgs);
            case "replace":
                return applyReplace(value.toString(), filterArgs);
            default:
                // 检查是否是转换器调用
                if (templateMappingManager != null
                        && templateMappingManager.getTransformer(filterName) != null) {
                    return applyTransformer(value.toString(), filterName);
                }
                logger.warn("不支持的过滤器: {}", filterName);
                return value;
        }
    }

    /** 应用转换器 */
    private String applyTransformer(String value, String transformerName) {
        if (templateMappingManager == null) {
            logger.warn("TemplateMappingManager未初始化，无法使用转换器: {}", transformerName);
            return value;
        }

        try {
            Map<String, String> transformer =
                    templateMappingManager.getTransformer(transformerName);
            if (transformer == null) {
                logger.warn("转换器不存在: {}", transformerName);
                return value;
            }

            logger.info("应用转换器 {} 处理值: {}", transformerName, value);
            logger.info("转换器映射表: {}", transformer);

            // 查找匹配的转换器规则
            for (Map.Entry<String, String> entry : transformer.entrySet()) {
                String pattern = entry.getKey();
                String mappedValue = entry.getValue();

                // 支持包含匹配
                if (value.toLowerCase().contains(pattern.toLowerCase())) {
                    logger.info("转换器 {} 匹配成功: {} -> {}", transformerName, value, mappedValue);
                    return mappedValue;
                }
            }

            logger.warn("转换器 {} 未找到匹配项，返回原值: {}", transformerName, value);
            return value;

        } catch (Exception e) {
            logger.error("应用转换器失败: {}", transformerName, e);
            return value;
        }
    }

    /** 应用 join 过滤器 */
    private String applyJoinFilter(String value, String separator) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }

        // 如果值本身就是逗号分隔的字符串，直接用指定分隔符连接
        if (value.contains(",")) {
            String[] parts = value.split(",");
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) result.append(separator);
                result.append(parts[i].trim());
            }
            return result.toString();
        }

        return value;
    }

    /** 应用正则表达式提取过滤器 */
    private String applyRegexExtract(String value, String regexPattern) {
        if (value == null
                || value.trim().isEmpty()
                || regexPattern == null
                || regexPattern.trim().isEmpty()) {
            return value;
        }

        try {
            logger.info("正则表达式提取: 输入值='{}', 参数='{}'", value, regexPattern);

            // 支持两种格式：
            // 1. 简单模式：regex_extract('pattern') - 提取第一个匹配组
            // 2. 替换模式：regex_extract('pattern', 'replacement') - 使用替换模式

            // 解析参数，考虑引号内的逗号不应该被分割
            String[] parts = parseRegexArgs(regexPattern);
            String pattern = parts[0].trim();
            String replacement = parts.length > 1 ? parts[1].trim() : "$1";

            logger.info("正则表达式提取: 模式='{}', 替换='{}', 输入值='{}'", pattern, replacement, value);

            java.util.regex.Pattern compiledPattern = java.util.regex.Pattern.compile(pattern);
            java.util.regex.Matcher matcher = compiledPattern.matcher(value);

            if (matcher.find()) {
                // 如果 replacement 只包含组引用，则拼接返回对应组
                if (replacement.matches("(\\$\\d+)(\\.\\$\\d+)*")) {
                    String extracted = replacement;
                    // 替换组引用
                    for (int i = 1; i <= matcher.groupCount(); i++) {
                        extracted = extracted.replace("$" + i, matcher.group(i));
                    }
                    logger.info("正则表达式提取成功: 结果='{}'", extracted);
                    return extracted;
                } else {
                    String replaced = matcher.replaceFirst(replacement);
                    logger.info("正则表达式替换成功: 结果='{}'", replaced);
                    return replaced;
                }
            } else {
                logger.warn("正则表达式提取失败: 模式'{}' 不匹配输入值'{}'", pattern, value);
                return value;
            }

        } catch (Exception e) {
            logger.error("正则表达式提取出错: pattern='{}', value='{}'", regexPattern, value, e);
            return value;
        }
    }

    /** 解析 regex_extract 的参数，正确处理引号内的逗号 */
    private String[] parseRegexArgs(String args) {
        if (args == null || args.trim().isEmpty()) {
            return new String[0];
        }

        List<String> result = new ArrayList<>();
        StringBuilder currentArg = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = '\0';

        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);

            if (!inQuotes && (c == '\'' || c == '"')) {
                inQuotes = true;
                quoteChar = c;
            } else if (inQuotes && c == quoteChar) {
                inQuotes = false;
                quoteChar = '\0';
            } else if (!inQuotes && c == ',') {
                result.add(currentArg.toString().trim());
                currentArg.setLength(0);
                continue;
            }

            currentArg.append(c);
        }

        if (currentArg.length() > 0) {
            result.add(currentArg.toString().trim());
        }

        // 移除每个参数的引号
        for (int i = 0; i < result.size(); i++) {
            String arg = result.get(i);
            if ((arg.startsWith("'") && arg.endsWith("'"))
                    || (arg.startsWith("\"") && arg.endsWith("\""))) {
                result.set(i, arg.substring(1, arg.length() - 1));
            }
        }

        return result.toArray(new String[0]);
    }

    /**
     * 应用 split 过滤器 - 字符串分割
     *
     * @param value 输入字符串
     * @param delimiter 分隔符，默认为 "/"
     * @return 分割后的字符串数组
     */
    private String[] applySplit(String value, String delimiter) {
        if (value == null || value.trim().isEmpty()) {
            return new String[0];
        }

        // 如果没有指定分隔符，使用默认的 "/"
        String actualDelimiter =
                (delimiter != null && !delimiter.trim().isEmpty()) ? delimiter.trim() : "/";

        logger.info("字符串分割: 输入值='{}', 分隔符='{}'", value, actualDelimiter);

        String[] result = value.split(actualDelimiter);
        logger.info("分割结果: {}", java.util.Arrays.toString(result));

        return result;
    }

    /**
     * 应用 get 过滤器 - 获取数组指定位置的元素
     *
     * @param value 输入值（可能是字符串数组）
     * @param indexStr 索引字符串，支持负数索引
     * @return 指定位置的元素
     */
    private String applyGet(Object value, String indexStr) {
        if (value == null) {
            return "";
        }

        // 如果不是字符串数组，直接返回字符串形式
        if (!(value instanceof String[])) {
            return value.toString();
        }

        String[] array = (String[]) value;
        if (array.length == 0) {
            return "";
        }

        try {
            int index = Integer.parseInt(indexStr.trim());

            // 支持负数索引
            if (index < 0) {
                index = array.length + index;
            }

            if (index >= 0 && index < array.length) {
                String result = array[index];
                logger.info("数组获取: 索引={}, 结果='{}'", indexStr, result);
                return result;
            } else {
                logger.warn("数组索引超出范围: 索引={}, 数组长度={}", indexStr, array.length);
                return "";
            }
        } catch (NumberFormatException e) {
            logger.error("无效的数组索引: {}", indexStr, e);
            return "";
        }
    }

    /**
     * 应用 replace 过滤器 - 字符串替换
     *
     * @param value 输入字符串
     * @param args 替换参数，格式为 "old,new"
     * @return 替换后的字符串
     */
    private String applyReplace(String value, String args) {
        if (value == null) {
            return "";
        }

        if (args == null || args.trim().isEmpty()) {
            return value;
        }

        // 解析替换参数，格式为 "old,new"
        String[] parts = args.split(",", 2);
        if (parts.length == 2) {
            String oldStr = parts[0].trim();
            String newStr = parts[1].trim();

            logger.info("字符串替换: 输入值='{}', 替换 '{}' -> '{}'", value, oldStr, newStr);

            String result = value.replace(oldStr, newStr);
            logger.info("替换结果: '{}'", result);
            return result;
        } else {
            logger.warn("replace 过滤器参数格式错误，应为 'old,new'，实际为: {}", args);
            return value;
        }
    }

    /** 应用 join 过滤器到数组 */
    private String applyJoinFilterOnArray(String[] value, String separator) {
        if (value == null || value.length == 0) {
            return "";
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length; i++) {
            if (i > 0) {
                result.append(separator);
            }
            result.append(value[i] != null ? value[i].trim() : "");
        }
        return result.toString();
    }
}
