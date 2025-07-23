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
import org.apache.seatunnel.tools.x2seatunnel.model.MappingTracker;
import org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 模板变量解析器 - 支持基础变量、默认值、条件映射和转换器调用 */
public class TemplateVariableResolver {

    private static final Logger logger = LoggerFactory.getLogger(TemplateVariableResolver.class);

    // 标志：遇到 default 过滤器时抑制缺失字段记录
    private boolean suppressMissing = false;

    // Jinja2 变量模式：{{ datax.path.to.value }}
    private static final Pattern JINJA2_VARIABLE_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\}\\}");

    // Jinja2 过滤器模式：{{ datax.path.to.value | filter }}
    private static final Pattern JINJA2_FILTER_PATTERN =
            Pattern.compile("\\{\\{\\s*([^}|]+)\\s*\\|\\s*([^}]+)\\s*\\}\\}");

    private final ObjectMapper objectMapper;
    private final TemplateMappingManager templateMappingManager;
    private final MappingTracker mappingTracker;

    // 当前解析上下文：记录正在解析的目标字段路径
    private String currentTargetContext = null;

    // 标志：当前是否在处理复杂转换（包含过滤器的复合表达式）
    private boolean processingComplexTransform = false;

    // 字段引用跟踪器
    private DataXFieldExtractor.FieldReferenceTracker fieldReferenceTracker;

    public TemplateVariableResolver(
            TemplateMappingManager templateMappingManager, MappingTracker mappingTracker) {
        this.objectMapper = new ObjectMapper();
        this.templateMappingManager = templateMappingManager;
        this.mappingTracker = mappingTracker;
    }

    public TemplateVariableResolver(TemplateMappingManager templateMappingManager) {
        this.objectMapper = new ObjectMapper();
        this.templateMappingManager = templateMappingManager;
        this.mappingTracker = null; // 旧版本兼容，无映射跟踪
    }

    public TemplateVariableResolver() {
        this.objectMapper = new ObjectMapper();
        this.templateMappingManager = null;
        this.mappingTracker = null;
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

            // 1. 使用智能上下文解析处理所有变量
            result = resolveWithSmartContext(result, rootNode);

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

            // 使用智能上下文解析处理所有变量
            result = resolveWithSmartContext(result, rootNode);

            logger.debug("模板变量解析完成");
            return result;

        } catch (Exception e) {
            logger.error("模板变量解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板变量解析失败: " + e.getMessage(), e);
        }
    }

    /** 解析 Jinja2 风格的基础变量：{{ datax.path.to.value }} */
    private String resolveJinja2Variables(String content, JsonNode rootNode) {
        logger.debug(
                "开始解析Jinja2变量，内容长度: {}, fieldReferenceTracker: {}",
                content.length(),
                fieldReferenceTracker != null ? "已设置" : "未设置");

        Matcher matcher = JINJA2_VARIABLE_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String value = extractValueFromJinja2Path(rootNode, path);
            String resolvedValue = (value != null) ? value : "";

            logger.debug("找到变量: {}, 解析值: {}", path, resolvedValue);

            // 增加字段引用计数
            if (fieldReferenceTracker != null && path.startsWith("datax.")) {
                // 修复路径重复问题：datax.job.xxx -> job.xxx
                String normalizedPath =
                        path.startsWith("datax.job.")
                                ? path.substring(6)
                                : path.replace("datax.", "job.");
                logger.debug("解析变量时增加引用计数: {} -> {}", path, normalizedPath);
                incrementFieldReference(normalizedPath);
            } else {
                logger.debug(
                        "跳过引用计数: fieldReferenceTracker={}, path={}",
                        fieldReferenceTracker != null ? "已设置" : "未设置",
                        path);
            }

            matcher.appendReplacement(sb, Matcher.quoteReplacement(resolvedValue));
        }
        matcher.appendTail(sb);

        logger.debug("Jinja2变量解析完成");
        return sb.toString();
    }

    /** 解析 Jinja2 风格的过滤器变量：{{ datax.path.to.value | filter }} */
    private String resolveJinja2FilterVariables(String content, JsonNode rootNode) {
        logger.debug("开始解析过滤器变量，内容: {}", content.trim());
        Matcher matcher = JINJA2_FILTER_PATTERN.matcher(content);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String path = matcher.group(1).trim();
            String filterExpression = matcher.group(2).trim();

            logger.debug("找到过滤器变量: {}, 过滤器: {}", path, filterExpression);

            // 增加字段引用计数
            if (fieldReferenceTracker != null && path.startsWith("datax.")) {
                // 修复路径重复问题：datax.job.xxx -> job.xxx
                String normalizedPath =
                        path.startsWith("datax.job.")
                                ? path.substring(6)
                                : path.replace("datax.", "job.");
                logger.debug("过滤器变量增加引用计数: {} -> {}", path, normalizedPath);
                incrementFieldReference(normalizedPath);
            }

            // 解析过滤器链：filter1 | filter2 | filter3
            String[] filters = parseFilterChain(filterExpression);
            // 如果首个过滤器为 default，抑制缺失字段记录
            boolean needSuppress = filters.length > 0 && filters[0].startsWith("default");
            if (needSuppress) {
                this.suppressMissing = true;
            }
            // 提取原始值
            String value = extractValueFromJinja2Path(rootNode, path);
            if (needSuppress) {
                this.suppressMissing = false;
            }

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
                    // 记录字段缺失
                    if (mappingTracker != null && !suppressMissing) {
                        mappingTracker.recordMissingField(path, "DataX配置中未找到该字段");
                    }
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
                            if (mappingTracker != null && !suppressMissing) {
                                mappingTracker.recordMissingField(path, "无效的数组索引: " + indexStr);
                            }
                            return null;
                        }
                    }
                } else {
                    currentNode = currentNode.get(part);
                }
            }

            if (currentNode != null && !currentNode.isNull()) {
                String value;
                if (currentNode.isArray()) {
                    // 如果是数组，返回数组的所有元素
                    StringBuilder result = new StringBuilder();
                    for (int i = 0; i < currentNode.size(); i++) {
                        if (i > 0) result.append(",");
                        result.append(currentNode.get(i).asText());
                    }
                    value = result.toString();
                } else {
                    value = currentNode.asText();
                }

                // 记录成功的字段提取，除非已抑制或者是复杂转换的一部分
                if (mappingTracker != null
                        && !suppressMissing
                        && value != null
                        && !value.isEmpty()
                        && !isPartOfComplexTransform()) {
                    mappingTracker.recordDirectMapping(
                            path, currentTargetContext, value, "直接从DataX提取");
                }

                return value;
            } else {
                // 记录字段缺失
                if (mappingTracker != null && !suppressMissing) {
                    mappingTracker.recordMissingField(path, "DataX配置中字段值为空");
                }
            }

        } catch (Exception e) {
            logger.warn("提取 Jinja2 路径值失败: {}", path, e);
            if (mappingTracker != null && !suppressMissing) {
                mappingTracker.recordMissingField(path, "提取失败: " + e.getMessage());
            }
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

        // 记录原始值，用于比较是否发生了转换
        Object originalValue = value;

        // 应用过滤器
        Object result;
        switch (filterName) {
            case "join":
                if (value instanceof String[]) {
                    result =
                            applyJoinFilterOnArray(
                                    (String[]) value, filterArgs.isEmpty() ? "," : filterArgs);
                } else {
                    result =
                            applyJoinFilter(
                                    value.toString(), filterArgs.isEmpty() ? "," : filterArgs);
                }
                break;
            case "default":
                String stringValue = value.toString();
                boolean usedDefaultValue = stringValue.isEmpty();
                result = usedDefaultValue ? filterArgs : stringValue;

                // 记录是否使用了默认值，供后续映射记录使用
                if (mappingTracker != null && !isPartOfComplexTransform()) {
                    if (usedDefaultValue) {
                        // 使用了默认值
                        mappingTracker.recordDefaultValue(
                                currentTargetContext, result.toString(), "应用默认值: " + filterArgs);
                    } else {
                        // 使用了原值，属于直接映射
                        mappingTracker.recordDirectMapping(
                                null, currentTargetContext, result.toString(), "使用原值，未应用默认值");
                    }
                }
                break;
            case "upper":
                result = value.toString().toUpperCase();
                break;
            case "lower":
                result = value.toString().toLowerCase();
                break;
            case "regex_extract":
                {
                    // 使用原始filterExpression提取参数，保证包含引号和逗号
                    int lpos = filterExpression.indexOf('(');
                    int rpos = findMatchingCloseParen(filterExpression, lpos);
                    String rawArgs = filterExpression.substring(lpos + 1, rpos);
                    String extractedVal = applyRegexExtract(value.toString(), rawArgs);
                    result = extractedVal;
                    // 记录正则提取转换，仅此一次
                    if (mappingTracker != null
                            && !equals(originalValue, result)
                            && !isPartOfComplexTransform()) {
                        mappingTracker.recordTransformMapping(
                                null, currentTargetContext, result.toString(), filterName);
                    }
                }
                break;
            case "jdbc_driver_mapper":
                result = applyTransformer(value.toString(), "jdbc_driver_mapper");
                break;
            case "split":
                result = applySplit(value.toString(), filterArgs);
                break;
            case "get":
                result = applyGet(value, filterArgs);
                break;
            case "replace":
                result = applyReplace(value.toString(), filterArgs);
                break;
            default:
                // 检查是否是转换器调用
                if (templateMappingManager != null
                        && templateMappingManager.getTransformer(filterName) != null) {
                    result = applyTransformer(value.toString(), filterName);
                } else {
                    logger.warn("不支持的过滤器: {}", filterName);
                    result = value;
                }
        }

        // 记录字段转换（如果发生了转换）
        if (mappingTracker != null && !equals(originalValue, result)) {
            if ("regex_extract".equals(filterName)) {
                // 已在 regex_extract case 中记录，跳过重复记录
            } else if ("default".equals(filterName)) {
                // default过滤器的映射记录已经在case中处理，跳过重复记录
            } else if (!isPartOfComplexTransform()) {
                // 其他过滤器转换
                mappingTracker.recordTransformMapping(
                        null, currentTargetContext, result.toString(), filterName);
            }
        }

        return result;
    }

    /** 判断两个对象是否相等 */
    private boolean equals(Object obj1, Object obj2) {
        if (obj1 == null && obj2 == null) return true;
        if (obj1 == null || obj2 == null) return false;
        return obj1.toString().equals(obj2.toString());
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

    /** 设置当前目标上下文（用于映射跟踪） 这个方法可以被外部调用，在解析特定配置段时设置上下文 */
    public void setCurrentTargetContext(String targetContext) {
        this.currentTargetContext = targetContext;
    }

    /** 清除当前目标上下文 */
    public void clearCurrentTargetContext() {
        this.currentTargetContext = null;
    }

    /** 设置字段引用跟踪器 */
    public void setFieldReferenceTracker(DataXFieldExtractor.FieldReferenceTracker tracker) {
        this.fieldReferenceTracker = tracker;
    }

    /** 获取字段引用跟踪器 */
    public DataXFieldExtractor.FieldReferenceTracker getFieldReferenceTracker() {
        return this.fieldReferenceTracker;
    }

    /** 增加字段引用计数，支持数组字段的智能匹配 */
    private void incrementFieldReference(String normalizedPath) {
        if (fieldReferenceTracker == null) {
            return;
        }

        // 直接引用的字段
        fieldReferenceTracker.incrementReference(normalizedPath);
        logger.debug("字段引用计数: {}", normalizedPath);

        // 处理数组字段的双向匹配
        Map<String, String> allFields = fieldReferenceTracker.getAllFields();

        // 情况1：如果引用的是数组字段，需要将数组的所有元素也标记为已引用
        // 例如：引用 job.content[0].reader.parameter.connection[0].jdbcUrl 时，
        // 也要将 job.content[0].reader.parameter.connection[0].jdbcUrl[0], jdbcUrl[1] 等标记为已引用
        for (String fieldPath : allFields.keySet()) {
            if (isArrayElementOf(fieldPath, normalizedPath)) {
                fieldReferenceTracker.incrementReference(fieldPath);
                logger.debug("数组元素引用计数: {} (来自数组引用: {})", fieldPath, normalizedPath);
            }
        }

        // 情况2：如果引用的是数组元素，需要将对应的数组本身也标记为已引用
        // 例如：引用 job.content[0].reader.parameter.connection[0].jdbcUrl[0] 时，
        // 也要将 job.content[0].reader.parameter.connection[0].jdbcUrl 标记为已引用
        String arrayFieldName = getArrayFieldNameFromElement(normalizedPath);
        if (arrayFieldName != null && allFields.containsKey(arrayFieldName)) {
            fieldReferenceTracker.incrementReference(arrayFieldName);
            logger.debug("数组字段引用计数: {} (来自数组元素引用: {})", arrayFieldName, normalizedPath);
        }
    }

    /**
     * 判断 fieldPath 是否是 arrayPath 的数组元素 例如：job.content[0].reader.parameter.connection[0].jdbcUrl[0]
     * 是 job.content[0].reader.parameter.connection[0].jdbcUrl 的元素
     */
    private boolean isArrayElementOf(String fieldPath, String arrayPath) {
        // 检查是否是数组元素模式：arrayPath[index]
        if (fieldPath.startsWith(arrayPath + "[") && fieldPath.endsWith("]")) {
            // 提取索引部分，确保是数字
            String indexPart = fieldPath.substring(arrayPath.length() + 1, fieldPath.length() - 1);
            try {
                Integer.parseInt(indexPart);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * 从数组元素路径中提取数组字段名 例如：job.content[0].reader.parameter.connection[0].jdbcUrl[0] ->
     * job.content[0].reader.parameter.connection[0].jdbcUrl
     */
    private String getArrayFieldNameFromElement(String elementPath) {
        // 检查是否是数组元素模式：xxx[数字]
        if (elementPath.matches(".*\\[\\d+\\]$")) {
            int lastBracket = elementPath.lastIndexOf('[');
            return elementPath.substring(0, lastBracket);
        }
        return null;
    }

    /** 检查行是否包含过滤器 */
    private boolean containsFilters(String line) {
        return line.contains("|") && containsVariable(line);
    }

    /** 检查当前是否在处理复杂转换 */
    private boolean isPartOfComplexTransform() {
        return processingComplexTransform;
    }

    /** 记录复杂转换映射（包含多个变量和过滤器的行） */
    private void recordComplexTransformMapping(
            String originalLine, String resolvedLine, String targetContext) {
        if (mappingTracker == null) {
            return;
        }

        // 提取原始模板表达式
        String templateExpression = extractTemplateExpression(originalLine);

        // 提取最终值
        String finalValue = extractFinalValue(resolvedLine);

        // 提取使用的过滤器列表
        String filtersUsed = extractFiltersFromExpression(templateExpression);

        // 对模板表达式进行Markdown转义
        String escapedTemplateExpression = escapeMarkdownTableContent(templateExpression);

        // 记录为转换映射，使用转义后的模板表达式作为来源
        mappingTracker.recordTransformMapping(
                escapedTemplateExpression, targetContext, finalValue, filtersUsed);

        logger.debug(
                "记录复合转换映射: {} -> {} = {}", escapedTemplateExpression, targetContext, finalValue);
    }

    /** 提取模板表达式 */
    private String extractTemplateExpression(String line) {
        // 提取 = 后面的部分，去掉引号
        if (line.contains("=")) {
            String value = line.substring(line.indexOf("=") + 1).trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }
        return line.trim();
    }

    /** 提取最终值 */
    private String extractFinalValue(String resolvedLine) {
        if (resolvedLine.contains("=")) {
            String value = resolvedLine.substring(resolvedLine.indexOf("=") + 1).trim();
            if (value.startsWith("\"") && value.endsWith("\"")) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }
        return resolvedLine.trim();
    }

    /** 从模板表达式中提取过滤器列表 */
    private String extractFiltersFromExpression(String templateExpression) {
        if (templateExpression == null || !templateExpression.contains("|")) {
            return "";
        }

        Set<String> filters = new HashSet<>();

        // 使用正则表达式匹配所有的过滤器
        Pattern filterPattern = Pattern.compile("\\|\\s*([a-zA-Z_][a-zA-Z0-9_]*)");
        Matcher matcher = filterPattern.matcher(templateExpression);

        while (matcher.find()) {
            String filter = matcher.group(1);
            filters.add(filter);
        }

        // 将过滤器列表转换为字符串，用逗号分隔
        return String.join(", ", filters);
    }

    /** 对Markdown表格内容进行转义 */
    private String escapeMarkdownTableContent(String content) {
        if (content == null) {
            return "";
        }

        // 转义Markdown表格中的特殊字符
        return content.replace("|", "\\|") // 转义管道符
                .replace("\n", " ") // 将换行符替换为空格
                .replace("\r", "") // 移除回车符
                .trim();
    }

    /** 检查是否是硬编码的默认值配置行 */
    private boolean isHardcodedDefaultValue(String trimmedLine) {
        if (trimmedLine.isEmpty() || trimmedLine.startsWith("#") || !trimmedLine.contains("=")) {
            return false;
        }

        // 排除包含变量的行（这些已经在其他地方处理了）
        if (containsVariable(trimmedLine)) {
            return false;
        }

        // 排除结构性的行（如 "}" 等）
        if (trimmedLine.equals("}") || trimmedLine.equals("{")) {
            return false;
        }

        // 通用模式：任何不包含变量的 key = value 配置行都被认为是硬编码的默认值
        // 这包括：数字、布尔值、引号字符串等
        return trimmedLine.matches(".*=\\s*(.+)\\s*$");
    }

    /** 记录硬编码的默认值 */
    private void recordHardcodedDefaultValue(String trimmedLine, String targetContext) {
        if (mappingTracker == null) {
            return;
        }

        // 提取配置键和值
        String[] parts = trimmedLine.split("=", 2);
        if (parts.length != 2) {
            return;
        }

        String key = parts[0].trim();
        String value = parts[1].trim();

        // 移除引号
        if (value.startsWith("\"") && value.endsWith("\"")) {
            value = value.substring(1, value.length() - 1);
        }

        // 记录为默认值
        mappingTracker.recordDefaultValue(targetContext, value, "模板硬编码默认值");

        logger.debug("记录硬编码默认值: {} = {} (路径: {})", key, value, targetContext);
    }

    /** 智能上下文解析 - 逐行分析模板结构，推断准确的目标字段路径 */
    private String resolveWithSmartContext(String content, JsonNode rootNode) {
        StringBuilder result = new StringBuilder();
        String[] lines = content.split("\n");

        List<String> configPath = new ArrayList<>(); // 当前配置路径栈

        for (String line : lines) {
            String trimmedLine = line.trim();
            int indentLevel = getIndentLevel(line);

            // 更新配置路径栈
            updateConfigPath(configPath, trimmedLine, indentLevel);

            // 如果这行包含变量，设置准确的目标上下文
            if (containsVariable(line)) {
                logger.debug("发现包含变量的行: {}", line.trim());
                String targetContext = buildTargetContext(configPath, trimmedLine);
                String previousContext = this.currentTargetContext;
                this.currentTargetContext = targetContext;

                try {
                    // 检查这行是否包含过滤器，决定如何记录映射
                    boolean hasFilters = containsFilters(line);
                    String originalLine = line;

                    // 如果包含过滤器，设置复杂转换标志
                    if (hasFilters) {
                        processingComplexTransform = true;
                    }

                    // 解析该行的变量
                    String resolvedLine = resolveJinja2FilterVariables(line, rootNode);
                    resolvedLine = resolveJinja2Variables(resolvedLine, rootNode);

                    // 如果包含过滤器，记录为复合转换映射
                    if (hasFilters && mappingTracker != null) {
                        recordComplexTransformMapping(originalLine, resolvedLine, targetContext);
                    }

                    result.append(resolvedLine).append("\n");
                } finally {
                    // 恢复之前的上下文和标志
                    this.currentTargetContext = previousContext;
                    this.processingComplexTransform = false;
                }
            } else {
                // 检查是否是硬编码的默认值配置行
                if (isHardcodedDefaultValue(trimmedLine)) {
                    String targetContext = buildTargetContext(configPath, trimmedLine);
                    recordHardcodedDefaultValue(trimmedLine, targetContext);
                }

                // 没有变量的行直接添加
                result.append(line).append("\n");
            }
        }

        // 移除最后一个换行符
        if (result.length() > 0) {
            result.setLength(result.length() - 1);
        }

        return result.toString();
    }

    /** 检查行是否包含模板变量 */
    private boolean containsVariable(String line) {
        return line.contains("{{") && line.contains("}}");
    }

    /** 获取行的缩进级别 */
    private int getIndentLevel(String line) {
        int indent = 0;
        for (char c : line.toCharArray()) {
            if (c == ' ') {
                indent++;
            } else if (c == '\t') {
                indent += 4; // tab视为4个空格
            } else {
                break;
            }
        }
        return indent;
    }

    /** 更新配置路径栈 */
    private void updateConfigPath(List<String> configPath, String trimmedLine, int indentLevel) {
        logger.debug(
                "更新配置路径: indentLevel={}, 当前configPath={}, trimmedLine='{}'",
                indentLevel,
                configPath,
                trimmedLine);

        // 忽略空行和注释行，不要因为它们而影响配置路径
        if (trimmedLine.isEmpty() || trimmedLine.startsWith("#")) {
            logger.debug("忽略空行或注释行，保持configPath不变: {}", configPath);
            return;
        }

        // 根据缩进调整路径深度（每2个空格为一级）
        int targetDepth = indentLevel / 2;

        logger.debug("计算目标深度: targetDepth={}", targetDepth);

        while (configPath.size() > targetDepth) {
            String removed = configPath.remove(configPath.size() - 1);
            logger.debug("移除路径元素: {}, 剩余configPath={}", removed, configPath);
        }

        // 如果这是一个配置块的开始，添加到路径中
        if (trimmedLine.endsWith("{")) {
            String configKey = trimmedLine.substring(0, trimmedLine.indexOf("{")).trim();
            if (!configKey.isEmpty()) {
                configPath.add(configKey);
                logger.debug("添加路径元素: {}, 更新后configPath={}", configKey, configPath);
            }
        }
    }

    /** 构建目标上下文路径 */
    private String buildTargetContext(List<String> configPath, String trimmedLine) {
        StringBuilder targetPath = new StringBuilder();

        // 添加配置路径
        for (String pathPart : configPath) {
            if (targetPath.length() > 0) {
                targetPath.append(".");
            }
            targetPath.append(pathPart);
        }

        // 如果当前行包含具体的配置项（key = value格式），添加配置键
        if (trimmedLine.contains("=")) {
            String configKey = extractConfigKey(trimmedLine);
            if (configKey != null && !configKey.isEmpty()) {
                if (targetPath.length() > 0) {
                    targetPath.append(".");
                }
                targetPath.append(configKey);
            }
        }

        String result = targetPath.toString();
        logger.debug(
                "构建目标上下文: configPath={}, trimmedLine='{}', result='{}'",
                configPath,
                trimmedLine,
                result);
        return result;
    }

    /** 提取配置键名 */
    private String extractConfigKey(String trimmedLine) {
        if (trimmedLine.contains("=")) {
            // key = value 格式
            return trimmedLine.substring(0, trimmedLine.indexOf("=")).trim();
        }
        return null;
    }

    /**
     * 分析模板并提取字段映射关系（替代 HOCON 解析）
     *
     * @param templateContent 模板内容
     * @param templateType 模板类型 (source/sink)
     * @return 字段路径到变量列表的映射
     */
    public Map<String, List<String>> analyzeTemplateFieldMappings(
            String templateContent, String templateType) {
        Map<String, List<String>> fieldMappings = new HashMap<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return fieldMappings;
        }

        String[] lines = templateContent.split("\n");
        List<String> configPath = new ArrayList<>();

        for (String line : lines) {
            String trimmedLine = line.trim();
            int indentLevel = getIndentLevel(line);

            // 更新配置路径栈
            updateConfigPath(configPath, trimmedLine, indentLevel);

            // 如果这行包含变量，提取字段路径和变量
            if (containsVariable(line)) {
                String fieldPath = buildFieldPath(templateType, configPath, trimmedLine);
                List<String> variables = extractVariablesFromLine(line);

                if (!variables.isEmpty()) {
                    fieldMappings.put(fieldPath, variables);
                    logger.debug("提取字段映射: {} -> {}", fieldPath, variables);
                }
            }
        }

        return fieldMappings;
    }

    /** 从行中提取所有模板变量 */
    private List<String> extractVariablesFromLine(String line) {
        List<String> variables = new ArrayList<>();

        // 提取过滤器变量
        Matcher filterMatcher = JINJA2_FILTER_PATTERN.matcher(line);
        while (filterMatcher.find()) {
            String path = filterMatcher.group(1).trim();
            variables.add(path);
        }

        // 提取基础变量（排除已经被过滤器模式匹配的）
        String lineAfterFilters = filterMatcher.replaceAll("");
        Matcher variableMatcher = JINJA2_VARIABLE_PATTERN.matcher(lineAfterFilters);
        while (variableMatcher.find()) {
            String path = variableMatcher.group(1).trim();
            variables.add(path);
        }

        return variables;
    }

    /** 构建字段路径 */
    private String buildFieldPath(
            String templateType, List<String> configPath, String trimmedLine) {
        StringBuilder fieldPath = new StringBuilder();

        // 添加模板类型前缀
        if (templateType != null && !templateType.isEmpty()) {
            fieldPath.append(templateType);
        }

        // 添加配置路径
        for (String pathPart : configPath) {
            if (fieldPath.length() > 0) {
                fieldPath.append(".");
            }
            fieldPath.append(pathPart);
        }

        // 如果当前行包含具体的配置项（key = value格式），添加配置键
        String configKey = extractConfigKey(trimmedLine);
        if (configKey != null && !configKey.isEmpty()) {
            if (fieldPath.length() > 0) {
                fieldPath.append(".");
            }
            fieldPath.append(configKey);
        }

        return fieldPath.toString();
    }

    /**
     * 使用模板分析解析模板并跟踪字段映射（替代 HOCON 方案）
     *
     * @param templateContent 模板内容
     * @param templateType 模板类型 (source/sink)
     * @param dataXConfig DataX配置
     * @return 解析后的内容
     */
    public String resolveWithTemplateAnalysis(
            String templateContent, String templateType, DataXConfig dataXConfig) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return templateContent;
        }

        logger.info("使用模板分析解析模板类型: {}", templateType);

        try {
            // 1. 分析模板，提取字段变量映射
            Map<String, List<String>> fieldVariables =
                    analyzeTemplateFieldMappings(templateContent, templateType);

            // 2. 将DataXConfig转换为JsonNode以便路径查询
            JsonNode rootNode = objectMapper.valueToTree(dataXConfig);

            // 3. 解析模板内容
            String result = templateContent;

            // 4. 对每个字段进行变量解析和映射跟踪
            for (Map.Entry<String, List<String>> entry : fieldVariables.entrySet()) {
                String fieldPath = entry.getKey();
                List<String> variables = entry.getValue();

                // 设置当前目标上下文为精确的字段路径
                this.currentTargetContext = fieldPath;

                logger.debug("处理字段: {} -> 变量: {}", fieldPath, variables);
            }

            // 5. 处理 Jinja2 风格变量
            result = resolveJinja2FilterVariables(result, rootNode);
            result = resolveJinja2Variables(result, rootNode);

            // 6. 重置上下文
            this.currentTargetContext = null;

            logger.info("模板分析解析完成，字段总数: {}", fieldVariables.size());
            return result;

        } catch (Exception e) {
            logger.error("模板分析解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板分析解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 使用模板分析解析模板并跟踪字段映射（使用原始JSON字符串）
     *
     * @param templateContent 模板内容
     * @param templateType 模板类型 (source/sink)
     * @param dataXJsonContent DataX JSON配置内容
     * @return 解析后的内容
     */
    public String resolveWithTemplateAnalysis(
            String templateContent, String templateType, String dataXJsonContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return templateContent;
        }

        logger.info("使用模板分析解析模板类型: {}", templateType);

        try {
            // 1. 分析模板，提取字段变量映射
            Map<String, List<String>> fieldVariables =
                    analyzeTemplateFieldMappings(templateContent, templateType);

            // 2. 直接解析JSON字符串为JsonNode
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);

            // 3. 使用智能上下文解析处理所有变量
            String result = resolveWithSmartContext(templateContent, rootNode);

            logger.info("模板分析解析完成，字段总数: {}", fieldVariables.size());
            return result;

        } catch (Exception e) {
            logger.error("模板分析解析失败: {}", e.getMessage(), e);
            throw new RuntimeException("模板分析解析失败: " + e.getMessage(), e);
        }
    }

    /** 验证模板语法（基于 Jinja2 模式） */
    public boolean validateTemplate(String templateContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return true;
        }

        try {
            // 检查是否存在未闭合的模板变量
            long openCount = templateContent.chars().filter(ch -> ch == '{').count();
            long closeCount = templateContent.chars().filter(ch -> ch == '}').count();

            if (openCount != closeCount) {
                logger.warn("模板验证失败: 花括号不匹配");
                return false;
            }

            // 检查变量语法是否正确
            Matcher matcher = JINJA2_VARIABLE_PATTERN.matcher(templateContent);
            while (matcher.find()) {
                String variable = matcher.group(1).trim();
                if (variable.isEmpty()) {
                    logger.warn("模板验证失败: 发现空变量");
                    return false;
                }
            }

            Matcher filterMatcher = JINJA2_FILTER_PATTERN.matcher(templateContent);
            while (filterMatcher.find()) {
                String variable = filterMatcher.group(1).trim();
                String filter = filterMatcher.group(2).trim();
                if (variable.isEmpty() || filter.isEmpty()) {
                    logger.warn("模板验证失败: 发现空变量或过滤器");
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            logger.error("模板验证异常: {}", e.getMessage(), e);
            return false;
        }
    }

    /** 获取模板的根键名（如 Jdbc, Kafka 等） */
    public String getTemplateRootKey(String templateContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return null;
        }

        String[] lines = templateContent.split("\n");
        for (String line : lines) {
            String trimmed = line.trim();
            if (trimmed.matches("\\w+\\s*\\{")) {
                return trimmed.substring(0, trimmed.indexOf('{')).trim();
            }
        }

        return null;
    }
}
