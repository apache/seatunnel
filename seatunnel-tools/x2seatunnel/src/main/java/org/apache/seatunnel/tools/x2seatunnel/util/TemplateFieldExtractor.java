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

package org.apache.seatunnel.tools.x2seatunnel.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 模板字段提取器 - 提取模板中引用的DataX字段路径 */
public class TemplateFieldExtractor {

    private static final Logger logger = LoggerFactory.getLogger(TemplateFieldExtractor.class);

    // 匹配模板变量的正则表达式：{{ datax.xxx }}
    private static final Pattern DATAX_VARIABLE_PATTERN =
            Pattern.compile("\\{\\{\\s*datax\\.([^}|\\s]+)(?:\\s*\\|[^}]*)?\\s*\\}\\}");

    /**
     * 从模板内容中提取所有引用的DataX字段路径
     *
     * @param templateContent 模板内容
     * @return 引用的DataX字段路径集合
     */
    public Set<String> extractReferencedFields(String templateContent) {
        Set<String> referencedFields = new HashSet<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return referencedFields;
        }

        Matcher matcher = DATAX_VARIABLE_PATTERN.matcher(templateContent);

        while (matcher.find()) {
            String fieldPath = matcher.group(1); // 提取 datax. 后面的部分
            String normalizedPath = normalizeFieldPath(fieldPath);
            referencedFields.add(normalizedPath);

            logger.trace("提取模板引用字段: {} -> {}", matcher.group(0), normalizedPath);
        }

        logger.debug("从模板中提取到 {} 个引用字段", referencedFields.size());
        return referencedFields;
    }

    /**
     * 从多个模板内容中提取所有引用的DataX字段路径
     *
     * @param templateContents 多个模板内容
     * @return 引用的DataX字段路径集合
     */
    public Set<String> extractReferencedFields(String... templateContents) {
        Set<String> allReferencedFields = new HashSet<>();

        for (String templateContent : templateContents) {
            if (templateContent != null) {
                Set<String> fields = extractReferencedFields(templateContent);
                allReferencedFields.addAll(fields);
            }
        }

        logger.debug(
                "从 {} 个模板中总共提取到 {} 个引用字段", templateContents.length, allReferencedFields.size());
        return allReferencedFields;
    }

    /**
     * 标准化字段路径，将模板中的路径格式转换为与DataX JSON路径一致的格式
     *
     * @param fieldPath 原始字段路径
     * @return 标准化后的字段路径
     */
    private String normalizeFieldPath(String fieldPath) {
        // 模板中：job.content[0].reader.parameter.username
        // 标准化为：job.content[0].reader.parameter.username
        // 直接返回，因为模板中已经是正确的格式

        return fieldPath;
    }

    /**
     * 检查模板内容是否包含DataX变量引用
     *
     * @param templateContent 模板内容
     * @return 是否包含DataX变量引用
     */
    public boolean containsDataXReferences(String templateContent) {
        if (templateContent == null || templateContent.trim().isEmpty()) {
            return false;
        }

        return DATAX_VARIABLE_PATTERN.matcher(templateContent).find();
    }

    /**
     * 获取模板中所有DataX变量的详细信息（包括过滤器）
     *
     * @param templateContent 模板内容
     * @return 变量详细信息集合
     */
    public Set<String> extractVariableDetails(String templateContent) {
        Set<String> variableDetails = new HashSet<>();

        if (templateContent == null || templateContent.trim().isEmpty()) {
            return variableDetails;
        }

        Matcher matcher = DATAX_VARIABLE_PATTERN.matcher(templateContent);

        while (matcher.find()) {
            String fullVariable = matcher.group(0); // 完整的变量表达式
            variableDetails.add(fullVariable);

            logger.trace("提取变量详情: {}", fullVariable);
        }

        return variableDetails;
    }
}
