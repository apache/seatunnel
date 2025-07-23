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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** DataX字段提取器 - 提取DataX JSON配置中的所有字段路径 */
public class DataXFieldExtractor {

    private static final Logger logger = LoggerFactory.getLogger(DataXFieldExtractor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 从DataX JSON字符串中提取所有字段路径
     *
     * @param dataXJsonContent DataX JSON配置内容
     * @return 所有字段路径的集合
     */
    public Set<String> extractAllFields(String dataXJsonContent) {
        Set<String> allFields = new HashSet<>();

        try {
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);
            extractFieldsRecursively(rootNode, "", allFields);

            logger.debug("从DataX配置中提取到 {} 个字段", allFields.size());
            return allFields;

        } catch (Exception e) {
            logger.error("提取DataX字段失败: {}", e.getMessage(), e);
            return allFields;
        }
    }

    /**
     * 递归提取JSON节点中的所有字段路径
     *
     * @param node 当前JSON节点
     * @param currentPath 当前路径
     * @param allFields 收集所有字段的集合
     */
    private void extractFieldsRecursively(
            JsonNode node, String currentPath, Set<String> allFields) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            // 处理对象节点
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String fieldPath =
                        currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;

                if (fieldValue.isValueNode()) {
                    // 叶子节点，记录字段路径
                    allFields.add(fieldPath);
                    logger.trace("提取字段: {} = {}", fieldPath, fieldValue.asText());
                } else {
                    // 继续递归
                    extractFieldsRecursively(fieldValue, fieldPath, allFields);
                }
            }
        } else if (node.isArray()) {
            // 处理数组节点
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                String arrayPath = currentPath + "[" + i + "]";
                extractFieldsRecursively(arrayElement, arrayPath, allFields);
            }
        } else if (node.isValueNode()) {
            // 值节点，记录字段路径
            allFields.add(currentPath);
            logger.trace("提取字段: {} = {}", currentPath, node.asText());
        }
    }

    /**
     * 过滤出有意义的DataX字段（排除一些系统字段）
     *
     * @param allFields 所有字段
     * @return 过滤后的字段
     */
    public Set<String> filterMeaningfulFields(Set<String> allFields) {
        Set<String> meaningfulFields = new HashSet<>();

        for (String field : allFields) {
            // 只保留 content 下的 reader 和 writer 参数，以及 setting 下的配置
            if (field.contains(".content[")
                    && (field.contains(".reader.parameter.")
                            || field.contains(".writer.parameter."))) {
                meaningfulFields.add(field);
            } else if (field.contains(".setting.")) {
                meaningfulFields.add(field);
            }
            // 可以根据需要添加更多过滤规则
        }

        logger.debug("过滤后保留 {} 个有意义的字段", meaningfulFields.size());
        return meaningfulFields;
    }

    /**
     * 从DataX JSON字符串中提取所有字段路径和值的映射
     *
     * @param dataXJsonContent DataX JSON配置内容
     * @return 字段路径到值的映射
     */
    public Map<String, String> extractAllFieldsWithValues(String dataXJsonContent) {
        Map<String, String> fieldValueMap = new HashMap<>();

        try {
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);
            extractFieldsWithValuesRecursively(rootNode, "", fieldValueMap);

            logger.debug("从DataX配置中提取到 {} 个字段及其值", fieldValueMap.size());
            return fieldValueMap;

        } catch (Exception e) {
            logger.error("提取DataX字段和值失败: {}", e.getMessage(), e);
            return fieldValueMap;
        }
    }

    /**
     * 递归提取JSON节点中的所有字段路径和值
     *
     * @param node 当前JSON节点
     * @param currentPath 当前路径
     * @param fieldValueMap 收集字段路径和值的映射
     */
    private void extractFieldsWithValuesRecursively(
            JsonNode node, String currentPath, Map<String, String> fieldValueMap) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            // 处理对象节点
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String fieldPath =
                        currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;

                if (fieldValue.isValueNode()) {
                    // 叶子节点，记录字段路径和值
                    String value = fieldValue.asText();
                    fieldValueMap.put(fieldPath, value);
                    logger.trace("提取字段: {} = {}", fieldPath, value);
                } else {
                    // 继续递归
                    extractFieldsWithValuesRecursively(fieldValue, fieldPath, fieldValueMap);
                }
            }
        } else if (node.isArray()) {
            // 处理数组节点
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                String arrayPath = currentPath + "[" + i + "]";
                extractFieldsWithValuesRecursively(arrayElement, arrayPath, fieldValueMap);
            }
        } else if (node.isValueNode()) {
            // 值节点，记录字段路径和值
            String value = node.asText();
            fieldValueMap.put(currentPath, value);
            logger.trace("提取字段: {} = {}", currentPath, value);
        }
    }

    /**
     * 过滤出有意义的DataX字段及其值
     *
     * @param allFieldsWithValues 所有字段及其值
     * @return 过滤后的字段及其值
     */
    public Map<String, String> filterMeaningfulFieldsWithValues(
            Map<String, String> allFieldsWithValues) {
        Map<String, String> meaningfulFields = new HashMap<>();
        Set<String> arrayFieldsProcessed = new HashSet<>();

        for (Map.Entry<String, String> entry : allFieldsWithValues.entrySet()) {
            String field = entry.getKey();
            String value = entry.getValue();

            // 只保留 content 下的 reader 和 writer 参数，以及 setting 下的配置
            if (field.contains(".content[")
                    && (field.contains(".reader.parameter.")
                            || field.contains(".writer.parameter."))) {

                // 检查是否是数组元素（如 column[0], table[1] 等）
                String arrayField = getArrayFieldName(field);
                if (arrayField != null) {
                    // 如果是数组元素，只记录数组本身，不记录每个元素
                    if (!arrayFieldsProcessed.contains(arrayField)) {
                        // 收集该数组的所有值
                        String arrayValues = collectArrayValues(allFieldsWithValues, arrayField);
                        meaningfulFields.put(arrayField, arrayValues);
                        arrayFieldsProcessed.add(arrayField);
                        logger.trace("处理数组字段: {} = {}", arrayField, arrayValues);
                    }
                } else {
                    // 非数组字段，直接添加
                    meaningfulFields.put(field, value);
                }
            } else if (field.contains(".setting.")) {
                meaningfulFields.put(field, value);
            }
        }

        logger.debug("过滤后保留 {} 个有意义的字段及其值（数组字段已合并）", meaningfulFields.size());
        return meaningfulFields;
    }

    /** 字段引用跟踪器 - 用于跟踪DataX字段的引用情况 */
    public static class FieldReferenceTracker {
        private final Map<String, String> fieldValues = new HashMap<>();
        private final Map<String, Integer> referenceCount = new HashMap<>();

        public void addField(String fieldPath, String value) {
            fieldValues.put(fieldPath, value);
            referenceCount.put(fieldPath, 0);
        }

        public void incrementReference(String fieldPath) {
            referenceCount.put(fieldPath, referenceCount.getOrDefault(fieldPath, 0) + 1);
        }

        public Map<String, String> getUnreferencedFields() {
            Map<String, String> unreferenced = new HashMap<>();
            for (Map.Entry<String, Integer> entry : referenceCount.entrySet()) {
                if (entry.getValue() == 0) {
                    String fieldPath = entry.getKey();
                    String value = fieldValues.get(fieldPath);
                    unreferenced.put(fieldPath, value);
                }
            }
            return unreferenced;
        }

        public int getTotalFields() {
            return fieldValues.size();
        }

        public int getReferencedFieldCount() {
            return (int) referenceCount.values().stream().filter(count -> count > 0).count();
        }

        public int getUnreferencedFieldCount() {
            return (int) referenceCount.values().stream().filter(count -> count == 0).count();
        }

        public Map<String, String> getAllFields() {
            return new HashMap<>(fieldValues);
        }
    }

    /**
     * 创建字段引用跟踪器
     *
     * @param dataXJsonContent DataX JSON配置内容
     * @return 字段引用跟踪器
     */
    public FieldReferenceTracker createFieldReferenceTracker(String dataXJsonContent) {
        FieldReferenceTracker tracker = new FieldReferenceTracker();

        try {
            Map<String, String> allFieldsWithValues = extractAllFieldsWithValues(dataXJsonContent);
            Map<String, String> meaningfulFields =
                    filterMeaningfulFieldsWithValues(allFieldsWithValues);

            for (Map.Entry<String, String> entry : meaningfulFields.entrySet()) {
                tracker.addField(entry.getKey(), entry.getValue());
            }

            logger.debug("创建字段引用跟踪器，包含 {} 个字段", tracker.getTotalFields());
            return tracker;

        } catch (Exception e) {
            logger.error("创建字段引用跟踪器失败: {}", e.getMessage(), e);
            return tracker;
        }
    }

    /**
     * 检查字段是否是数组元素，如果是则返回数组字段名 例如：job.content[0].reader.parameter.column[1] ->
     * job.content[0].reader.parameter.column
     */
    private String getArrayFieldName(String field) {
        // 匹配模式：xxx[数字]
        if (field.matches(".*\\[\\d+\\]$")) {
            int lastBracket = field.lastIndexOf('[');
            return field.substring(0, lastBracket);
        }
        return null;
    }

    /** 收集数组字段的所有值 例如：column[0]=id, column[1]=name -> "id,name" */
    private String collectArrayValues(Map<String, String> allFields, String arrayField) {
        List<String> values = new ArrayList<>();

        for (Map.Entry<String, String> entry : allFields.entrySet()) {
            String field = entry.getKey();
            if (field.startsWith(arrayField + "[") && field.matches(".*\\[\\d+\\]$")) {
                values.add(entry.getValue());
            }
        }

        return String.join(",", values);
    }
}
