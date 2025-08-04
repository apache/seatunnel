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

/** DataX field extractor - extract all field paths from DataX JSON configuration */
public class DataXFieldExtractor {

    private static final Logger logger = LoggerFactory.getLogger(DataXFieldExtractor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Set<String> extractAllFields(String dataXJsonContent) {
        Set<String> allFields = new HashSet<>();

        try {
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);
            extractFieldsRecursively(rootNode, "", allFields);
            return allFields;

        } catch (Exception e) {
            logger.error("Failed to extract DataX fields: {}", e.getMessage(), e);
            return allFields;
        }
    }

    /**
     * Recursively extract all field paths from the JSON node
     *
     * @param node the current JSON node
     * @param currentPath the current path
     * @param allFields the set to collect all fields
     */
    private void extractFieldsRecursively(
            JsonNode node, String currentPath, Set<String> allFields) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            // Process object node
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String fieldPath =
                        currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;

                if (fieldValue.isValueNode()) {
                    // Leaf node, record the field path
                    allFields.add(fieldPath);
                    logger.debug("Extracted field: {} = {}", fieldPath, fieldValue.asText());
                } else {
                    // Continue recursion
                    extractFieldsRecursively(fieldValue, fieldPath, allFields);
                }
            }
        } else if (node.isArray()) {
            // Process array node
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                String arrayPath = currentPath + "[" + i + "]";
                extractFieldsRecursively(arrayElement, arrayPath, allFields);
            }
        } else if (node.isValueNode()) {
            // Value node, record the field path
            allFields.add(currentPath);
            logger.debug("Extracted field: {} = {}", currentPath, node.asText());
        }
    }

    /**
     * Filter meaningful DataX fields (excluding system fields)
     *
     * @param allFields all fields
     * @return filtered meaningful fields
     */
    public Set<String> filterMeaningfulFields(Set<String> allFields) {
        Set<String> meaningfulFields = new HashSet<>();

        for (String field : allFields) {
            // Only keep reader and writer parameters under content, and configurations under
            // setting
            if (field.contains(".content[")
                    && (field.contains(".reader.parameter.")
                            || field.contains(".writer.parameter."))) {
                meaningfulFields.add(field);
            } else if (field.contains(".setting.")) {
                meaningfulFields.add(field);
            }
            // More filtering rules can be added as needed
        }

        logger.debug("{} meaningful fields retained after filtering", meaningfulFields.size());
        return meaningfulFields;
    }

    /**
     * Extract mappings of all field paths and their values from DataX JSON string
     *
     * @param dataXJsonContent DataX JSON configuration content
     * @return mappings from field paths to values
     */
    public Map<String, String> extractAllFieldsWithValues(String dataXJsonContent) {
        Map<String, String> fieldValueMap = new HashMap<>();

        try {
            JsonNode rootNode = objectMapper.readTree(dataXJsonContent);
            extractFieldsWithValuesRecursively(rootNode, "", fieldValueMap);

            logger.debug(
                    "Extracted {} fields with values from DataX configuration",
                    fieldValueMap.size());
            return fieldValueMap;

        } catch (Exception e) {
            logger.error("Failed to extract DataX fields and values: {}", e.getMessage(), e);
            return fieldValueMap;
        }
    }

    /**
     * Recursively extract all field paths and their values from the JSON node
     *
     * @param node the current JSON node
     * @param currentPath the current path
     * @param fieldValueMap the map to collect field paths and values
     */
    private void extractFieldsWithValuesRecursively(
            JsonNode node, String currentPath, Map<String, String> fieldValueMap) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                String fieldPath =
                        currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;

                if (fieldValue.isValueNode()) {
                    // Leaf node, record the field path and value
                    String value = fieldValue.asText();
                    fieldValueMap.put(fieldPath, value);
                    logger.debug("Extracted field: {} = {}", fieldPath, value);
                } else {
                    extractFieldsWithValuesRecursively(fieldValue, fieldPath, fieldValueMap);
                }
            }
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode arrayElement = node.get(i);
                String arrayPath = currentPath + "[" + i + "]";
                extractFieldsWithValuesRecursively(arrayElement, arrayPath, fieldValueMap);
            }
        } else if (node.isValueNode()) {
            // Value node, record the field path and value
            String value = node.asText();
            fieldValueMap.put(currentPath, value);
            logger.debug("Extracted field: {} = {}", currentPath, value);
        }
    }

    /**
     * Filter meaningful DataX fields and their values
     *
     * @param allFieldsWithValues all fields and their values
     * @return filtered meaningful fields and their values
     */
    public Map<String, String> filterMeaningfulFieldsWithValues(
            Map<String, String> allFieldsWithValues) {
        Map<String, String> meaningfulFields = new HashMap<>();
        Set<String> arrayFieldsProcessed = new HashSet<>();

        for (Map.Entry<String, String> entry : allFieldsWithValues.entrySet()) {
            String field = entry.getKey();
            String value = entry.getValue();

            if (field.contains(".content[")
                    && (field.contains(".reader.parameter.")
                            || field.contains(".writer.parameter."))) {

                String arrayField = getArrayFieldName(field);
                if (arrayField != null) {
                    // If it's an array element, only record the array itself, not each element
                    if (!arrayFieldsProcessed.contains(arrayField)) {
                        String arrayValues = collectArrayValues(allFieldsWithValues, arrayField);
                        meaningfulFields.put(arrayField, arrayValues);
                        arrayFieldsProcessed.add(arrayField);
                        logger.debug("Processed array field: {} = {}", arrayField, arrayValues);
                    }
                } else {
                    // Non-array field, add directly
                    meaningfulFields.put(field, value);
                }
            } else if (field.contains(".setting.")) {
                meaningfulFields.put(field, value);
            }
        }

        logger.debug(
                "Retained {} meaningful fields and their values after filtering (array fields merged)",
                meaningfulFields.size());
        return meaningfulFields;
    }

    /** Field reference tracker - track reference status of DataX fields */
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
     * Create a field reference tracker
     *
     * @param dataXJsonContent DataX JSON configuration content
     * @return the field reference tracker
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

            logger.debug(
                    "Created field reference tracker with {} fields", tracker.getTotalFields());
            return tracker;

        } catch (Exception e) {
            logger.error("Failed to create field reference tracker: {}", e.getMessage(), e);
            return tracker;
        }
    }

    /**
     * Check if a field is an array element. If so, return the array field name. For example:
     * job.content[0].reader.parameter.column[1] -> job.content[0].reader.parameter.column
     */
    private String getArrayFieldName(String field) {
        // Match pattern: xxx[number]
        if (field.matches(".*\\[\\d+\\]$")) {
            int lastBracket = field.lastIndexOf('[');
            return field.substring(0, lastBracket);
        }
        return null;
    }

    /**
     * Collect all values of an array field. For example: column[0]=id, column[1]=name -> "id,name"
     */
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
