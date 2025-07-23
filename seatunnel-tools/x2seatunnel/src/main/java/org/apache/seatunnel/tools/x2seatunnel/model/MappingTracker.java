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

package org.apache.seatunnel.tools.x2seatunnel.model;

import org.apache.seatunnel.tools.x2seatunnel.util.DataXFieldExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 映射跟踪器 - 记录字段映射过程，用于生成详细的转换报告 */
public class MappingTracker {

    private static final Logger logger = LoggerFactory.getLogger(MappingTracker.class);

    private final List<FieldMapping> directMappings = new ArrayList<>(); // 直接映射
    private final List<FieldMapping> transformMappings = new ArrayList<>(); // 转换映射（过滤器）
    private final List<FieldMapping> defaultValues = new ArrayList<>(); // 使用默认值
    private final List<FieldMapping> missingFields = new ArrayList<>(); // 缺失字段
    private final List<FieldMapping> unmappedFields = new ArrayList<>(); // 未映射字段

    /** 记录成功的直接映射 */
    public void recordDirectMapping(
            String sourcePath, String targetField, String value, String description) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, targetField, value, description, MappingType.DIRECT);
        directMappings.add(mapping);
        logger.debug("记录直接映射: {} -> {} = {}", sourcePath, targetField, value);
    }

    /** 记录转换映射的字段（使用过滤器） */
    public void recordTransformMapping(
            String sourcePath, String targetField, String value, String filterName) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, targetField, value, filterName, MappingType.TRANSFORM);
        transformMappings.add(mapping);
        logger.debug("记录转换映射: {} -> {} = {} (过滤器: {})", sourcePath, targetField, value, filterName);
    }

    /** 记录使用默认值的字段 */
    public void recordDefaultValue(String targetField, String value, String reason) {
        FieldMapping mapping =
                new FieldMapping(null, targetField, value, reason, MappingType.DEFAULT);
        defaultValues.add(mapping);
        logger.debug("记录默认值: {} = {} ({})", targetField, value, reason);
    }

    /** 记录缺失的必填字段 */
    public void recordMissingField(String sourcePath, String reason) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, null, null, reason, MappingType.MISSING);
        missingFields.add(mapping);
        logger.debug("记录缺失字段: {} ({})", sourcePath, reason);
    }

    /** 记录未映射的字段 */
    public void recordUnmappedField(String sourcePath, String value, String reason) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, null, value, reason, MappingType.UNMAPPED);
        unmappedFields.add(mapping);
        logger.debug("记录未映射字段: {} = {} ({})", sourcePath, value, reason);
    }

    /** 生成完整的映射结果 */
    public MappingResult generateMappingResult() {
        MappingResult result = new MappingResult();

        // 转换直接映射
        for (FieldMapping mapping : directMappings) {
            result.addSuccessMapping(
                    mapping.getSourcePath(), mapping.getTargetField(), mapping.getValue());
        }

        // 转换转换映射字段
        for (FieldMapping mapping : transformMappings) {
            result.addTransformMapping(
                    mapping.getSourcePath(),
                    mapping.getTargetField(),
                    mapping.getValue(),
                    mapping.getDescription());
        }

        // 转换默认值字段 - 单独归类
        for (FieldMapping mapping : defaultValues) {
            result.addDefaultValueField(
                    mapping.getTargetField(), mapping.getValue(), mapping.getDescription());
        }

        // 转换缺失字段
        for (FieldMapping mapping : missingFields) {
            result.addMissingRequiredField(mapping.getSourcePath(), mapping.getDescription());
        }

        // 转换未映射字段
        for (FieldMapping mapping : unmappedFields) {
            result.addUnmappedField(
                    mapping.getSourcePath(), mapping.getValue(), mapping.getDescription());
        }

        result.setSuccess(true);

        logger.info(
                "映射跟踪完成: 直接映射({})个, 转换映射({})个, 默认值({})个, 缺失({})个, 未映射({})个",
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());

        return result;
    }

    /** 重置映射跟踪器状态，为新的转换过程做准备 */
    public void reset() {
        directMappings.clear();
        transformMappings.clear();
        defaultValues.clear();
        missingFields.clear();
        unmappedFields.clear();
        logger.info("映射跟踪器已重置");
    }

    /**
     * 基于字段引用跟踪器计算并记录未映射的字段
     *
     * @param fieldReferenceTracker 字段引用跟踪器
     */
    public void calculateUnmappedFieldsFromTracker(
            DataXFieldExtractor.FieldReferenceTracker fieldReferenceTracker) {
        try {
            if (fieldReferenceTracker == null) {
                logger.warn("字段引用跟踪器为空，跳过未映射字段计算");
                return;
            }

            // 获取未引用的字段
            Map<String, String> unreferencedFields = fieldReferenceTracker.getUnreferencedFields();

            // 记录未映射字段（带实际值）
            for (Map.Entry<String, String> entry : unreferencedFields.entrySet()) {
                String fieldPath = entry.getKey();
                String actualValue = entry.getValue();
                recordUnmappedField(fieldPath, actualValue, "DataX中存在但模板中未引用");
            }

            logger.info(
                    "未映射字段计算完成: 总字段({})个, 已引用({})个, 未映射({})个",
                    fieldReferenceTracker.getTotalFields(),
                    fieldReferenceTracker.getReferencedFieldCount(),
                    fieldReferenceTracker.getUnreferencedFieldCount());

        } catch (Exception e) {
            logger.error("计算未映射字段失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 获取统计信息的简要描述
     *
     * @return 统计信息字符串
     */
    public String getStatisticsText() {
        return String.format(
                "直接映射: %d, 转换映射: %d, 默认值: %d, 缺失: %d, 未映射: %d",
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());
    }

    /** 获取统计信息 */
    public MappingStatistics getStatistics() {
        return new MappingStatistics(
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());
    }

    /** 字段映射数据模型 */
    public static class FieldMapping {
        private final String sourcePath; // 源字段路径，如 job.content[0].reader.parameter.username
        private final String targetField; // 目标字段名，如 source.Jdbc.user
        private final String value; // 字段值
        private final String description; // 映射说明
        private final MappingType type; // 映射类型

        public FieldMapping(
                String sourcePath,
                String targetField,
                String value,
                String description,
                MappingType type) {
            this.sourcePath = sourcePath;
            this.targetField = targetField;
            this.value = value;
            this.description = description;
            this.type = type;
        }

        // Getters
        public String getSourcePath() {
            return sourcePath;
        }

        public String getTargetField() {
            return targetField;
        }

        public String getValue() {
            return value;
        }

        public String getDescription() {
            return description;
        }

        public MappingType getType() {
            return type;
        }

        @Override
        public String toString() {
            return String.format(
                    "%s: %s -> %s = %s (%s)", type, sourcePath, targetField, value, description);
        }
    }

    /** 映射类型枚举 */
    public enum MappingType {
        DIRECT, // 直接映射
        TRANSFORM, // 转换映射（过滤器）
        DEFAULT, // 默认值
        MISSING, // 缺失字段
        UNMAPPED // 未映射字段
    }

    /** 映射统计信息 */
    public static class MappingStatistics {
        private final int directMappings;
        private final int transformMappings;
        private final int defaultValues;
        private final int missingFields;
        private final int unmappedFields;

        public MappingStatistics(
                int directMappings,
                int transformMappings,
                int defaultValues,
                int missingFields,
                int unmappedFields) {
            this.directMappings = directMappings;
            this.transformMappings = transformMappings;
            this.defaultValues = defaultValues;
            this.missingFields = missingFields;
            this.unmappedFields = unmappedFields;
        }

        public int getDirectMappings() {
            return directMappings;
        }

        public int getTransformMappings() {
            return transformMappings;
        }

        public int getDefaultValues() {
            return defaultValues;
        }

        public int getMissingFields() {
            return missingFields;
        }

        public int getUnmappedFields() {
            return unmappedFields;
        }

        public int getTotalFields() {
            return directMappings
                    + transformMappings
                    + defaultValues
                    + missingFields
                    + unmappedFields;
        }

        @Override
        public String toString() {
            return String.format(
                    "直接映射: %d, 转换映射: %d, 默认值: %d, 缺失: %d, 未映射: %d, 总计: %d",
                    directMappings,
                    transformMappings,
                    defaultValues,
                    missingFields,
                    unmappedFields,
                    getTotalFields());
        }
    }
}
