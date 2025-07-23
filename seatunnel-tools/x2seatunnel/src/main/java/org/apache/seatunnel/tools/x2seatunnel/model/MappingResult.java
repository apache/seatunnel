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

import java.util.ArrayList;
import java.util.List;

/** 映射结果数据模型 */
public class MappingResult {

    private boolean success = false;
    private String errorMessage;
    private SeaTunnelConfig seaTunnelConfig;

    // 基本信息
    private String sourceTemplate;
    private String sinkTemplate;
    private String readerType;
    private String writerType;

    // 映射结果统计
    private List<MappingItem> successMappings = new ArrayList<>();
    private List<TransformMapping> transformMappings = new ArrayList<>(); // 新增：转换映射字段
    private List<DefaultValueField> defaultValues = new ArrayList<>(); // 新增：默认值字段
    private List<MissingField> missingRequiredFields = new ArrayList<>();
    private List<UnmappedField> unmappedFields = new ArrayList<>();

    /** 成功映射的字段 */
    public static class MappingItem {
        private String sourceField;
        private String targetField;
        private String value;

        public MappingItem(String sourceField, String targetField, String value) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.value = value;
        }

        // Getters
        public String getSourceField() {
            return sourceField;
        }

        public String getTargetField() {
            return targetField;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return sourceField + " -> " + targetField + " = " + value;
        }
    }

    /** 转换映射的字段（使用了过滤器） */
    public static class TransformMapping {
        private String sourceField;
        private String targetField;
        private String value;
        private String filterName;

        public TransformMapping(
                String sourceField, String targetField, String value, String filterName) {
            this.sourceField = sourceField;
            this.targetField = targetField;
            this.value = value;
            this.filterName = filterName;
        }

        // Getters
        public String getSourceField() {
            return sourceField;
        }

        public String getTargetField() {
            return targetField;
        }

        public String getValue() {
            return value;
        }

        public String getFilterName() {
            return filterName;
        }

        @Override
        public String toString() {
            return sourceField
                    + " -> "
                    + targetField
                    + " = "
                    + value
                    + " (过滤器: "
                    + filterName
                    + ")";
        }
    }

    /** 使用默认值的字段 */
    public static class DefaultValueField {
        private String fieldName;
        private String value;
        private String reason;

        public DefaultValueField(String fieldName, String value, String reason) {
            this.fieldName = fieldName;
            this.value = value;
            this.reason = reason;
        }

        // Getters
        public String getFieldName() {
            return fieldName;
        }

        public String getValue() {
            return value;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return fieldName + " = " + value + " (默认值: " + reason + ")";
        }
    }

    /** 缺失的必填字段 */
    public static class MissingField {
        private String fieldName;
        private String reason;

        public MissingField(String fieldName, String reason) {
            this.fieldName = fieldName;
            this.reason = reason;
        }

        // Getters
        public String getFieldName() {
            return fieldName;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return fieldName + " (原因: " + reason + ")";
        }
    }

    /** 未映射的字段 */
    public static class UnmappedField {
        private String fieldName;
        private String value;
        private String reason;

        public UnmappedField(String fieldName, String value, String reason) {
            this.fieldName = fieldName;
            this.value = value;
            this.reason = reason;
        }

        // Getters
        public String getFieldName() {
            return fieldName;
        }

        public String getValue() {
            return value;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return fieldName + " = " + value + " (原因: " + reason + ")";
        }
    }

    // 添加映射结果的便捷方法
    public void addSuccessMapping(String sourceField, String targetField, String value) {
        successMappings.add(new MappingItem(sourceField, targetField, value));
    }

    public void addTransformMapping(
            String sourceField, String targetField, String value, String filterName) {
        transformMappings.add(new TransformMapping(sourceField, targetField, value, filterName));
    }

    public void addDefaultValueField(String fieldName, String value, String reason) {
        defaultValues.add(new DefaultValueField(fieldName, value, reason));
    }

    public void addMissingRequiredField(String fieldName, String reason) {
        missingRequiredFields.add(new MissingField(fieldName, reason));
    }

    public void addUnmappedField(String fieldName, String value, String reason) {
        unmappedFields.add(new UnmappedField(fieldName, value, reason));
    }

    // Getter and Setter methods
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public SeaTunnelConfig getSeaTunnelConfig() {
        return seaTunnelConfig;
    }

    public void setSeaTunnelConfig(SeaTunnelConfig seaTunnelConfig) {
        this.seaTunnelConfig = seaTunnelConfig;
    }

    public String getSourceTemplate() {
        return sourceTemplate;
    }

    public void setSourceTemplate(String sourceTemplate) {
        this.sourceTemplate = sourceTemplate;
    }

    public String getSinkTemplate() {
        return sinkTemplate;
    }

    public void setSinkTemplate(String sinkTemplate) {
        this.sinkTemplate = sinkTemplate;
    }

    public String getReaderType() {
        return readerType;
    }

    public void setReaderType(String readerType) {
        this.readerType = readerType;
    }

    public String getWriterType() {
        return writerType;
    }

    public void setWriterType(String writerType) {
        this.writerType = writerType;
    }

    public List<MappingItem> getSuccessMappings() {
        return successMappings;
    }

    public List<TransformMapping> getTransformMappings() {
        return transformMappings;
    }

    public List<DefaultValueField> getDefaultValues() {
        return defaultValues;
    }

    public List<MissingField> getMissingRequiredFields() {
        return missingRequiredFields;
    }

    public List<UnmappedField> getUnmappedFields() {
        return unmappedFields;
    }

    @Override
    public String toString() {
        return "MappingResult{"
                + "success="
                + success
                + ", successMappings="
                + successMappings.size()
                + ", transformMappings="
                + transformMappings.size()
                + ", defaultValues="
                + defaultValues.size()
                + ", missingRequiredFields="
                + missingRequiredFields.size()
                + ", unmappedFields="
                + unmappedFields.size()
                + '}';
    }
}
