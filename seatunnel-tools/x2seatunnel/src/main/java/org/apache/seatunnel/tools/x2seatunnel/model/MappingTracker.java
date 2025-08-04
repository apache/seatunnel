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

/** Mapping tracker - records field mapping process for generating detailed conversion reports */
public class MappingTracker {

    private static final Logger logger = LoggerFactory.getLogger(MappingTracker.class);

    private final List<FieldMapping> directMappings = new ArrayList<>(); // Direct mappings
    private final List<FieldMapping> transformMappings =
            new ArrayList<>(); // Transform mappings (filters)
    private final List<FieldMapping> defaultValues = new ArrayList<>(); // Default values used
    private final List<FieldMapping> missingFields = new ArrayList<>(); // Missing fields
    private final List<FieldMapping> unmappedFields = new ArrayList<>(); // Unmapped fields

    /** Record successful direct mapping */
    public void recordDirectMapping(
            String sourcePath, String targetField, String value, String description) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, targetField, value, description, MappingType.DIRECT);
        directMappings.add(mapping);
        logger.debug("Recording direct mapping: {} -> {} = {}", sourcePath, targetField, value);
    }

    /** Record transform mapping fields (using filters) */
    public void recordTransformMapping(
            String sourcePath, String targetField, String value, String filterName) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, targetField, value, filterName, MappingType.TRANSFORM);
        transformMappings.add(mapping);
        logger.debug(
                "Recording transform mapping: {} -> {} = {} (filter: {})",
                sourcePath,
                targetField,
                value,
                filterName);
    }

    /** Record fields using default values */
    public void recordDefaultValue(String targetField, String value, String reason) {
        FieldMapping mapping =
                new FieldMapping(null, targetField, value, reason, MappingType.DEFAULT);
        defaultValues.add(mapping);
        logger.debug("Recording default value: {} = {} ({})", targetField, value, reason);
    }

    /** Record missing required fields */
    public void recordMissingField(String sourcePath, String reason) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, null, null, reason, MappingType.MISSING);
        missingFields.add(mapping);
        logger.debug("Recording missing field: {} ({})", sourcePath, reason);
    }

    /** Record unmapped fields */
    public void recordUnmappedField(String sourcePath, String value, String reason) {
        FieldMapping mapping =
                new FieldMapping(sourcePath, null, value, reason, MappingType.UNMAPPED);
        unmappedFields.add(mapping);
        logger.debug("Recording unmapped field: {} = {} ({})", sourcePath, value, reason);
    }

    /** Generate complete mapping result */
    public MappingResult generateMappingResult() {
        MappingResult result = new MappingResult();

        // Convert direct mappings
        for (FieldMapping mapping : directMappings) {
            result.addSuccessMapping(
                    mapping.getSourcePath(), mapping.getTargetField(), mapping.getValue());
        }

        // Convert transform mapping fields
        for (FieldMapping mapping : transformMappings) {
            result.addTransformMapping(
                    mapping.getSourcePath(),
                    mapping.getTargetField(),
                    mapping.getValue(),
                    mapping.getDescription());
        }

        // Convert default value fields - separate category
        for (FieldMapping mapping : defaultValues) {
            result.addDefaultValueField(
                    mapping.getTargetField(), mapping.getValue(), mapping.getDescription());
        }

        // Convert missing fields
        for (FieldMapping mapping : missingFields) {
            result.addMissingRequiredField(mapping.getSourcePath(), mapping.getDescription());
        }

        // Convert unmapped fields
        for (FieldMapping mapping : unmappedFields) {
            result.addUnmappedField(
                    mapping.getSourcePath(), mapping.getValue(), mapping.getDescription());
        }

        result.setSuccess(true);

        logger.info(
                "Mapping tracking completed: direct mappings({}), transform mappings({}), default values({}), missing({}), unmapped({})",
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());

        return result;
    }

    /** Reset mapping tracker state for new conversion process */
    public void reset() {
        directMappings.clear();
        transformMappings.clear();
        defaultValues.clear();
        missingFields.clear();
        unmappedFields.clear();
        logger.info("Mapping tracker has been reset");
    }

    /**
     * Calculate and record unmapped fields based on field reference tracker
     *
     * @param fieldReferenceTracker field reference tracker
     */
    public void calculateUnmappedFieldsFromTracker(
            DataXFieldExtractor.FieldReferenceTracker fieldReferenceTracker) {
        try {
            if (fieldReferenceTracker == null) {
                logger.warn("Field reference tracker is null, skipping unmapped field calculation");
                return;
            }

            // Get unreferenced fields
            Map<String, String> unreferencedFields = fieldReferenceTracker.getUnreferencedFields();

            // Record unmapped fields (with actual values)
            for (Map.Entry<String, String> entry : unreferencedFields.entrySet()) {
                String fieldPath = entry.getKey();
                String actualValue = entry.getValue();
                recordUnmappedField(
                        fieldPath, actualValue, "Exists in DataX but not referenced in template");
            }

            logger.info(
                    "Unmapped field calculation completed: total fields({}), referenced({}), unmapped({})",
                    fieldReferenceTracker.getTotalFields(),
                    fieldReferenceTracker.getReferencedFieldCount(),
                    fieldReferenceTracker.getUnreferencedFieldCount());

        } catch (Exception e) {
            logger.error("Failed to calculate unmapped fields: {}", e.getMessage(), e);
        }
    }

    /**
     * Get brief description of statistics
     *
     * @return statistics string
     */
    public String getStatisticsText() {
        return String.format(
                "Direct mappings: %d, Transform mappings: %d, Default values: %d, Missing: %d, Unmapped: %d",
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());
    }

    /** Get statistics */
    public MappingStatistics getStatistics() {
        return new MappingStatistics(
                directMappings.size(),
                transformMappings.size(),
                defaultValues.size(),
                missingFields.size(),
                unmappedFields.size());
    }

    /** Field mapping data model */
    public static class FieldMapping {
        private final String
                sourcePath; // Source field path, e.g. job.content[0].reader.parameter.username
        private final String targetField; // Target field name, e.g. source.Jdbc.user
        private final String value; // Field value
        private final String description; // Mapping description
        private final MappingType type; // Mapping type

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

    /** Mapping type enumeration */
    public enum MappingType {
        DIRECT, // Direct mapping
        TRANSFORM, // Transform mapping (filters)
        DEFAULT, // Default value
        MISSING, // Missing field
        UNMAPPED // Unmapped field
    }

    /** Mapping statistics */
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
                    "Direct mappings: %d, Transform mappings: %d, Default values: %d, Missing: %d, Unmapped: %d, Total: %d",
                    directMappings,
                    transformMappings,
                    defaultValues,
                    missingFields,
                    unmappedFields,
                    getTotalFields());
        }
    }
}
