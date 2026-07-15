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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;

import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Data
public class MappingConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // Element type
    private LabelType type;
    private String label;

    // Vertex-specific
    private IdStrategy idStrategy;
    private List<String> idFields;

    // Edge-specific
    private SourceTargetConfig sourceConfig;
    private SourceTargetConfig targetConfig;
    private Frequency frequency;
    private List<String> sortKeys;

    // Property config
    private List<String> properties;

    // Field mapping (source field name → target property name)
    private Map<String, String> fieldMapping;
    // Per-field value mapping: outer key = source field name, inner map = rawValue -> mappedValue.
    // Scoping by field prevents one column's rule from bleeding into another (e.g. gender M->male
    // must not also rewrite status M).
    private Map<String, Map<Object, Object>> valueMapping;
    private List<String> nullableKeys;
    private List<String> notNullableKeys;
    private List<String> nullValues;

    // Time config
    private String dateFormat;
    private String timeZone;

    // Label metadata (for schema creation)
    private Long ttl;
    private String ttlStartTime;
    private String enableLabelIndex;
    private Map<String, Object> userdata;

    public enum LabelType {
        VERTEX,
        EDGE
    }

    @Data
    public static class SourceTargetConfig implements Serializable {

        private static final long serialVersionUID = 1L;
        private String label;
        private List<String> idFields;
    }

    public Map<String, String> getFieldMapping() {
        return fieldMapping == null ? Collections.emptyMap() : fieldMapping;
    }

    public Map<String, Map<Object, Object>> getValueMapping() {
        return valueMapping == null ? Collections.emptyMap() : valueMapping;
    }

    public List<String> getNullValues() {
        return nullValues == null ? Collections.emptyList() : nullValues;
    }

    public List<String> getNullableKeys() {
        return nullableKeys == null ? Collections.emptyList() : nullableKeys;
    }

    public List<String> getNotNullableKeys() {
        return notNullableKeys == null ? Collections.emptyList() : notNullableKeys;
    }

    public List<String> getSortKeys() {
        return sortKeys == null ? Collections.emptyList() : sortKeys;
    }

    public List<String> getProperties() {
        return properties == null ? Collections.emptyList() : properties;
    }

    /** Converts a legacy SchemaConfig to the new unified MappingConfig. */
    public static MappingConfig fromLegacySchemaConfig(SchemaConfig schema) {
        MappingConfig config = new MappingConfig();

        // Element type & label
        if (schema.getType() != null) {
            config.setType(LabelType.valueOf(schema.getType().name()));
        }
        config.setLabel(schema.getLabel());

        // Vertex config
        config.setIdStrategy(schema.getIdStrategy());
        config.setIdFields(schema.getIdFields());

        // Edge config
        if (schema.getSourceConfig() != null) {
            SourceTargetConfig src = new SourceTargetConfig();
            src.setLabel(schema.getSourceConfig().getLabel());
            src.setIdFields(schema.getSourceConfig().getIdFields());
            config.setSourceConfig(src);
        }
        if (schema.getTargetConfig() != null) {
            SourceTargetConfig tgt = new SourceTargetConfig();
            tgt.setLabel(schema.getTargetConfig().getLabel());
            tgt.setIdFields(schema.getTargetConfig().getIdFields());
            config.setTargetConfig(tgt);
        }
        config.setFrequency(schema.getFrequency());

        // Properties
        config.setProperties(schema.getProperties());

        // Label metadata
        config.setTtl(schema.getTtl());
        config.setTtlStartTime(schema.getTtlStartTime());
        config.setEnableLabelIndex(schema.getEnableLabelIndex());
        config.setUserdata(schema.getUserdata());

        // Flatten the nested mapping config
        if (schema.getMapping() != null) {
            MappingConfig legacy = schema.getMapping();
            config.setFieldMapping(legacy.getFieldMapping());
            config.setValueMapping(legacy.getValueMapping());
            config.setNullableKeys(legacy.getNullableKeys());
            config.setNullValues(legacy.getNullValues());
            config.setSortKeys(legacy.getSortKeys());
            config.setDateFormat(legacy.getDateFormat());
            config.setTimeZone(legacy.getTimeZone());
        }

        return config;
    }
}
