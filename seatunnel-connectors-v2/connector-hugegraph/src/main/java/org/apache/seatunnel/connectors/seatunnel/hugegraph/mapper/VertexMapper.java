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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.E;

import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class VertexMapper implements GraphDataMapper {

    private final SchemaConfig schemaConfig;
    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final String labelId;
    private final HugeGraphClient client;
    private final Map<String, PropertyKey> propertyKeyCache;

    public VertexMapper(
            SchemaConfig schemaConfig, Map<String, Integer> fieldsIndex, HugeGraphClient client) {
        this.schemaConfig = schemaConfig;
        this.mappingConfig = getMappingConfig();
        this.client = client;
        this.labelId = client.getVertexLabelId(schemaConfig.getLabel());
        this.fieldsIndex = fieldsIndex;
        this.propertyKeyCache = getPropertyKeyCache();
    }

    private MappingConfig getMappingConfig() {
        MappingConfig mapping =
                schemaConfig.getMapping() == null ? new MappingConfig() : schemaConfig.getMapping();
        if (mapping.getFieldMapping() == null) {
            mapping.setFieldMapping(Collections.emptyMap());
        }
        if (mapping.getValueMapping() == null) {
            mapping.setValueMapping(Collections.emptyMap());
        }
        schemaConfig.setMapping(mapping);
        return mapping;
    }

    private HashMap<String, PropertyKey> getPropertyKeyCache() {
        HashMap<String, PropertyKey> cache = new HashMap<>();
        Map<String, String> fieldMapping = mappingConfig.getFieldMapping();
        for (String fieldName : fieldsIndex.keySet()) {
            String propertyName = fieldMapping.getOrDefault(fieldName, fieldName);
            cache.put(propertyName, client.getPropertyKey(propertyName));
        }
        return cache;
    }

    @Override
    public Vertex map(SeaTunnelRow row) {
        String label = schemaConfig.getLabel();
        E.checkArgument(label != null && !label.isEmpty(), "Vertex label can't be null or empty.");
        Vertex vertex = new Vertex(label);

        // 1. Set vertex ID
        Object id = extractId(row);
        if (id == null && schemaConfig.getIdStrategy() != IdStrategy.AUTOMATIC) {
            return null;
        }

        if (id != null && schemaConfig.getIdStrategy() != IdStrategy.PRIMARY_KEY) {
            vertex.id(id);
        }

        // 2. Set properties
        Map<String, String> fieldMapping = mappingConfig.getFieldMapping();

        for (Map.Entry<String, Integer> fieldEntry : fieldsIndex.entrySet()) {

            String fieldName = fieldEntry.getKey();
            String propertyName = fieldMapping.getOrDefault(fieldName, fieldName);
            Object rawValue = row.getField(fieldEntry.getValue());
            PropertyKey propertyKey = propertyKeyCache.get(propertyName);

            if (isConsideredNull(rawValue)) {
                continue;
            }

            Object fieldValue =
                    DataTypeUtil.convert(
                            rawValue,
                            propertyKey,
                            mappingConfig.getDateFormat(),
                            mappingConfig.getTimeZone());

            vertex.property(propertyName, getMappedValue(fieldValue));
        }

        return vertex;
    }

    @Override
    public Object extractId(SeaTunnelRow row) {
        IdStrategy strategy = schemaConfig.getIdStrategy();
        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

        List<String> idFields = schemaConfig.getIdFields();
        E.checkArgument(
                idFields != null && !idFields.isEmpty(),
                "The 'idFields' must be specified for ID strategy '%s'.",
                strategy);

        switch (strategy) {
            case PRIMARY_KEY:
                List<Object> pkValues = getFieldValues(row, idFields);
                if (pkValues.size() != idFields.size()
                        || pkValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return spliceVertexId(pkValues);
            case CUSTOMIZE_STRING:
                List<Object> stringValues = getFieldValues(row, idFields);
                if (stringValues.size() != idFields.size()
                        || stringValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return stringValues.stream().map(String::valueOf).collect(Collectors.joining(":"));
            case CUSTOMIZE_NUMBER:
                List<Object> numberValues = getFieldValues(row, idFields);
                if (numberValues.size() != 1) {
                    return null;
                }
                Object numValue = numberValues.get(0);
                if (isConsideredNull(numValue)) {
                    return null;
                }
                if (numValue instanceof Number) {
                    return ((Number) numValue).longValue();
                } else {
                    return Long.parseLong(String.valueOf(numValue));
                }
            case CUSTOMIZE_UUID:
                List<Object> uuidValues = getFieldValues(row, idFields);
                if (uuidValues.size() != 1) {
                    return null;
                }
                Object uuidValue = uuidValues.get(0);
                if (isConsideredNull(uuidValue)) {
                    return null;
                }
                return UUID.fromString(String.valueOf(uuidValue));
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        "Unsupported IdStrategy: " + strategy);
        }
    }

    private List<Object> getFieldValues(SeaTunnelRow row, List<String> fields) {
        List<Object> values = new ArrayList<>(fields.size());
        Map<String, String> fieldMapping = mappingConfig.getFieldMapping();
        for (String fieldName : fields) {

            Integer index = fieldsIndex.get(fieldName);
            if (index == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Field '%s' specified in id_fields not found in row schema. Available fields: %s",
                                fieldName, fieldsIndex.keySet()));
            }

            Object rawValue = row.getField(index);
            if (isConsideredNull(rawValue)) {
                continue;
            }

            String propertyName = fieldMapping.getOrDefault(fieldName, fieldName);
            PropertyKey propertyKey = propertyKeyCache.get(propertyName);

            Object fieldValue =
                    DataTypeUtil.convert(
                            rawValue,
                            propertyKey,
                            mappingConfig.getDateFormat(),
                            mappingConfig.getTimeZone());

            values.add(getMappedValue(fieldValue));
        }
        return values;
    }

    private boolean isConsideredNull(Object value) {
        if (value == null) {
            return true;
        }
        List<String> nullValues = mappingConfig.getNullValues();
        if (nullValues == null || nullValues.isEmpty()) {
            return false;
        }
        return nullValues.contains(String.valueOf(value));
    }

    private Object getMappedValue(Object originalValue) {
        Map<Object, Object> valueMapping = mappingConfig.getValueMapping();
        if (valueMapping.isEmpty()) {
            return originalValue;
        }
        return valueMapping.getOrDefault(originalValue, originalValue);
    }

    private String spliceVertexId(List<Object> primaryValues) {
        String joinedValues =
                primaryValues.stream().map(Object::toString).collect(Collectors.joining("!"));
        return String.format("%s:%s", labelId, joinedValues);
    }
}
