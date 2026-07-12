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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.E;

import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class VertexMapper implements GraphDataMapper {

    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final String labelId;
    private final HugeGraphClient client;
    private final Map<String, PropertyKey> propertyKeyCache;
    private final Set<String> propertySourceFields;

    public VertexMapper(
            MappingConfig mappingConfig, Map<String, Integer> fieldsIndex, HugeGraphClient client) {
        this.mappingConfig = mappingConfig;
        this.client = client;
        this.labelId = client.getVertexLabelId(mappingConfig.getLabel());
        this.fieldsIndex = fieldsIndex;
        this.propertySourceFields = resolvePropertySourceFields();
        this.propertyKeyCache = buildPropertyKeyCache();
    }

    private Set<String> resolvePropertySourceFields() {
        Set<String> fields = new HashSet<>();
        if (mappingConfig.getProperties().isEmpty()) {
            fields.addAll(fieldsIndex.keySet());
        } else {
            fields.addAll(mappingConfig.getProperties());
        }
        // PRIMARY_KEY idFields are always written as properties.
        if (mappingConfig.getIdStrategy() == IdStrategy.PRIMARY_KEY
                && mappingConfig.getIdFields() != null) {
            fields.addAll(mappingConfig.getIdFields());
        }
        return fields;
    }

    private HashMap<String, PropertyKey> buildPropertyKeyCache() {
        HashMap<String, PropertyKey> cache = new HashMap<>();
        Map<String, String> fm = mappingConfig.getFieldMapping();
        for (String sourceField : propertySourceFields) {
            String propName = fm.getOrDefault(sourceField, sourceField);
            if (!cache.containsKey(propName)) {
                cache.put(propName, client.getPropertyKey(propName));
            }
        }
        if (mappingConfig.getIdFields() != null) {
            for (String idField : mappingConfig.getIdFields()) {
                String propName = fm.getOrDefault(idField, idField);
                if (!cache.containsKey(propName)) {
                    PropertyKey propertyKey = client.getPropertyKeyOrNull(propName);
                    if (propertyKey != null) {
                        cache.put(propName, propertyKey);
                    }
                }
            }
        }
        return cache;
    }

    @Override
    public Vertex map(SeaTunnelRow row) {
        String label = mappingConfig.getLabel();
        E.checkArgument(label != null && !label.isEmpty(), "Vertex label can't be null or empty.");
        Vertex vertex = new Vertex(label);

        Object id = extractId(row);
        if (id == null && mappingConfig.getIdStrategy() != IdStrategy.AUTOMATIC) {
            return null;
        }

        if (id != null && mappingConfig.getIdStrategy() != IdStrategy.PRIMARY_KEY) {
            vertex.id(id);
        }

        Map<String, String> fm = mappingConfig.getFieldMapping();
        for (String sourceField : propertySourceFields) {
            Integer index = fieldsIndex.get(sourceField);
            if (index == null) {
                continue;
            }

            String propName = fm.getOrDefault(sourceField, sourceField);
            Object rawValue = row.getField(index);
            PropertyKey propertyKey = propertyKeyCache.get(propName);

            if (isConsideredNull(rawValue)) {
                continue;
            }

            Object converted =
                    DataTypeUtil.convert(
                            rawValue,
                            propertyKey,
                            mappingConfig.getDateFormat(),
                            mappingConfig.getTimeZone());
            vertex.property(propName, getMappedValue(converted));
        }

        return vertex;
    }

    @Override
    public Object extractId(SeaTunnelRow row) {
        IdStrategy strategy = mappingConfig.getIdStrategy();
        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

        List<String> idFields = mappingConfig.getIdFields();
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
        Map<String, String> fm = mappingConfig.getFieldMapping();
        for (String fieldName : fields) {
            Integer index = fieldsIndex.get(fieldName);
            if (index == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[VERTEX/%s]: Field '%s' specified in idFields not found in row schema. "
                                        + "Available fields: %s",
                                mappingConfig.getLabel(), fieldName, fieldsIndex.keySet()));
            }

            Object rawValue = row.getField(index);
            if (isConsideredNull(rawValue)) {
                continue;
            }

            String propName = fm.getOrDefault(fieldName, fieldName);
            PropertyKey propertyKey = propertyKeyCache.get(propName);

            Object converted = rawValue;
            if (propertyKey != null) {
                converted =
                        DataTypeUtil.convert(
                                rawValue,
                                propertyKey,
                                mappingConfig.getDateFormat(),
                                mappingConfig.getTimeZone());
            }
            values.add(getMappedValue(converted));
        }
        return values;
    }

    private boolean isConsideredNull(Object value) {
        if (value == null) {
            return true;
        }
        List<String> nullValues = mappingConfig.getNullValues();
        return !nullValues.isEmpty() && nullValues.contains(String.valueOf(value));
    }

    private Object getMappedValue(Object originalValue) {
        Map<Object, Object> vm = mappingConfig.getValueMapping();
        if (vm.isEmpty()) {
            return originalValue;
        }
        return vm.getOrDefault(originalValue, originalValue);
    }

    private String spliceVertexId(List<Object> primaryValues) {
        String joinedValues =
                primaryValues.stream().map(Object::toString).collect(Collectors.joining("!"));
        return String.format("%s:%s", labelId, joinedValues);
    }
}
