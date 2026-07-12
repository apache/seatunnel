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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.SourceTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;

import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class EdgeMapper implements GraphDataMapper {

    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final HugeGraphClient client;
    private final String labelId;
    private final Map<String, PropertyKey> propertyKeyCache;
    private final Set<String> propertySourceFields;
    private final Set<String> edgeIdSourceFields;

    // Cached at construction time to avoid per-row schema queries
    private final String sourceVertexLabelId;
    private final IdStrategy sourceIdStrategy;
    private final String targetVertexLabelId;
    private final IdStrategy targetIdStrategy;

    public EdgeMapper(
            MappingConfig mappingConfig, Map<String, Integer> fieldsIndex, HugeGraphClient client) {
        this.mappingConfig = mappingConfig;
        this.client = client;
        this.labelId = client.getEdgeLabelId(mappingConfig.getLabel());
        this.fieldsIndex = fieldsIndex;
        this.edgeIdSourceFields = resolveEdgeIdSourceFields();
        this.propertySourceFields = resolvePropertySourceFields();
        this.propertyKeyCache = buildPropertyKeyCache();

        // Cache source/target vertex metadata to avoid per-row schema queries
        this.sourceVertexLabelId =
                client.getVertexLabelId(mappingConfig.getSourceConfig().getLabel());
        this.sourceIdStrategy = client.getIdStrategy(mappingConfig.getSourceConfig().getLabel());
        this.targetVertexLabelId =
                client.getVertexLabelId(mappingConfig.getTargetConfig().getLabel());
        this.targetIdStrategy = client.getIdStrategy(mappingConfig.getTargetConfig().getLabel());
    }

    private Set<String> resolveEdgeIdSourceFields() {
        Set<String> fields = new HashSet<>();
        if (mappingConfig.getSourceConfig() != null
                && mappingConfig.getSourceConfig().getIdFields() != null) {
            fields.addAll(mappingConfig.getSourceConfig().getIdFields());
        }
        if (mappingConfig.getTargetConfig() != null
                && mappingConfig.getTargetConfig().getIdFields() != null) {
            fields.addAll(mappingConfig.getTargetConfig().getIdFields());
        }
        return fields;
    }

    private Set<String> resolvePropertySourceFields() {
        Set<String> fields = new HashSet<>();
        if (mappingConfig.getProperties().isEmpty()) {
            fields.addAll(fieldsIndex.keySet());
        } else {
            fields.addAll(mappingConfig.getProperties());
        }
        // Endpoint ID fields are never edge properties — they only locate the source/target
        // vertices. Sort keys, however, ARE edge properties and must be retained.
        fields.removeAll(edgeIdSourceFields);
        fields.addAll(mappingConfig.getSortKeys());
        return fields;
    }

    private HashMap<String, PropertyKey> buildPropertyKeyCache() {
        HashMap<String, PropertyKey> cache = new HashMap<>();
        Map<String, String> fm = mappingConfig.getFieldMapping();

        // Cache for property fields
        for (String sourceField : propertySourceFields) {
            String propName = fm.getOrDefault(sourceField, sourceField);
            if (!cache.containsKey(propName)) {
                cache.put(propName, client.getPropertyKey(propName));
            }
        }

        // Cache for id fields (needed for type conversion during ID construction)
        for (String idField : edgeIdSourceFields) {
            String propName = fm.getOrDefault(idField, idField);
            if (!cache.containsKey(propName)) {
                PropertyKey pk = client.getPropertyKeyOrNull(propName);
                if (pk != null) {
                    cache.put(propName, pk);
                }
            }
        }

        return cache;
    }

    @Override
    public Edge map(SeaTunnelRow row) {
        Object sourceId = buildVertexId(row, mappingConfig.getSourceConfig());
        Object targetId = buildVertexId(row, mappingConfig.getTargetConfig());

        if (sourceId == null || targetId == null) {
            return null;
        }

        Edge edge = new Edge(mappingConfig.getLabel());
        edge.sourceId(sourceId);
        edge.targetId(targetId);
        edge.sourceLabel(mappingConfig.getSourceConfig().getLabel());
        edge.targetLabel(mappingConfig.getTargetConfig().getLabel());

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
            edge.property(propName, getMappedValue(converted));
        }
        return edge;
    }

    private Object buildVertexId(SeaTunnelRow row, SourceTargetConfig config) {
        boolean isSource = config == mappingConfig.getSourceConfig();
        String vertexLabelId = isSource ? sourceVertexLabelId : targetVertexLabelId;
        IdStrategy strategy = isSource ? sourceIdStrategy : targetIdStrategy;
        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

        List<String> idFields = config.getIdFields();
        switch (strategy) {
            case PRIMARY_KEY:
                List<Object> pkValues = getFieldValues(row, idFields);
                if (pkValues.size() != idFields.size()
                        || pkValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return spliceVertexId(vertexLabelId, pkValues);
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
                                "Mapping[EDGE/%s]: Field '%s' specified in idFields not found in row schema. "
                                        + "Available fields: %s",
                                mappingConfig.getLabel(), fieldName, fieldsIndex.keySet()));
            }

            Object rawValue = row.getField(index);
            if (isConsideredNull(rawValue)) {
                continue;
            }

            String propName = fm.getOrDefault(fieldName, fieldName);
            PropertyKey propertyKey = propertyKeyCache.get(propName);

            if (propertyKey != null) {
                Object converted =
                        DataTypeUtil.convert(
                                rawValue,
                                propertyKey,
                                mappingConfig.getDateFormat(),
                                mappingConfig.getTimeZone());
                values.add(getMappedValue(converted));
            } else {
                values.add(getMappedValue(rawValue));
            }
        }
        return values;
    }

    /**
     * Extracts the Edge ID matching the HugeGraph server-side 5-part EdgeId format:
     * {ownerVertexId}>{edgeLabelId}>{subEdgeLabelId}>{sortValues}>{otherVertexId}
     *
     * <p>For general (non-hierarchical) edge labels the sub-label ID equals the edge label ID, so
     * the label ID appears twice. SINGLE frequency edges have an empty sortValues segment; MULTIPLE
     * frequency uses the sortKeys values. Vertex IDs are prefixed with 'S' for String IDs and 'L'
     * for Number IDs.
     */
    @Override
    public Object extractId(SeaTunnelRow row) {
        Object sourceId = buildVertexId(row, mappingConfig.getSourceConfig());
        Object targetId = buildVertexId(row, mappingConfig.getTargetConfig());

        if (sourceId == null || targetId == null) {
            return null;
        }

        return spliceEdgeId(sourceId, targetId, labelId, getSortKeyValues(row));
    }

    /**
     * Splices the HugeGraph server-side 5-part EdgeId. Package-private and static so the
     * format-sensitive layout (S/L prefix, doubled label id for the sub-label segment, sortValues
     * segment) is unit-testable without a live server — this is the DELETE-correctness path.
     */
    static String spliceEdgeId(
            Object sourceId, Object targetId, String labelId, String sortValues) {
        String sourcePrefix = (sourceId instanceof Number) ? "L" : "S";
        String targetPrefix = (targetId instanceof Number) ? "L" : "S";
        return String.format(
                "%s%s>%s>%s>%s>%s%s",
                sourcePrefix, sourceId, labelId, labelId, sortValues, targetPrefix, targetId);
    }

    private String getSortKeyValues(SeaTunnelRow row) {
        Frequency frequency = mappingConfig.getFrequency();
        if (frequency == null || frequency == Frequency.SINGLE) {
            return "";
        }
        List<String> sortKeys = mappingConfig.getSortKeys();
        if (sortKeys.isEmpty()) {
            return "";
        }
        List<Object> skValues = getFieldValues(row, sortKeys);
        return skValues.stream().map(Object::toString).collect(Collectors.joining("!"));
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

    private String spliceVertexId(String vertexLabelId, List<Object> primaryValues) {
        String joinedValues =
                primaryValues.stream().map(Object::toString).collect(Collectors.joining("!"));
        return String.format("%s:%s", vertexLabelId, joinedValues);
    }
}
