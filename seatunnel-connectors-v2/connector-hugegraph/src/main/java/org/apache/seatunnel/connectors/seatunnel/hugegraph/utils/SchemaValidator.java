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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates the connector configuration against the HugeGraph server schema. Validation is
 * per-mapping: only fields involved in each mapping are checked.
 */
public final class SchemaValidator {

    private final HugeGraphClient client;
    private final SeaTunnelRowType rowType;

    public SchemaValidator(HugeGraphClient client, SeaTunnelRowType rowType) {
        this.client = client;
        this.rowType = rowType;
    }

    public void validate(List<MappingConfig> mappings) {
        for (MappingConfig mapping : mappings) {
            validateMapping(mapping);
        }
    }

    private void validateMapping(MappingConfig mapping) {
        validateMappingConfig(mapping);
        if (mapping.getType() == LabelType.VERTEX) {
            validateVertexMapping(mapping);
        } else {
            validateEdgeMapping(mapping);
        }
    }

    private void validateMappingConfig(MappingConfig mapping) {
        E.checkNotNull(mapping.getType(), "type", "mapping");
        E.checkNotNull(mapping.getLabel(), "label", "mapping");

        if (mapping.getType() == LabelType.VERTEX) {
            E.checkNotNull(
                    mapping.getIdStrategy(),
                    "idStrategy",
                    String.format("mapping[VERTEX/%s]", mapping.getLabel()));
            if (mapping.getIdStrategy() != IdStrategy.AUTOMATIC) {
                E.checkNotEmpty(
                        mapping.getIdFields(),
                        "idFields",
                        String.format("mapping[VERTEX/%s]", mapping.getLabel()));
                validateSourceFields(mapping, mapping.getIdFields(), "idFields");
            }
        } else {
            E.checkNotNull(
                    mapping.getSourceConfig(),
                    "sourceConfig",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            E.checkNotNull(
                    mapping.getTargetConfig(),
                    "targetConfig",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            E.checkNotNull(
                    mapping.getSourceConfig().getLabel(),
                    "sourceConfig.label",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            E.checkNotNull(
                    mapping.getTargetConfig().getLabel(),
                    "targetConfig.label",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            E.checkNotEmpty(
                    mapping.getSourceConfig().getIdFields(),
                    "sourceConfig.idFields",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            E.checkNotEmpty(
                    mapping.getTargetConfig().getIdFields(),
                    "targetConfig.idFields",
                    String.format("mapping[EDGE/%s]", mapping.getLabel()));
            validateSourceFields(
                    mapping, mapping.getSourceConfig().getIdFields(), "sourceConfig.idFields");
            validateSourceFields(
                    mapping, mapping.getTargetConfig().getIdFields(), "targetConfig.idFields");

            if (mapping.getFrequency() == Frequency.MULTIPLE) {
                E.checkNotEmpty(
                        mapping.getSortKeys(),
                        "sortKeys",
                        String.format(
                                "mapping[EDGE/%s] with frequency=MULTIPLE", mapping.getLabel()));
                validateSourceFields(mapping, mapping.getSortKeys(), "sortKeys");
            }
        }
        validateSourceFields(mapping, mapping.getProperties(), "properties");
    }

    private void validateVertexMapping(MappingConfig mapping) {
        String label = mapping.getLabel();
        VertexLabel vertexLabel = client.getVertexLabel(label);
        if (vertexLabel.idStrategy() != mapping.getIdStrategy()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[VERTEX/%s]: idStrategy mismatch — server='%s', config='%s'",
                            label, vertexLabel.idStrategy(), mapping.getIdStrategy()));
        }
        if (mapping.getIdStrategy() == IdStrategy.PRIMARY_KEY) {
            List<String> configuredPrimaryKeys =
                    mapToTargetNames(mapping.getIdFields(), mapping.getFieldMapping());
            if (!vertexLabel.primaryKeys().equals(configuredPrimaryKeys)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[VERTEX/%s]: primary key mismatch — server='%s', config='%s'",
                                label, vertexLabel.primaryKeys(), configuredPrimaryKeys));
            }
        }

        Set<String> hgProperties = vertexLabel.properties();
        Set<String> targetProperties = resolveTargetProperties(mapping);

        // PRIMARY_KEY idFields are always included
        if (mapping.getIdStrategy() == IdStrategy.PRIMARY_KEY && mapping.getIdFields() != null) {
            Map<String, String> fm = mapping.getFieldMapping();
            for (String idField : mapping.getIdFields()) {
                targetProperties.add(fm.getOrDefault(idField, idField));
            }
        }

        for (String propName : targetProperties) {
            validateProperty(label, propName, hgProperties, mapping);
        }
    }

    private void validateEdgeMapping(MappingConfig mapping) {
        String label = mapping.getLabel();
        EdgeLabel edgeLabel = client.getEdgeLabel(label);
        Frequency configuredFrequency =
                mapping.getFrequency() == null ? Frequency.SINGLE : mapping.getFrequency();
        if (edgeLabel.frequency() != configuredFrequency) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: frequency mismatch — server='%s', config='%s'",
                            label, edgeLabel.frequency(), configuredFrequency));
        }
        List<String> configuredSortKeys =
                configuredFrequency == Frequency.MULTIPLE
                        ? mapToTargetNames(mapping.getSortKeys(), mapping.getFieldMapping())
                        : java.util.Collections.emptyList();
        if (!edgeLabel.sortKeys().equals(configuredSortKeys)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: sort key mismatch — server='%s', config='%s'",
                            label, edgeLabel.sortKeys(), configuredSortKeys));
        }

        // Validate source/target labels match
        if (!edgeLabel.sourceLabel().equals(mapping.getSourceConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: sourceLabel mismatch — server='%s', config='%s'",
                            label, edgeLabel.sourceLabel(), mapping.getSourceConfig().getLabel()));
        }
        if (!edgeLabel.targetLabel().equals(mapping.getTargetConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: targetLabel mismatch — server='%s', config='%s'",
                            label, edgeLabel.targetLabel(), mapping.getTargetConfig().getLabel()));
        }
        validateEndpointIdentity(mapping, mapping.getSourceConfig(), "sourceConfig");
        validateEndpointIdentity(mapping, mapping.getTargetConfig(), "targetConfig");

        Set<String> hgProperties = edgeLabel.properties();
        Set<String> targetProperties = resolveTargetProperties(mapping);

        // Edge source/target idFields are NOT edge properties (unless explicitly in properties)
        for (String propName : targetProperties) {
            validateProperty(label, propName, hgProperties, mapping);
        }
    }

    /**
     * Resolves the set of target property names for validation. Only includes fields listed in
     * mapping.properties (after fieldMapping transformation).
     */
    private Set<String> resolveTargetProperties(MappingConfig mapping) {
        Set<String> result = new HashSet<>();
        Map<String, String> fieldMapping = mapping.getFieldMapping();

        Set<String> sourceFields = new HashSet<>();
        if (mapping.getProperties().isEmpty()) {
            for (String fieldName : rowType.getFieldNames()) {
                sourceFields.add(fieldName);
            }
            if (mapping.getType() == LabelType.EDGE) {
                removeIdFields(sourceFields, mapping.getSourceConfig());
                removeIdFields(sourceFields, mapping.getTargetConfig());
            }
        } else {
            sourceFields.addAll(mapping.getProperties());
        }
        if (mapping.getType() == LabelType.EDGE) {
            sourceFields.addAll(mapping.getSortKeys());
        }

        for (String sourceField : sourceFields) {
            result.add(fieldMapping.getOrDefault(sourceField, sourceField));
        }
        return result;
    }

    private static void removeIdFields(
            Set<String> fields, MappingConfig.SourceTargetConfig sourceTargetConfig) {
        if (sourceTargetConfig != null && sourceTargetConfig.getIdFields() != null) {
            fields.removeAll(sourceTargetConfig.getIdFields());
        }
    }

    private void validateProperty(
            String label, String propName, Set<String> hgProperties, MappingConfig mapping) {
        if (!hgProperties.contains(propName)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[%s/%s]: Property '%s' does not exist in HugeGraph schema. "
                                    + "Available properties for label '%s': %s",
                            mapping.getType(), label, propName, label, hgProperties));
        }

        // Find source field to check type compatibility
        String sourceField = findSourceField(propName, mapping.getFieldMapping());
        int fieldIndex = findFieldIndex(sourceField);
        if (fieldIndex < 0) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[%s/%s]: Source field '%s' for property '%s' does not exist in input row",
                            mapping.getType(), label, sourceField, propName));
        }

        SeaTunnelDataType<?> seaType = rowType.getFieldType(fieldIndex);
        PropertyKey propertyKey = client.getPropertyKey(propName);
        DataType hgType = propertyKey.dataType();
        Cardinality cardinality = propertyKey.cardinality();

        if (!isCompatible(seaType, hgType, cardinality)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[%s/%s]: Type mismatch for property '%s' — "
                                    + "SeaTunnel type '%s' is not compatible with HugeGraph type '%s' (cardinality=%s).",
                            mapping.getType(), label, propName, seaType, hgType, cardinality));
        }
    }

    private boolean isCompatible(
            SeaTunnelDataType<?> seaTunnelType, DataType hugeGraphType, Cardinality cardinality) {
        switch (seaTunnelType.getSqlType()) {
            case BYTES:
                return hugeGraphType == DataType.BLOB;
            case TINYINT:
            case SMALLINT:
            case INT:
                return hugeGraphType == DataType.INT;
            case BIGINT:
                return hugeGraphType == DataType.LONG;
            case FLOAT:
                return hugeGraphType == DataType.FLOAT;
            case DOUBLE:
                return hugeGraphType == DataType.DOUBLE;
            case BOOLEAN:
                return hugeGraphType == DataType.BOOLEAN;
            case DATE:
            case TIMESTAMP:
                return hugeGraphType == DataType.DATE;
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelType).getElementType();
                if (cardinality != Cardinality.SINGLE) {
                    return isCompatible(elementType, hugeGraphType, Cardinality.LIST);
                } else {
                    return false;
                }
            case MAP:
            case DECIMAL:
            case ROW:
            case TIME:
            case NULL:
            case STRING:
                return hugeGraphType == DataType.TEXT;
            default:
                return false;
        }
    }

    private String findSourceField(String targetProp, Map<String, String> fieldMapping) {
        for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
            if (targetProp.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return targetProp;
    }

    private void validateEndpointIdentity(
            MappingConfig mapping, MappingConfig.SourceTargetConfig endpoint, String endpointName) {
        VertexLabel vertexLabel = client.getVertexLabel(endpoint.getLabel());
        IdStrategy idStrategy = vertexLabel.idStrategy();
        List<String> idFields = endpoint.getIdFields();
        if (idStrategy == IdStrategy.AUTOMATIC) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: %s label '%s' uses AUTOMATIC IDs, which cannot be reconstructed from input fields",
                            mapping.getLabel(), endpointName, endpoint.getLabel()));
        }
        if (idStrategy == IdStrategy.PRIMARY_KEY) {
            List<String> configuredPrimaryKeys =
                    mapToTargetNames(idFields, mapping.getFieldMapping());
            if (!vertexLabel.primaryKeys().equals(configuredPrimaryKeys)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[EDGE/%s]: %s primary key mismatch for label '%s' — server='%s', config='%s'",
                                mapping.getLabel(),
                                endpointName,
                                endpoint.getLabel(),
                                vertexLabel.primaryKeys(),
                                configuredPrimaryKeys));
            }
        } else if ((idStrategy == IdStrategy.CUSTOMIZE_NUMBER
                        || idStrategy == IdStrategy.CUSTOMIZE_UUID)
                && idFields.size() != 1) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Mapping[EDGE/%s]: %s for %s requires exactly one id field, but got %s",
                            mapping.getLabel(), endpointName, idStrategy, idFields.size()));
        }
    }

    private static List<String> mapToTargetNames(
            List<String> sourceFields, Map<String, String> fieldMapping) {
        return sourceFields.stream()
                .map(field -> fieldMapping.getOrDefault(field, field))
                .collect(java.util.stream.Collectors.toList());
    }

    private int findFieldIndex(String fieldName) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (rowType.getFieldName(i).equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private void validateSourceFields(
            MappingConfig mapping, List<String> sourceFields, String optionName) {
        if (sourceFields == null) {
            return;
        }
        for (String sourceField : sourceFields) {
            if (findFieldIndex(sourceField) < 0) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Mapping[%s/%s]: Field '%s' configured in '%s' does not exist in input row",
                                mapping.getType(), mapping.getLabel(), sourceField, optionName));
            }
        }
    }
}
