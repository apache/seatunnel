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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ReservedColumns;
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

    /**
     * Runs only the config-level checks that do not touch the server, so a job with a malformed
     * mapping fails before any schema is persisted to HugeGraph. HugeGraph label DDL is
     * non-transactional and its primary keys / sort keys / frequency are effectively immutable, so
     * persisting a property key or vertex label and then failing config validation would leave a
     * schema fragment the user cannot fix in place.
     */
    public void validateConfigOnly(List<MappingConfig> mappings) {
        for (MappingConfig mapping : mappings) {
            validateMappingConfig(mapping);
        }
    }

    /**
     * Fails fast — BEFORE any schema is created — when a mapping targets a label that already
     * exists on the server with incompatible immutable attributes (vertex id strategy / primary
     * keys, edge frequency / sort keys / endpoints). HugeGraph cannot ALTER these attributes, and
     * {@code ensureSchema} creates the PropertyKeys and labels for the <em>other</em> mappings
     * first — so catching such a mismatch only in the post-create {@link #validate} would leave
     * those creations behind as schema pollution and trap the user in a retry loop that never
     * reconciles. Running this read-only check up front means an incompatible pre-existing label
     * aborts the job with zero new writes; the fix is to drop that label on the server and re-run.
     *
     * <p>Only labels that already exist are inspected; missing ones are left for {@code
     * ensureSchema} to create. Edge endpoint identity is intentionally not checked here (the
     * endpoint vertex labels may not exist yet under CREATE_SCHEMA_WHEN_NOT_EXIST); it is verified
     * afterwards by {@link #validate}.
     */
    public void validateExistingLabels(List<MappingConfig> mappings) {
        for (MappingConfig mapping : mappings) {
            if (mapping.getType() == LabelType.VERTEX) {
                if (client.getVertexLabelOrNull(mapping.getLabel()) != null) {
                    validateVertexMapping(mapping);
                }
            } else {
                EdgeLabel existing = client.getEdgeLabelOrNull(mapping.getLabel());
                if (existing != null) {
                    validateExistingEdgeLabel(mapping, existing);
                }
            }
        }
    }

    /**
     * Checks an already-existing EdgeLabel's immutable attributes (frequency, sort keys, source /
     * target labels) against the config. Deliberately omits the endpoint-vertex identity checks
     * that {@link #validateEdgeMapping} performs, because those require the endpoint labels to
     * already exist — which is not guaranteed before {@code ensureSchema} runs.
     */
    private void validateExistingEdgeLabel(MappingConfig mapping, EdgeLabel edgeLabel) {
        String label = mapping.getLabel();
        Frequency configuredFrequency =
                mapping.getFrequency() == null ? Frequency.SINGLE : mapping.getFrequency();
        if (edgeLabel.frequency() != configuredFrequency) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: frequency mismatch — server='%s', config='%s'. The "
                                    + "EdgeLabel already exists with an immutable frequency; drop it "
                                    + "on the server before re-running.",
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
                            "Mapping[EDGE/%s]: sort key mismatch — server='%s', config='%s'. The "
                                    + "EdgeLabel already exists with immutable sort keys; drop it on "
                                    + "the server before re-running.",
                            label, edgeLabel.sortKeys(), configuredSortKeys));
        }
        if (!edgeLabel.sourceLabel().equals(mapping.getSourceConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: sourceLabel mismatch — server='%s', config='%s'. The "
                                    + "EdgeLabel already exists; drop it on the server before re-running.",
                            label, edgeLabel.sourceLabel(), mapping.getSourceConfig().getLabel()));
        }
        if (!edgeLabel.targetLabel().equals(mapping.getTargetConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Mapping[EDGE/%s]: targetLabel mismatch — server='%s', config='%s'. The "
                                    + "EdgeLabel already exists; drop it on the server before re-running.",
                            label, edgeLabel.targetLabel(), mapping.getTargetConfig().getLabel()));
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

        // nullableKeys and notNullableKeys are two opposite ways to steer nullability of an
        // auto-created label. Setting both is ambiguous — notNullableKeys is silently ignored once
        // an explicit nullableKeys allow-list is present — so reject it up front instead.
        if (!mapping.getNullableKeys().isEmpty() && !mapping.getNotNullableKeys().isEmpty()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Mapping[%s/%s]: 'nullableKeys' and 'notNullableKeys' are mutually "
                                    + "exclusive — set at most one.",
                            mapping.getType(), mapping.getLabel()));
        }

        // `properties` (selected whitelist) and `ignored` (blacklist) are opposite ways to choose
        // the property set; setting both is ambiguous.
        if (!mapping.getProperties().isEmpty() && !mapping.getIgnored().isEmpty()) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Mapping[%s/%s]: 'properties' (selected) and 'ignored' are mutually "
                                    + "exclusive — set at most one.",
                            mapping.getType(), mapping.getLabel()));
        }
        validateSourceFields(mapping, mapping.getIgnored(), "ignored");

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
                // A vertex that reuses the reserved ~id column supplies the id externally, which
                // only CUSTOMIZE_* strategies accept. PRIMARY_KEY derives its id from property
                // values (use those columns instead) and AUTOMATIC is server-assigned.
                if (ReservedColumns.isRawIdPassthrough(mapping.getIdFields())
                        && mapping.getIdStrategy() != IdStrategy.CUSTOMIZE_STRING
                        && mapping.getIdStrategy() != IdStrategy.CUSTOMIZE_NUMBER
                        && mapping.getIdStrategy() != IdStrategy.CUSTOMIZE_UUID) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format(
                                    "Mapping[VERTEX/%s]: idFields '%s' (raw-id passthrough) requires a "
                                            + "CUSTOMIZE_STRING/NUMBER/UUID id strategy, but got '%s'.",
                                    mapping.getLabel(),
                                    mapping.getIdFields().get(0),
                                    mapping.getIdStrategy()));
                }
            }
            if (mapping.isUnfold()) {
                validateUnfoldable(
                        mapping,
                        mapping.getIdStrategy(),
                        mapping.getIdFields(),
                        String.format("mapping[VERTEX/%s]", mapping.getLabel()));
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
            // Endpoint id strategy lives on the server vertex label (unknown at config time), so
            // here we only enforce the config-derivable rules; the CUSTOMIZE-endpoint requirement
            // is
            // enforced at runtime when building ids.
            if (mapping.isUnfoldSource()) {
                validateUnfoldable(
                        mapping,
                        null,
                        mapping.getSourceConfig().getIdFields(),
                        String.format("mapping[EDGE/%s] sourceConfig", mapping.getLabel()));
            }
            if (mapping.isUnfoldTarget()) {
                validateUnfoldable(
                        mapping,
                        null,
                        mapping.getTargetConfig().getIdFields(),
                        String.format("mapping[EDGE/%s] targetConfig", mapping.getLabel()));
            }
        }
        validateSourceFields(mapping, mapping.getProperties(), "properties");
    }

    /**
     * unfold expands a single list-valued id cell into multiple elements, so it requires exactly
     * one id field and cannot be combined with raw-id passthrough. When {@code strategy} is known
     * (vertex), it must be a CUSTOMIZE_* strategy; for edge endpoints the strategy is server-side
     * and checked when ids are built.
     */
    private static void validateUnfoldable(
            MappingConfig mapping, IdStrategy strategy, List<String> idFields, String context) {
        if (idFields == null || idFields.size() != 1) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "%s: unfold requires exactly one id field, but got %s.",
                            context, idFields));
        }
        if (ReservedColumns.isRawIdPassthrough(idFields)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "%s: unfold cannot be combined with raw-id passthrough (%s).",
                            context, idFields.get(0)));
        }
        if (strategy != null
                && strategy != IdStrategy.CUSTOMIZE_STRING
                && strategy != IdStrategy.CUSTOMIZE_NUMBER
                && strategy != IdStrategy.CUSTOMIZE_UUID) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "%s: unfold requires a CUSTOMIZE_STRING/NUMBER/UUID id strategy, but got '%s'.",
                            context, strategy));
        }
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
     * mapping.properties (after fieldMapping transformation). Reserved columns (~id, ~label, ...)
     * emitted by the HugeGraph Source are excluded — they are not HugeGraph property keys and must
     * not be validated as such.
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
            sourceFields.removeAll(mapping.getIgnored());
            // Reserved columns (~id, ~label, ~source_id, ~target_id, ~source_label, ~target_label)
            // are emitted by the HugeGraph Source as routing/passthrough columns, not as
            // HugeGraph property keys. VertexMapper.applyProperties skips them; the validator
            // must do the same or it will fail trying to getPropertyKey("~id") from the server.
            ReservedColumns.stripReserved(sourceFields);
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
        // Raw-id passthrough reuses the pre-assembled ~source_id/~target_id string and never
        // rebuilds the endpoint vertex, so there is nothing to match against the label's primary
        // keys. Requiring the label to exist (getVertexLabel above) is enough.
        if (ReservedColumns.isRawIdPassthrough(endpoint.getIdFields())) {
            return;
        }
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
