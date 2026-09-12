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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.LabelOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ReservedColumns;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages HugeGraph schema lifecycle during Sink initialization. Handles auto-creation under
 * CREATE_SCHEMA_WHEN_NOT_EXIST and strict validation under ERROR_WHEN_SCHEMA_NOT_EXIST.
 */
public final class SchemaManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    private final HugeGraphClient client;
    private final HugeGraphSchemaSaveMode saveMode;
    private final SeaTunnelRowType rowType;

    public SchemaManager(
            HugeGraphClient client, HugeGraphSchemaSaveMode saveMode, SeaTunnelRowType rowType) {
        this.client = client;
        this.saveMode = saveMode;
        this.rowType = rowType;
    }

    /**
     * Ensures schema exists for all mappings. Creation order: PropertyKeys first, then
     * VertexLabels, then EdgeLabels — so edge source/target labels exist before edge creation.
     */
    public void ensureSchema(List<MappingConfig> mappings) {
        if (saveMode == HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
            // Phase 1: create all PropertyKeys
            for (MappingConfig mapping : mappings) {
                Set<String> propertyNames = resolveTargetPropertyNames(mapping);
                createMissingPropertyKeys(mapping, propertyNames);
                if (mapping.getType() == LabelType.EDGE) {
                    createMissingPropertyKeys(mapping, resolveEndpointPropertyNames(mapping));
                }
            }
            // Phase 2: create all VertexLabels
            Set<String> resolvedVertexLabels = new HashSet<>();
            for (MappingConfig mapping : mappings) {
                if (mapping.getType() == LabelType.VERTEX) {
                    Set<String> propertyNames = resolveTargetPropertyNames(mapping);
                    createVertexLabelIfMissing(mapping, propertyNames);
                    resolvedVertexLabels.add(mapping.getLabel());
                }
            }
            // Edge-only mappings still need reconstructable endpoint vertex schemas.
            for (MappingConfig mapping : mappings) {
                if (mapping.getType() == LabelType.EDGE) {
                    createEndpointVertexLabelIfMissing(
                            mapping, mapping.getSourceConfig(), resolvedVertexLabels);
                    createEndpointVertexLabelIfMissing(
                            mapping, mapping.getTargetConfig(), resolvedVertexLabels);
                }
            }
            // Phase 3: create all EdgeLabels (source/target vertex labels now guaranteed to exist)
            for (MappingConfig mapping : mappings) {
                if (mapping.getType() == LabelType.EDGE) {
                    Set<String> propertyNames = resolveTargetPropertyNames(mapping);
                    createEdgeLabelIfMissing(mapping, propertyNames);
                }
            }
        } else {
            for (MappingConfig mapping : mappings) {
                Set<String> propertyNames = resolveTargetPropertyNames(mapping);
                validateSchemaExists(mapping, propertyNames);
            }
        }
    }

    private Set<String> resolveEndpointPropertyNames(MappingConfig mapping) {
        Set<String> propertyNames = new HashSet<>();
        addEndpointPropertyNames(mapping.getSourceConfig(), mapping, propertyNames);
        addEndpointPropertyNames(mapping.getTargetConfig(), mapping, propertyNames);
        return propertyNames;
    }

    private void addEndpointPropertyNames(
            MappingConfig.SourceTargetConfig endpoint,
            MappingConfig mapping,
            Set<String> propertyNames) {
        if (endpoint == null || ReservedColumns.isRawIdPassthrough(endpoint.getIdFields())) {
            // Raw-id passthrough reuses a reserved column (~source_id/~target_id) that is not a
            // HugeGraph property — never create a PropertyKey for it.
            return;
        }
        List<String> properties =
                mapToTargetNames(endpoint.getIdFields(), mapping.getFieldMapping());
        if (properties != null) {
            propertyNames.addAll(properties);
        }
    }

    private void createEndpointVertexLabelIfMissing(
            MappingConfig edgeMapping,
            MappingConfig.SourceTargetConfig endpoint,
            Set<String> resolvedVertexLabels) {
        if (endpoint == null || resolvedVertexLabels.contains(endpoint.getLabel())) {
            return;
        }
        if (endpoint.getIdFields() == null || endpoint.getIdFields().isEmpty()) {
            return;
        }
        // Raw-id passthrough carries a pre-assembled endpoint id, not primary-key columns, so we
        // cannot synthesize a PRIMARY_KEY vertex label from it. Such a clone assumes the endpoint
        // vertex label already exists; leave creation to the user.
        if (ReservedColumns.isRawIdPassthrough(endpoint.getIdFields())) {
            return;
        }
        if (client.getVertexLabelOrNull(endpoint.getLabel()) != null) {
            resolvedVertexLabels.add(endpoint.getLabel());
            return;
        }

        List<String> primaryKeys =
                mapToTargetNames(endpoint.getIdFields(), edgeMapping.getFieldMapping());
        LOG.info(
                "Mapping[EDGE/{}]: Auto-creating endpoint VertexLabel '{}' with PRIMARY_KEY fields={}",
                edgeMapping.getLabel(),
                endpoint.getLabel(),
                primaryKeys);
        client.createVertexLabelIfNotExist(
                endpoint.getLabel(),
                IdStrategy.PRIMARY_KEY,
                primaryKeys,
                new ArrayList<>(primaryKeys),
                new ArrayList<>(),
                new LabelOptions(null, null, null, null));
        resolvedVertexLabels.add(endpoint.getLabel());
    }

    /** Resolves the set of target property names that will be written for this mapping. */
    private Set<String> resolveTargetPropertyNames(MappingConfig mapping) {
        Set<String> result = new HashSet<>();
        Map<String, String> fieldMapping = mapping.getFieldMapping();

        Set<String> sourceFields = new HashSet<>();
        if (mapping.getProperties().isEmpty()) {
            for (String fieldName : rowType.getFieldNames()) {
                // Reserved columns emitted by HugeGraph Source (~id, ~label, ...) are not valid
                // HugeGraph property key names — HugeGraph rejects PropertyKey names starting
                // with '~'. An implicit Source→Sink round-trip would otherwise attempt to create
                // them and fail at label creation time.
                if (fieldName != null && !fieldName.startsWith("~")) {
                    sourceFields.add(fieldName);
                }
            }
            if (mapping.getType() == LabelType.EDGE) {
                removeIdFields(sourceFields, mapping.getSourceConfig());
                removeIdFields(sourceFields, mapping.getTargetConfig());
            }
            // `ignored` blacklist only applies in implicit mode; must match the mappers so schema
            // creation and writes agree on the property set.
            sourceFields.removeAll(mapping.getIgnored());
        } else {
            sourceFields.addAll(mapping.getProperties());
        }
        if (mapping.getType() == LabelType.EDGE) {
            sourceFields.addAll(mapping.getSortKeys());
        }

        for (String sourceField : sourceFields) {
            String targetProp = fieldMapping.getOrDefault(sourceField, sourceField);
            result.add(targetProp);
        }

        // PRIMARY_KEY idFields are always written as properties by VertexMapper,
        // so they must be included regardless of whether properties list is empty
        if (mapping.getType() == LabelType.VERTEX
                && mapping.getIdStrategy() == IdStrategy.PRIMARY_KEY
                && mapping.getIdFields() != null) {
            for (String idField : mapping.getIdFields()) {
                String targetProp = fieldMapping.getOrDefault(idField, idField);
                result.add(targetProp);
            }
        }

        return result;
    }

    private static void removeIdFields(
            Set<String> fields, MappingConfig.SourceTargetConfig sourceTargetConfig) {
        if (sourceTargetConfig != null && sourceTargetConfig.getIdFields() != null) {
            fields.removeAll(sourceTargetConfig.getIdFields());
        }
    }

    private void createMissingPropertyKeys(MappingConfig mapping, Set<String> targetPropertyNames) {
        Map<String, String> fieldMapping = mapping.getFieldMapping();

        for (String targetProp : targetPropertyNames) {
            if (client.getPropertyKeyOrNull(targetProp) != null) {
                continue;
            }

            String sourceField = findSourceField(targetProp, fieldMapping);
            int fieldIndex = findFieldIndex(sourceField);
            if (fieldIndex < 0) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[%s/%s]: Source field '%s' for target property '%s' "
                                        + "not found in input row. Available fields: %s",
                                mapping.getType(),
                                mapping.getLabel(),
                                sourceField,
                                targetProp,
                                getFieldNames()));
            }

            SeaTunnelDataType<?> seaType = rowType.getFieldType(fieldIndex);
            DataType hgType = inferHugeGraphDataType(seaType, mapping, targetProp);
            Cardinality cardinality = inferCardinality(seaType);

            LOG.info(
                    "Mapping[{}/{}]: Auto-creating PropertyKey '{}' with type={}, cardinality={}",
                    mapping.getType(),
                    mapping.getLabel(),
                    targetProp,
                    hgType,
                    cardinality);
            client.createPropertyKeyIfNotExist(targetProp, hgType, cardinality);
        }
    }

    private void createVertexLabelIfMissing(MappingConfig mapping, Set<String> propertyNames) {
        if (client.getVertexLabelOrNull(mapping.getLabel()) != null) {
            LOG.debug("VertexLabel '{}' already exists, skipping creation.", mapping.getLabel());
            return;
        }

        IdStrategy idStrategy =
                mapping.getIdStrategy() != null ? mapping.getIdStrategy() : IdStrategy.PRIMARY_KEY;
        // Primary keys must reference target property names (after fieldMapping), matching the
        // property names used for label creation
        List<String> primaryKeys =
                idStrategy == IdStrategy.PRIMARY_KEY
                        ? mapToTargetNames(mapping.getIdFields(), mapping.getFieldMapping())
                        : null;

        List<String> nullableKeys = computeNullableKeys(mapping, propertyNames);

        LOG.info(
                "Mapping[VERTEX/{}]: Auto-creating VertexLabel with idStrategy={}, properties={}",
                mapping.getLabel(),
                idStrategy,
                propertyNames);
        client.createVertexLabelIfNotExist(
                mapping.getLabel(),
                idStrategy,
                primaryKeys,
                new ArrayList<>(propertyNames),
                nullableKeys,
                buildLabelOptions(mapping));
    }

    private void createEdgeLabelIfMissing(MappingConfig mapping, Set<String> propertyNames) {
        if (client.getEdgeLabelOrNull(mapping.getLabel()) != null) {
            LOG.debug("EdgeLabel '{}' already exists, skipping creation.", mapping.getLabel());
            return;
        }

        E.checkNotNull(mapping.getSourceConfig(), "sourceConfig", "edge mapping");
        E.checkNotNull(mapping.getTargetConfig(), "targetConfig", "edge mapping");

        Frequency frequency =
                mapping.getFrequency() != null ? mapping.getFrequency() : Frequency.SINGLE;
        // Sort keys must reference target property names (after fieldMapping)
        List<String> sortKeys =
                frequency == Frequency.MULTIPLE
                        ? mapToTargetNames(mapping.getSortKeys(), mapping.getFieldMapping())
                        : null;

        if (frequency == Frequency.MULTIPLE && (sortKeys == null || sortKeys.isEmpty())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Mapping[EDGE/%s]: 'sortKeys' must be specified when frequency is MULTIPLE.",
                            mapping.getLabel()));
        }

        List<String> nullableKeys = computeNullableKeys(mapping, propertyNames);

        LOG.info(
                "Mapping[EDGE/{}]: Auto-creating EdgeLabel ({}→{}) with frequency={}, properties={}",
                mapping.getLabel(),
                mapping.getSourceConfig().getLabel(),
                mapping.getTargetConfig().getLabel(),
                frequency,
                propertyNames);
        client.createEdgeLabelIfNotExist(
                mapping.getLabel(),
                mapping.getSourceConfig().getLabel(),
                mapping.getTargetConfig().getLabel(),
                frequency,
                sortKeys,
                new ArrayList<>(propertyNames),
                nullableKeys,
                buildLabelOptions(mapping));
    }

    /**
     * Collects the optional label attributes (ttl / ttlStartTime / enableLabelIndex / userdata)
     * from the mapping so they are actually applied at label creation instead of silently ignored.
     */
    private LabelOptions buildLabelOptions(MappingConfig mapping) {
        Boolean enableLabelIndex =
                mapping.getEnableLabelIndex() == null
                        ? null
                        : Boolean.parseBoolean(mapping.getEnableLabelIndex());
        return new LabelOptions(
                mapping.getTtl(),
                mapping.getTtlStartTime(),
                enableLabelIndex,
                mapping.getUserdata());
    }

    private void validateSchemaExists(MappingConfig mapping, Set<String> targetPropertyNames) {
        if (mapping.getType() == LabelType.VERTEX) {
            if (client.getVertexLabelOrNull(mapping.getLabel()) == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[VERTEX/%s]: VertexLabel does not exist in HugeGraph. "
                                        + "Create it manually or set schema_save_mode=CREATE_SCHEMA_WHEN_NOT_EXIST.",
                                mapping.getLabel()));
            }
        } else {
            if (client.getEdgeLabelOrNull(mapping.getLabel()) == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[EDGE/%s]: EdgeLabel does not exist in HugeGraph. "
                                        + "Create it manually or set schema_save_mode=CREATE_SCHEMA_WHEN_NOT_EXIST.",
                                mapping.getLabel()));
            }
        }

        for (String propName : targetPropertyNames) {
            if (client.getPropertyKeyOrNull(propName) == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[%s/%s]: PropertyKey '%s' does not exist in HugeGraph. "
                                        + "Create it manually or set schema_save_mode=CREATE_SCHEMA_WHEN_NOT_EXIST.",
                                mapping.getType(), mapping.getLabel(), propName));
            }
        }
    }

    // --- Type inference ---

    private DataType inferHugeGraphDataType(
            SeaTunnelDataType<?> seaType, MappingConfig mapping, String propertyName) {
        switch (seaType.getSqlType()) {
            case STRING:
                return DataType.TEXT;
            case BIGINT:
                return DataType.LONG;
            case INT:
            case TINYINT:
            case SMALLINT:
                return DataType.INT;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
                return DataType.DOUBLE;
            case BOOLEAN:
                return DataType.BOOLEAN;
            case DATE:
            case TIMESTAMP:
                return DataType.DATE;
            case BYTES:
                return DataType.BLOB;
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) seaType).getElementType();
                return inferHugeGraphDataType(elementType, mapping, propertyName);
            case MAP:
            case ROW:
            case DECIMAL:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Mapping[%s/%s]: Source field for target property '%s' has unsupported "
                                        + "SeaTunnel type '%s' and cannot be auto-created in HugeGraph. "
                                        + "Pre-create the PropertyKey with an appropriate representation "
                                        + "(for example TEXT for serialized data), or use a Transform to "
                                        + "convert the field before the HugeGraph sink.",
                                mapping.getType(),
                                mapping.getLabel(),
                                propertyName,
                                seaType.getSqlType()));
            default:
                return DataType.TEXT;
        }
    }

    private Cardinality inferCardinality(SeaTunnelDataType<?> seaType) {
        if (seaType.getSqlType() == org.apache.seatunnel.api.table.type.SqlType.ARRAY) {
            return Cardinality.LIST;
        }
        return Cardinality.SINGLE;
    }

    // --- Helpers ---

    /** Maps source field names to target property names via fieldMapping. */
    private static List<String> mapToTargetNames(
            List<String> sourceFields, Map<String, String> fieldMapping) {
        if (sourceFields == null) {
            return null;
        }
        List<String> result = new ArrayList<>(sourceFields.size());
        for (String field : sourceFields) {
            result.add(fieldMapping.getOrDefault(field, field));
        }
        return result;
    }

    private String findSourceField(String targetProp, Map<String, String> fieldMapping) {
        for (Map.Entry<String, String> entry : fieldMapping.entrySet()) {
            if (targetProp.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return targetProp;
    }

    private int findFieldIndex(String fieldName) {
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (rowType.getFieldName(i).equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private String getFieldNames() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(rowType.getFieldName(i));
        }
        return sb.append("]").toString();
    }

    /**
     * Decides which target properties are declared nullable on a newly created label.
     *
     * <p>Default (when the mapping declares neither {@code nullableKeys} nor {@code
     * notNullableKeys}): every non-key property is nullable. HugeGraph server rejects any insert
     * whose row omits a non-nullable property, so the previous "everything non-null" default meant
     * a single null cell from JDBC/CSV/Kafka failed an entire batch. Loader and spark-connector
     * both default to nullable non-key properties; this matches them.
     *
     * <p>Key properties — primary keys (PRIMARY_KEY vertices) and MULTIPLE-edge sort keys — are
     * always excluded because HugeGraph server disallows them being nullable.
     *
     * <p>Explicit {@code nullableKeys} wins verbatim (subject to key-exclusion + presence
     * filtering). If it is empty, {@code notNullableKeys} carves out required-property opt-outs
     * from the default.
     */
    static List<String> computeNullableKeys(MappingConfig mapping, Set<String> propertyNames) {
        Map<String, String> fm = mapping.getFieldMapping();
        Set<String> keyProps = computeKeyProperties(mapping);

        List<String> explicit = mapping.getNullableKeys();
        if (!explicit.isEmpty()) {
            List<String> result = new ArrayList<>();
            for (String nk : explicit) {
                String targetName = fm.getOrDefault(nk, nk);
                if (propertyNames.contains(targetName) && !keyProps.contains(targetName)) {
                    result.add(targetName);
                }
            }
            return result;
        }

        Set<String> notNullableTargets = new HashSet<>();
        for (String nk : mapping.getNotNullableKeys()) {
            notNullableTargets.add(fm.getOrDefault(nk, nk));
        }

        List<String> result = new ArrayList<>();
        for (String propName : propertyNames) {
            if (!keyProps.contains(propName) && !notNullableTargets.contains(propName)) {
                result.add(propName);
            }
        }
        return result;
    }

    private static Set<String> computeKeyProperties(MappingConfig mapping) {
        Set<String> keys = new HashSet<>();
        Map<String, String> fm = mapping.getFieldMapping();
        if (mapping.getType() == LabelType.VERTEX
                && mapping.getIdStrategy() == IdStrategy.PRIMARY_KEY
                && mapping.getIdFields() != null) {
            for (String field : mapping.getIdFields()) {
                keys.add(fm.getOrDefault(field, field));
            }
        }
        if (mapping.getType() == LabelType.EDGE
                && mapping.getFrequency() == Frequency.MULTIPLE
                && mapping.getSortKeys() != null) {
            for (String field : mapping.getSortKeys()) {
                keys.add(fm.getOrDefault(field, field));
            }
        }
        return keys;
    }
}
