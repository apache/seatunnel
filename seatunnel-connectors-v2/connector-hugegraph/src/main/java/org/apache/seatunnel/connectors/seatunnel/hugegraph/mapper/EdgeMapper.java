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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ReservedColumns;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;

import org.apache.hugegraph.serializer.direct.util.SplicingIdGenerator;
import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.constant.Frequency;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
    private final boolean unfoldSource;
    private final boolean unfoldTarget;

    public EdgeMapper(
            MappingConfig mappingConfig, Map<String, Integer> fieldsIndex, HugeGraphClient client) {
        this.mappingConfig = mappingConfig;
        this.client = client;
        this.labelId = client.getEdgeLabelId(mappingConfig.getLabel());
        this.fieldsIndex = fieldsIndex;
        this.edgeIdSourceFields = resolveEdgeIdSourceFields();
        this.propertySourceFields = resolvePropertySourceFields();
        this.propertyKeyCache = buildPropertyKeyCache();
        this.unfoldSource = mappingConfig.isUnfoldSource();
        this.unfoldTarget = mappingConfig.isUnfoldTarget();

        // Cache source/target vertex metadata to avoid per-row schema queries
        this.sourceVertexLabelId =
                client.getVertexLabelId(mappingConfig.getSourceConfig().getLabel());
        this.sourceIdStrategy = client.getIdStrategy(mappingConfig.getSourceConfig().getLabel());
        this.targetVertexLabelId =
                client.getVertexLabelId(mappingConfig.getTargetConfig().getLabel());
        this.targetIdStrategy = client.getIdStrategy(mappingConfig.getTargetConfig().getLabel());
    }

    @Override
    public boolean isUnfoldEnabled() {
        return unfoldSource || unfoldTarget;
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
            // Implicit mode ("write every row field as a property") — endpoint id fields locate
            // vertices and would otherwise be duplicated onto the edge; drop them here.
            fields.addAll(fieldsIndex.keySet());
            fields.removeAll(edgeIdSourceFields);
            fields.removeAll(reservedSourceFields(fieldsIndex.keySet()));
            // `ignored` blacklist only applies in implicit mode.
            fields.removeAll(mappingConfig.getIgnored());
        } else {
            // Explicit mode — respect the user's list verbatim. If they list an endpoint field it
            // is genuinely meant to appear as an edge property, matching what SchemaManager
            // creates on the server.
            fields.addAll(mappingConfig.getProperties());
        }
        // Sort keys are always edge properties (server-side EdgeId requires them).
        fields.addAll(mappingConfig.getSortKeys());
        return fields;
    }

    /**
     * Reserved fields emitted by the HugeGraph Source (e.g. {@code ~id}, {@code ~label}). They are
     * not valid HugeGraph property key names — including them would fail at server-side property
     * key creation — so drop them from an implicit round-trip.
     */
    private static Set<String> reservedSourceFields(Set<String> allFields) {
        Set<String> reserved = new HashSet<>();
        for (String field : allFields) {
            if (field != null && field.startsWith("~")) {
                reserved.add(field);
            }
        }
        return reserved;
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
        return buildEdge(row, sourceId, targetId);
    }

    /**
     * INSERT/append-path expansion: when {@code unfold_source} / {@code unfold_target} is set, a
     * list-valued endpoint id cell expands into multiple endpoint ids and edges are produced for
     * the cartesian product. Only CUSTOMIZE endpoints are supported (validated in SchemaValidator).
     */
    @Override
    public List<GraphElement> mapAll(SeaTunnelRow row) {
        if (!unfoldSource && !unfoldTarget) {
            Edge edge = map(row);
            return edge == null ? Collections.emptyList() : Collections.singletonList(edge);
        }
        List<Object> sourceIds =
                buildVertexIdList(row, mappingConfig.getSourceConfig(), unfoldSource);
        List<Object> targetIds =
                buildVertexIdList(row, mappingConfig.getTargetConfig(), unfoldTarget);
        if (sourceIds.isEmpty() || targetIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<GraphElement> result = new ArrayList<>(sourceIds.size() * targetIds.size());
        for (Object sourceId : sourceIds) {
            for (Object targetId : targetIds) {
                result.add(buildEdge(row, sourceId, targetId));
            }
        }
        return result;
    }

    private List<Object> buildVertexIdList(
            SeaTunnelRow row, SourceTargetConfig config, boolean unfoldEndpoint) {
        if (!unfoldEndpoint) {
            Object id = buildVertexId(row, config);
            return id == null ? Collections.emptyList() : Collections.singletonList(id);
        }
        boolean isSource = config == mappingConfig.getSourceConfig();
        IdStrategy strategy = isSource ? sourceIdStrategy : targetIdStrategy;
        String idField = config.getIdFields().get(0);
        Integer idx = fieldsIndex.get(idField);
        if (idx == null) {
            return Collections.emptyList();
        }
        Object raw = row.getField(idx);
        if (isConsideredNull(raw)) {
            return Collections.emptyList();
        }
        List<Object> elements = DataTypeUtil.splitField(idField, raw);
        List<Object> ids = new ArrayList<>(elements.size());
        for (Object elem : elements) {
            if (isConsideredNull(elem)) {
                continue;
            }
            ids.add(coerceCustomizeId(strategy, elem));
        }
        return ids;
    }

    private static Object coerceCustomizeId(IdStrategy strategy, Object elem) {
        switch (strategy) {
            case CUSTOMIZE_STRING:
                return VertexMapper.checkVertexIdLength(String.valueOf(elem));
            case CUSTOMIZE_NUMBER:
                return VertexMapper.coerceNumberId(elem);
            case CUSTOMIZE_UUID:
                return elem instanceof UUID ? elem : UUID.fromString(String.valueOf(elem));
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        "unfold requires a CUSTOMIZE_STRING/NUMBER/UUID endpoint id strategy, but got "
                                + strategy);
        }
    }

    private Edge buildEdge(SeaTunnelRow row, Object sourceId, Object targetId) {
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
                            mappingConfig.getTimeZone(),
                            mappingConfig.getExtraDateFormats(),
                            mappingConfig.getListFormat());
            edge.property(propName, getMappedValue(sourceField, converted));
        }
        return edge;
    }

    private Object buildVertexId(SeaTunnelRow row, SourceTargetConfig config) {
        boolean isSource = config == mappingConfig.getSourceConfig();
        String vertexLabelId = isSource ? sourceVertexLabelId : targetVertexLabelId;
        IdStrategy strategy = isSource ? sourceIdStrategy : targetIdStrategy;
        List<String> idFields = config.getIdFields();

        // Raw-id passthrough: the endpoint id is already assembled in a reserved Source column
        // (~source_id / ~target_id). Use it directly so an edge can be cloned without re-deriving
        // the endpoint vertex ids from primary-key columns. Works for any endpoint id strategy
        // (including AUTOMATIC) because we only reuse the id string, never rebuild the vertex.
        if (ReservedColumns.isRawIdPassthrough(idFields)) {
            Integer idx = fieldsIndex.get(idFields.get(0));
            Object raw = idx == null ? null : row.getField(idx);
            if (isConsideredNull(raw)) {
                return null;
            }
            return coerceRawVertexId(String.valueOf(raw), strategy);
        }

        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

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
                return VertexMapper.spliceCustomizeStringId(stringValues);
            case CUSTOMIZE_NUMBER:
                List<Object> numberValues = getFieldValues(row, idFields);
                if (numberValues.size() != 1) {
                    return null;
                }
                Object numValue = numberValues.get(0);
                if (isConsideredNull(numValue)) {
                    return null;
                }
                return VertexMapper.coerceNumberId(numValue);
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
                                mappingConfig.getTimeZone(),
                                mappingConfig.getExtraDateFormats(),
                                mappingConfig.getListFormat());
                values.add(getMappedValue(fieldName, converted));
            } else {
                values.add(getMappedValue(fieldName, rawValue));
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
     * frequency uses the sortKeys values. Vertex IDs are prefixed with 'S' for String IDs, 'L' for
     * Number IDs, and 'U' for UUID IDs.
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
     * format-sensitive layout (vertex-id prefix, doubled label id for the sub-label segment,
     * sortValues segment) is unit-testable without a live server — this is the DELETE-correctness
     * path.
     *
     * <p>Uses HugeGraph's own {@link SplicingIdGenerator} so the encoding matches the server:
     * {@code concat} joins the five segments with {@code '>'} and backtick-escapes any {@code '>'}
     * inside a segment; {@code concatValues} joins the sort-key values with {@code '!'} and
     * backtick-escapes any {@code '!'}. Vertex ids carry the type prefix HugeGraph expects: {@code
     * 'L'} for numbers, {@code 'U'} for UUIDs, {@code 'S'} for strings.
     */
    static String spliceEdgeId(
            Object sourceId, Object targetId, String labelId, List<Object> sortValues) {
        String sort =
                (sortValues == null || sortValues.isEmpty())
                        ? ""
                        : SplicingIdGenerator.concatValues(sortValues);
        return SplicingIdGenerator.concat(
                vertexIdString(sourceId), labelId, labelId, sort, vertexIdString(targetId));
    }

    /**
     * Converts a reserved-column id string ({@code ~source_id}/{@code ~target_id}) back into the
     * Java id type the endpoint vertex uses, so {@link #vertexIdString} re-applies the correct
     * {@code L}/{@code U}/{@code S} prefix. PRIMARY_KEY / CUSTOMIZE_STRING ids stay strings (e.g.
     * {@code "1:marko"} → {@code "S1:marko"}); CUSTOMIZE_NUMBER / AUTOMATIC parse to a long;
     * CUSTOMIZE_UUID parses to a UUID.
     */
    private static Object coerceRawVertexId(String raw, IdStrategy strategy) {
        if (strategy == IdStrategy.CUSTOMIZE_NUMBER || strategy == IdStrategy.AUTOMATIC) {
            return Long.parseLong(raw);
        }
        if (strategy == IdStrategy.CUSTOMIZE_UUID) {
            return UUID.fromString(raw);
        }
        return raw;
    }

    /** Prepends the HugeGraph vertex-id type prefix: 'L' number, 'U' UUID, 'S' string. */
    private static String vertexIdString(Object id) {
        String prefix;
        if (id instanceof Number) {
            prefix = "L";
        } else if (id instanceof UUID) {
            prefix = "U";
        } else {
            prefix = "S";
        }
        return prefix + id;
    }

    private List<Object> getSortKeyValues(SeaTunnelRow row) {
        Frequency frequency = mappingConfig.getFrequency();
        if (frequency == null || frequency == Frequency.SINGLE) {
            return Collections.emptyList();
        }
        List<String> sortKeys = mappingConfig.getSortKeys();
        if (sortKeys.isEmpty()) {
            return Collections.emptyList();
        }
        return getFieldValues(row, sortKeys);
    }

    private boolean isConsideredNull(Object value) {
        if (value == null) {
            return true;
        }
        List<String> nullValues = mappingConfig.getNullValues();
        return !nullValues.isEmpty() && nullValues.contains(String.valueOf(value));
    }

    private Object getMappedValue(String sourceField, Object originalValue) {
        Map<String, Map<Object, Object>> vm = mappingConfig.getValueMapping();
        if (vm.isEmpty()) {
            return originalValue;
        }
        Map<Object, Object> perField = vm.get(sourceField);
        if (perField == null || perField.isEmpty()) {
            return originalValue;
        }
        return perField.getOrDefault(originalValue, originalValue);
    }

    private String spliceVertexId(String vertexLabelId, List<Object> primaryValues) {
        // HugeGraph primary-key vertex id = {vertexLabelId}:{concatValues(pk)}; concatValues joins
        // with '!' and backtick-escapes any '!' so pk values containing the separator still match.
        return VertexMapper.checkVertexIdLength(
                String.format(
                        "%s:%s", vertexLabelId, SplicingIdGenerator.concatValues(primaryValues)));
    }
}
