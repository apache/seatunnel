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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.ReservedColumns;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.E;

import org.apache.hugegraph.serializer.direct.util.SplicingIdGenerator;
import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
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
    private final boolean unfold;

    public VertexMapper(
            MappingConfig mappingConfig, Map<String, Integer> fieldsIndex, HugeGraphClient client) {
        this.mappingConfig = mappingConfig;
        this.client = client;
        this.labelId = client.getVertexLabelId(mappingConfig.getLabel());
        this.fieldsIndex = fieldsIndex;
        this.propertySourceFields = resolvePropertySourceFields();
        this.propertyKeyCache = buildPropertyKeyCache();
        this.unfold = mappingConfig.isUnfold();
    }

    @Override
    public boolean isUnfoldEnabled() {
        return unfold;
    }

    private Set<String> resolvePropertySourceFields() {
        Set<String> fields = new HashSet<>();
        if (mappingConfig.getProperties().isEmpty()) {
            fields.addAll(fieldsIndex.keySet());
            // Drop reserved columns emitted by HugeGraph Source (~id, ~label, ...) — they are not
            // valid HugeGraph property key names, so an implicit Source→Sink round-trip would
            // otherwise attempt to create them on the server.
            ReservedColumns.stripReserved(fields);
            // `ignored` blacklist only applies in implicit mode (an explicit `properties`
            // whitelist already lists exactly what to keep).
            fields.removeAll(mappingConfig.getIgnored());
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

        applyProperties(vertex, row, null);
        return vertex;
    }

    /**
     * INSERT/append-path expansion: when {@code unfold} is set, a list-valued CUSTOMIZE id cell
     * produces one vertex per element (all sharing the same non-id properties). Without unfold this
     * is just {@link #map} wrapped in a list.
     */
    @Override
    public List<GraphElement> mapAll(SeaTunnelRow row) {
        if (!unfold) {
            Vertex vertex = map(row);
            return vertex == null ? Collections.emptyList() : Collections.singletonList(vertex);
        }
        IdStrategy strategy = mappingConfig.getIdStrategy();
        String idField = mappingConfig.getIdFields().get(0);
        Integer idx = fieldsIndex.get(idField);
        if (idx == null) {
            return Collections.emptyList();
        }
        Object raw = row.getField(idx);
        if (isConsideredNull(raw)) {
            return Collections.emptyList();
        }
        List<Object> elements = DataTypeUtil.splitField(idField, raw);
        List<GraphElement> result = new ArrayList<>(elements.size());
        for (Object elem : elements) {
            if (isConsideredNull(elem)) {
                continue;
            }
            Vertex vertex = new Vertex(mappingConfig.getLabel());
            vertex.id(coerceCustomizeId(strategy, elem));
            // The id field is the unfolded source, not a property — skip it.
            applyProperties(vertex, row, idField);
            result.add(vertex);
        }
        return result;
    }

    private static Object coerceCustomizeId(IdStrategy strategy, Object elem) {
        switch (strategy) {
            case CUSTOMIZE_STRING:
                return checkVertexIdLength(String.valueOf(elem));
            case CUSTOMIZE_NUMBER:
                return coerceNumberId(elem);
            case CUSTOMIZE_UUID:
                return elem instanceof UUID ? elem : UUID.fromString(String.valueOf(elem));
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        "unfold requires a CUSTOMIZE_STRING/NUMBER/UUID id strategy, but got "
                                + strategy);
        }
    }

    private void applyProperties(Vertex vertex, SeaTunnelRow row, String skipField) {
        Map<String, String> fm = mappingConfig.getFieldMapping();
        for (String sourceField : propertySourceFields) {
            if (sourceField.equals(skipField)) {
                continue;
            }
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
            vertex.property(propName, getMappedValue(sourceField, converted));
        }
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

        // Raw-id passthrough: the vertex id is already assembled in the reserved ~id Source column.
        // Only meaningful for CUSTOMIZE_* strategies (the ones that accept an externally supplied
        // id); PRIMARY_KEY derives its id from property values and AUTOMATIC is server-assigned, so
        // both are rejected at config time in SchemaValidator.
        if (ReservedColumns.isRawIdPassthrough(idFields)) {
            Integer idx = fieldsIndex.get(idFields.get(0));
            Object raw = idx == null ? null : row.getField(idx);
            if (isConsideredNull(raw)) {
                return null;
            }
            switch (strategy) {
                case CUSTOMIZE_STRING:
                    return checkVertexIdLength(String.valueOf(raw));
                case CUSTOMIZE_NUMBER:
                    return coerceNumberId(raw);
                case CUSTOMIZE_UUID:
                    return UUID.fromString(String.valueOf(raw));
                default:
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format(
                                    "Mapping[VERTEX/%s]: idFields '%s' (raw-id passthrough) requires a "
                                            + "CUSTOMIZE_STRING/NUMBER/UUID id strategy, but got '%s'.",
                                    mappingConfig.getLabel(), idFields.get(0), strategy));
            }
        }

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
                return spliceCustomizeStringId(stringValues);
            case CUSTOMIZE_NUMBER:
                List<Object> numberValues = getFieldValues(row, idFields);
                if (numberValues.size() != 1) {
                    return null;
                }
                Object numValue = numberValues.get(0);
                if (isConsideredNull(numValue)) {
                    return null;
                }
                return coerceNumberId(numValue);
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
                                mappingConfig.getTimeZone(),
                                mappingConfig.getExtraDateFormats(),
                                mappingConfig.getListFormat());
            }
            values.add(getMappedValue(fieldName, converted));
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

    private String spliceVertexId(List<Object> primaryValues) {
        // HugeGraph primary-key vertex id = {vertexLabelId}:{concatValues(pk)}; concatValues joins
        // with '!' and backtick-escapes any '!' in a value, matching how the server assembles the
        // id. EdgeMapper uses the same helper for the same concept — a raw join here would produce
        // an ambiguous, server-mismatched id when a pk value contains '!', so DELETE / key-changing
        // UPDATE would target the wrong (or a non-existent) vertex.
        return checkVertexIdLength(
                String.format("%s:%s", labelId, SplicingIdGenerator.concatValues(primaryValues)));
    }

    /**
     * Assembles a CUSTOMIZE_STRING id from its id-field values. A single field is used verbatim —
     * it is unambiguous, and escaping it would change ids already written for the common
     * single-field case. Multiple fields are joined with ':' after backslash-escaping any ':' (and
     * the '\' escape char itself) in each value, so distinct field tuples cannot collapse to the
     * same id — e.g. ("x:y","z") and ("x","y:z") no longer both yield "x:y:z" and overwrite each
     * other. Shared by {@link VertexMapper} and {@code EdgeMapper} so both build ids identically.
     */
    static String spliceCustomizeStringId(List<Object> values) {
        if (values.size() == 1) {
            return checkVertexIdLength(String.valueOf(values.get(0)));
        }
        return checkVertexIdLength(
                values.stream()
                        .map(String::valueOf)
                        .map(VertexMapper::escapeIdSegment)
                        .collect(Collectors.joining(":")));
    }

    private static String escapeIdSegment(String value) {
        return value.replace("\\", "\\\\").replace(":", "\\:");
    }

    /**
     * Coerces a CUSTOMIZE_NUMBER id value to a long, consistently for both Number and String
     * inputs. A fractional value ({@code 1.9} whether it arrives as a Double or the string "1.9")
     * is rejected rather than silently truncated to {@code 1}; an integral decimal like {@code
     * 1.0}/"1.0" is accepted. Shared by {@link VertexMapper} and {@code EdgeMapper} so both behave
     * identically.
     */
    static long coerceNumberId(Object value) {
        java.math.BigDecimal decimal;
        try {
            decimal = new java.math.BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format("CUSTOMIZE_NUMBER id value '%s' is not a number.", value),
                    e);
        }
        try {
            return decimal.longValueExact();
        } catch (ArithmeticException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "CUSTOMIZE_NUMBER id value '%s' is not an integer: a fractional or "
                                    + "out-of-range value cannot be used as a numeric id (it would "
                                    + "otherwise be silently truncated).",
                            value),
                    e);
        }
    }

    /** HugeGraph server per-vertex id cap (see loader Constants.VERTEX_ID_LIMIT). */
    static final int VERTEX_ID_LIMIT = 128;

    /**
     * Rejects a string vertex id whose UTF-8 length exceeds the server limit, so the user gets a
     * clear client-side error instead of an opaque server rejection. Number/UUID ids are always
     * within the limit and are not checked.
     */
    static String checkVertexIdLength(String id) {
        int length = id.getBytes(StandardCharsets.UTF_8).length;
        if (length > VERTEX_ID_LIMIT) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "The vertex id length (%d bytes) exceeds the limit of %d: '%s'",
                            length, VERTEX_ID_LIMIT, id));
        }
        return id;
    }
}
