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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class HugeGraphSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSourceReader.class);

    /** HugeGraph server DATE serialization format: {@code yyyy-MM-dd HH:mm:ss.SSS}. */
    private static final DateTimeFormatter HUGEGRAPH_DATE_FORMAT =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 3, true)
                    .toFormatter();

    public static final String ID_FIELD = "~id";
    public static final String LABEL_FIELD = "~label";
    public static final String SOURCE_ID_FIELD = "~source_id";
    public static final String SOURCE_LABEL_FIELD = "~source_label";
    public static final String TARGET_ID_FIELD = "~target_id";
    public static final String TARGET_LABEL_FIELD = "~target_label";

    private final SingleSplitReaderContext context;
    private final HugeGraphSourceConfig sourceConfig;
    private final SeaTunnelRowType outputRowType;
    private final SeaTunnelRowType propertyRowType;
    private final HugeGraphOperations client;
    private String nextPage;
    private boolean started;
    private boolean finished;
    private boolean noMoreElementSignalled;
    private String lastEmittedId;
    private long duplicateSkipped;
    private long totalRecords;
    private int pageCount;

    public HugeGraphSourceReader(
            SingleSplitReaderContext context,
            HugeGraphSourceConfig sourceConfig,
            SeaTunnelRowType outputRowType) {
        this(
                context,
                sourceConfig,
                outputRowType,
                new HugeGraphClient(sourceConfig.getConnectionConfig()));
    }

    HugeGraphSourceReader(
            SingleSplitReaderContext context,
            HugeGraphSourceConfig sourceConfig,
            SeaTunnelRowType outputRowType,
            HugeGraphOperations client) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.outputRowType = outputRowType;
        this.propertyRowType = sourceConfig.getSchema();
        this.client = client;
        this.nextPage = null;
        this.started = false;
        this.finished = false;
        this.noMoreElementSignalled = false;
        this.lastEmittedId = null;
        this.duplicateSkipped = 0;
        this.totalRecords = 0;
        this.pageCount = 0;
    }

    @Override
    public void open() {
        try {
            // validateLabelAndSchema triggers the lazy client connection; if it throws (unknown
            // label, type mismatch, unreachable server) the framework may not call close(), so
            // release the just-opened client here to avoid leaking connection pools/threads.
            validateLabelAndSchema();
        } catch (RuntimeException e) {
            try {
                client.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        synchronized (output.getCheckpointLock()) {
            if (noMoreSplits) {
                signalNoMoreElement();
                return;
            }
            internalPollNext(output);
        }
    }

    /** Reads one bounded page so checkpoints can persist progress between page requests. */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) {
        if (finished) {
            noMoreSplits = true;
            signalNoMoreElement();
            return;
        }

        String requestedPage = nextPage;
        int recordCount;
        String responsePage;
        if (sourceConfig.getLabelType() == MappingConfig.LabelType.VERTEX) {
            PageResult<Vertex> page =
                    client.listVertices(
                            sourceConfig.getLabel(), requestedPage, sourceConfig.getPageSize());
            collectVertices(page.getRecords(), output);
            recordCount = page.getRecords().size();
            responsePage = page.getNextPage();
        } else {
            PageResult<Edge> page =
                    client.listEdges(
                            sourceConfig.getLabel(), requestedPage, sourceConfig.getPageSize());
            collectEdges(page.getRecords(), output);
            recordCount = page.getRecords().size();
            responsePage = page.getNextPage();
        }

        if (responsePage != null && responsePage.equals(requestedPage)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "HugeGraph pagination marker did not advance for label '%s': '%s'",
                            sourceConfig.getLabel(), responsePage));
        }

        started = true;
        nextPage = responsePage;
        totalRecords += recordCount;
        pageCount++;
        if (recordCount == 0 && responsePage != null) {
            LOG.debug(
                    "HugeGraph source received an empty intermediate page for label '{}'; continuing with the next marker",
                    sourceConfig.getLabel());
        }
        if (responsePage == null) {
            finished = true;
            noMoreSplits = true;
            LOG.info(
                    "HugeGraph source finished scanning label '{}': {} records in {} pages"
                            + " ({} server-side paging duplicates skipped)",
                    sourceConfig.getLabel(),
                    totalRecords,
                    pageCount,
                    duplicateSkipped);
            signalNoMoreElement();
        }
    }

    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeBoolean(started);
            writeNullableString(output, nextPage);
            output.writeBoolean(finished);
            writeNullableString(output, lastEmittedId);
            output.writeLong(duplicateSkipped);
            output.writeLong(totalRecords);
            output.writeInt(pageCount);
        }
        return bytes.toByteArray();
    }

    @Override
    protected void restoreState(byte[] restoredState) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(restoredState))) {
            started = input.readBoolean();
            nextPage = readNullableString(input);
            finished = input.readBoolean();
            lastEmittedId = readNullableString(input);
            duplicateSkipped = input.readLong();
            totalRecords = input.readLong();
            pageCount = input.readInt();
            noMoreSplits = finished;
            noMoreElementSignalled = false;
        } catch (IOException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to restore HugeGraph source pagination state",
                    e);
        }
    }

    private static void writeNullableString(DataOutputStream output, String value)
            throws IOException {
        output.writeBoolean(value != null);
        if (value != null) {
            output.writeUTF(value);
        }
    }

    private static String readNullableString(DataInputStream input) throws IOException {
        return input.readBoolean() ? input.readUTF() : null;
    }

    private void signalNoMoreElement() {
        if (!noMoreElementSignalled) {
            context.signalNoMoreElement();
            noMoreElementSignalled = true;
        }
    }

    private void validateLabelAndSchema() {
        Set<String> labelProperties;
        if (sourceConfig.getLabelType() == MappingConfig.LabelType.VERTEX) {
            labelProperties = client.getVertexLabelPropertiesOrNull(sourceConfig.getLabel());
            if (labelProperties == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Vertex label '%s' does not exist in HugeGraph schema",
                                sourceConfig.getLabel()));
            }
        } else {
            labelProperties = client.getEdgeLabelPropertiesOrNull(sourceConfig.getLabel());
            if (labelProperties == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Edge label '%s' does not exist in HugeGraph schema",
                                sourceConfig.getLabel()));
            }
        }

        for (int i = 0; i < propertyRowType.getTotalFields(); i++) {
            String propertyName = propertyRowType.getFieldName(i);
            if (!labelProperties.contains(propertyName)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Label '%s' does not contain property '%s'. Available properties: %s",
                                sourceConfig.getLabel(), propertyName, labelProperties));
            }
            validatePropertyType(propertyName, propertyRowType.getFieldType(i));
        }
    }

    private void validatePropertyType(String propertyName, SeaTunnelDataType<?> seaTunnelType) {
        Cardinality cardinality = client.getPropertyCardinality(propertyName);
        DataType propertyDataType = client.getPropertyDataType(propertyName);
        boolean serverIsMulti = cardinality != null && cardinality != Cardinality.SINGLE;
        boolean declaredArray = seaTunnelType.getSqlType() == SqlType.ARRAY;

        if (serverIsMulti && !declaredArray) {
            // Guides the user to the fix; without this hint, the server's Collection value would
            // ClassCastException mid-scan against the scalar row builder.
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Property '%s' has cardinality %s on the server but schema.fields "
                                    + "declares it as '%s'. Declare it as 'array<%s>' to read the "
                                    + "collection, or remove it from schema.fields.",
                            propertyName,
                            cardinality,
                            seaTunnelType,
                            toSeaTunnelType(propertyDataType, Cardinality.SINGLE)));
        }
        if (declaredArray && !serverIsMulti) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Property '%s' is declared as ARRAY in schema.fields but has "
                                    + "cardinality SINGLE on the server (type %s).",
                            propertyName, propertyDataType));
        }
        SeaTunnelDataType<?> expectedType = toSeaTunnelType(propertyDataType, cardinality);
        if (!expectedType.equals(seaTunnelType)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Type mismatch for property '%s': schema.fields declares '%s', "
                                    + "but HugeGraph type '%s' (cardinality=%s) maps to '%s'",
                            propertyName,
                            seaTunnelType,
                            propertyDataType,
                            cardinality,
                            expectedType));
        }
    }

    private SeaTunnelDataType<?> toSeaTunnelType(DataType dataType, Cardinality cardinality) {
        SeaTunnelDataType<?> scalar = toSeaTunnelScalarType(dataType);
        if (cardinality == null || cardinality == Cardinality.SINGLE) {
            return scalar;
        }
        // BLOB elements would produce byte[][], which downstream SeaTunnel operators do not
        // uniformly handle — reject with a clear message rather than a mysterious CCE later.
        if (dataType == DataType.BLOB) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Property type BLOB with cardinality %s is not supported for reads.",
                            cardinality));
        }
        return ArrayType.of(scalar);
    }

    private SeaTunnelDataType<?> toSeaTunnelScalarType(DataType dataType) {
        switch (dataType) {
            case TEXT:
                return BasicType.STRING_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case UUID:
                return BasicType.STRING_TYPE;
            case BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Unsupported HugeGraph property type for source: %s", dataType));
        }
    }

    private void collectVertices(List<Vertex> vertices, Collector<SeaTunnelRow> output) {
        for (Vertex vertex : vertices) {
            String id = String.valueOf(vertex.id());
            if (isAdjacentDuplicate(id)) {
                continue;
            }
            Object[] fields = new Object[outputRowType.getTotalFields()];
            fields[0] = id;
            fields[1] = vertex.label();
            fillProperties(vertex.properties(), fields, 2);
            output.collect(new SeaTunnelRow(fields));
        }
    }

    private void collectEdges(List<Edge> edges, Collector<SeaTunnelRow> output) {
        for (Edge edge : edges) {
            String id = String.valueOf(edge.id());
            if (isAdjacentDuplicate(id)) {
                continue;
            }
            Object[] fields = new Object[outputRowType.getTotalFields()];
            fields[0] = id;
            fields[1] = edge.label();
            fields[2] = String.valueOf(edge.sourceId());
            fields[3] = edge.sourceLabel();
            fields[4] = String.valueOf(edge.targetId());
            fields[5] = edge.targetLabel();
            fillProperties(edge.properties(), fields, 6);
            output.collect(new SeaTunnelRow(fields));
        }
    }

    /**
     * The HugeGraph RocksDB backend emits one duplicate record at every internal 500-record scan
     * boundary when limit >= 1000 (observed 2001 duplicates per 1M rows, all back-to-back). Element
     * IDs are unique within a label, so two consecutive identical IDs can only be that server-side
     * paging artifact — skip them with O(1) memory.
     */
    private boolean isAdjacentDuplicate(String id) {
        if (id.equals(lastEmittedId)) {
            duplicateSkipped++;
            return true;
        }
        lastEmittedId = id;
        return false;
    }

    private void fillProperties(
            java.util.Map<String, Object> properties, Object[] fields, int propertyOffset) {
        for (int i = 0; i < propertyRowType.getTotalFields(); i++) {
            String propertyName = propertyRowType.getFieldName(i);
            fields[propertyOffset + i] =
                    convertPropertyValue(
                            properties.get(propertyName), propertyRowType.getFieldType(i));
        }
    }

    private Object convertPropertyValue(Object value, SeaTunnelDataType<?> targetType) {
        if (value == null) {
            return null;
        }
        switch (targetType.getSqlType()) {
            case ARRAY:
                return convertArrayValue(value, (ArrayType<?, ?>) targetType);
            case INT:
                return ((Number) value).intValue();
            case BIGINT:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                return Boolean.parseBoolean(value.toString());
            case BYTES:
                if (value instanceof byte[]) {
                    return value;
                }
                return Base64.getDecoder().decode(value.toString());
            case TIMESTAMP:
                if (value instanceof Date) {
                    return LocalDateTime.ofInstant(((Date) value).toInstant(), getSourceZoneId());
                }
                if (value instanceof Number) {
                    return LocalDateTime.ofInstant(
                            new Date(((Number) value).longValue()).toInstant(), getSourceZoneId());
                }
                return parseDateTime(value.toString());
            case STRING:
                return value.toString();
            default:
                return value;
        }
    }

    /**
     * Converts a HugeGraph LIST/SET property value (returned as a Collection by the client) into a
     * typed SeaTunnel array. HugeGraph 1.5.0 returns LIST as {@code ArrayList} and SET as {@code
     * HashSet}; both flow through {@link Collection} here. SET's original insertion order is not
     * guaranteed by the server, so callers relying on stable ordering must use LIST.
     */
    private Object convertArrayValue(Object value, ArrayType<?, ?> targetType) {
        if (!(value instanceof Collection)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "Expected a Collection for ARRAY property, got %s",
                            value.getClass().getName()));
        }
        Collection<?> collection = (Collection<?>) value;
        SeaTunnelDataType<?> elementType = targetType.getElementType();
        Object array = Array.newInstance(elementType.getTypeClass(), collection.size());
        int i = 0;
        for (Object element : collection) {
            Array.set(array, i++, convertPropertyValue(element, elementType));
        }
        return array;
    }

    private ZoneId getSourceZoneId() {
        return sourceConfig.getTimeZone() == null
                ? ZoneId.systemDefault()
                : ZoneId.of(sourceConfig.getTimeZone());
    }

    /**
     * Parses a HugeGraph DATE property returned as a String. The server serializes dates as {@code
     * yyyy-MM-dd HH:mm:ss.SSS} (space separator, optional fractional seconds), which {@link
     * LocalDateTime#parse} rejects because it only accepts the ISO 'T' separator. Accept both the
     * space-separated server format and ISO-8601 (in case a future/config variant emits a 'T').
     */
    private static LocalDateTime parseDateTime(String text) {
        try {
            return LocalDateTime.parse(text, HUGEGRAPH_DATE_FORMAT);
        } catch (DateTimeParseException e) {
            return LocalDateTime.parse(text);
        }
    }
}
