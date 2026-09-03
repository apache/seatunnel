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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphOperations;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.PageResult;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.HugeGraphTypeConverter;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Reads HugeGraph vertices/edges into SeaTunnel rows, one assigned split at a time.
 *
 * <p>A {@code LABEL_LIST} split pages the whole label via the server-side list API (with optional
 * server-side property filtering). A {@code SHARD} split scans a key range via the scan API and
 * filters to the configured label client-side (the scan API cannot filter by label or property).
 * Progress (page marker, finished flag, dedup id) is stored on the split so a checkpoint can resume
 * mid-scan after failover.
 */
public class HugeGraphSourceReader implements SourceReader<SeaTunnelRow, HugeGraphSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSourceReader.class);

    private static final long IDLE_POLL_INTERVAL_MS = 1000L;

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

    private final SourceReader.Context context;
    private final HugeGraphSourceConfig sourceConfig;
    // Per-label read context, keyed by label. Single-label mode has exactly one entry; read-all
    // mode has one per discovered label. The reader resolves the entry by each split's active
    // label.
    private final Map<String, LabelTableContext> labelContexts;
    private final HugeGraphOperations client;

    private final Deque<HugeGraphSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private HugeGraphSourceSplit currentSplit;
    private volatile boolean noMoreSplits;

    // Dedup state for the split currently being read; mirrored to/from currentSplit around each
    // page
    // so it survives checkpoints.
    private String lastEmittedId;
    private long duplicateSkipped;
    private long totalRecords;
    private int pageCount;

    public HugeGraphSourceReader(
            SourceReader.Context context,
            HugeGraphSourceConfig sourceConfig,
            Map<String, LabelTableContext> labelContexts) {
        this(
                context,
                sourceConfig,
                labelContexts,
                new HugeGraphClient(sourceConfig.getConnectionConfig()));
    }

    HugeGraphSourceReader(
            SourceReader.Context context,
            HugeGraphSourceConfig sourceConfig,
            Map<String, LabelTableContext> labelContexts,
            HugeGraphOperations client) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.labelContexts = labelContexts;
        this.client = client;
    }

    /**
     * The label the given split reads: a LABEL_LIST split names it directly; a SHARD split scans a
     * key range of all labels and inherits the single configured label (shard splits are only
     * created in single-label mode).
     */
    private String activeLabel(HugeGraphSourceSplit split) {
        return split.getLabel() != null ? split.getLabel() : sourceConfig.getLabel();
    }

    private LabelTableContext contextFor(String label) {
        LabelTableContext ctx = labelContexts.get(label);
        if (ctx == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format("No read context for label '%s'.", label));
        }
        return ctx;
    }

    @Override
    public void open() {
        // Read-all mode auto-discovers each label's row type from the server, so there is no user
        // schema to validate against (it cannot mismatch). Skip validation; the reader resolves
        // each split's context by label at read time.
        if (sourceConfig.isReadAllLabels()) {
            return;
        }
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
    public void addSplits(List<HugeGraphSourceSplit> splits) {
        if (splits != null) {
            pendingSplits.addAll(splits);
        }
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public List<HugeGraphSourceSplit> snapshotState(long checkpointId) {
        List<HugeGraphSourceSplit> state = new ArrayList<>();
        HugeGraphSourceSplit cur = currentSplit;
        if (cur != null && !cur.isFinished()) {
            state.add(cur);
        }
        state.addAll(pendingSplits);
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // No-op: the source keeps no server-side cursor to acknowledge.
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws InterruptedException {
        boolean idle = false;
        synchronized (output.getCheckpointLock()) {
            if (currentSplit == null) {
                currentSplit = pendingSplits.poll();
            }
            if (currentSplit == null) {
                if (noMoreSplits && Boundedness.BOUNDED.equals(context.getBoundedness())) {
                    context.signalNoMoreElement();
                } else {
                    context.sendSplitRequest();
                    idle = true;
                }
            } else {
                readOnePage(currentSplit, output);
            }
        }
        if (idle) {
            Thread.sleep(IDLE_POLL_INTERVAL_MS);
        }
    }

    /** Reads one bounded page of the current split so checkpoints can persist progress. */
    private void readOnePage(HugeGraphSourceSplit split, Collector<SeaTunnelRow> output) {
        String requestedPage = split.getPage();
        this.lastEmittedId = split.getLastEmittedId();
        String label = activeLabel(split);
        LabelTableContext ctx = contextFor(label);

        int recordCount;
        String responsePage;
        if (sourceConfig.getLabelType() == MappingConfig.LabelType.VERTEX) {
            PageResult<Vertex> page = fetchVertexPage(split, label, requestedPage);
            List<Vertex> records =
                    split.isShardMode()
                            ? filterVerticesByLabel(page.getRecords(), label)
                            : page.getRecords();
            collectVertices(records, output, ctx);
            recordCount = page.getRecords().size();
            responsePage = page.getNextPage();
        } else {
            PageResult<Edge> page = fetchEdgePage(split, label, requestedPage);
            List<Edge> records =
                    split.isShardMode()
                            ? filterEdgesByLabel(page.getRecords(), label)
                            : page.getRecords();
            collectEdges(records, output, ctx);
            recordCount = page.getRecords().size();
            responsePage = page.getNextPage();
        }

        if (responsePage != null && responsePage.equals(requestedPage)) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "HugeGraph pagination marker did not advance for split '%s': '%s'",
                            split.splitId(), responsePage));
        }

        split.setLastEmittedId(this.lastEmittedId);
        split.setPage(responsePage);
        totalRecords += recordCount;
        pageCount++;
        if (recordCount == 0 && responsePage != null) {
            LOG.debug(
                    "HugeGraph source received an empty intermediate page for split '{}'; continuing",
                    split.splitId());
        }
        if (responsePage == null) {
            split.setFinished(true);
            LOG.info(
                    "HugeGraph source finished split '{}' (label '{}'): {} records in {} pages"
                            + " ({} server-side paging duplicates skipped)",
                    split.splitId(),
                    label,
                    totalRecords,
                    pageCount,
                    duplicateSkipped);
            // Reset per-split counters for the next split.
            totalRecords = 0;
            pageCount = 0;
            duplicateSkipped = 0;
            currentSplit = null;
        }
    }

    private PageResult<Vertex> fetchVertexPage(
            HugeGraphSourceSplit split, String label, String requestedPage) {
        if (split.isShardMode()) {
            return client.scanVertices(split.toShard(), requestedPage, sourceConfig.getPageSize());
        }
        return client.listVertices(
                label, sourceConfig.getFilter(), requestedPage, sourceConfig.getPageSize());
    }

    private PageResult<Edge> fetchEdgePage(
            HugeGraphSourceSplit split, String label, String requestedPage) {
        if (split.isShardMode()) {
            return client.scanEdges(split.toShard(), requestedPage, sourceConfig.getPageSize());
        }
        return client.listEdges(
                label, sourceConfig.getFilter(), requestedPage, sourceConfig.getPageSize());
    }

    /**
     * Shard scans return elements of all labels in the key range; keep only {@code label}. (Shard
     * mode is single-label only, so this filters to the one configured label.)
     */
    private List<Vertex> filterVerticesByLabel(List<Vertex> records, String label) {
        List<Vertex> filtered = new ArrayList<>(records.size());
        for (Vertex vertex : records) {
            if (label.equals(vertex.label())) {
                filtered.add(vertex);
            }
        }
        return filtered;
    }

    private List<Edge> filterEdgesByLabel(List<Edge> records, String label) {
        List<Edge> filtered = new ArrayList<>(records.size());
        for (Edge edge : records) {
            if (label.equals(edge.label())) {
                filtered.add(edge);
            }
        }
        return filtered;
    }

    private void validateLabelAndSchema() {
        // Single-label mode only (read-all skips validation in open()); exactly one context, keyed
        // by the configured label.
        SeaTunnelRowType propertyRowType = contextFor(sourceConfig.getLabel()).getPropertyRowType();
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

        // A filter keyed on a non-existent property would be silently dropped by the server and
        // return the whole label — fail fast so the misconfiguration surfaces at open() instead.
        // Values are also coerced to the property's server type: the server matches by typed value,
        // so a BOOLEAN property filtered with the string "true" (or a LONG filtered with an int)
        // would otherwise match nothing and return 0 rows with no error.
        Map<String, Object> filter = sourceConfig.getFilter();
        if (filter != null && !filter.isEmpty()) {
            Map<String, Object> coerced = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : filter.entrySet()) {
                String key = entry.getKey();
                if (!labelProperties.contains(key)) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                            String.format(
                                    "filter property '%s' is not a property of label '%s'. "
                                            + "Available properties: %s",
                                    key, sourceConfig.getLabel(), labelProperties));
                }
                coerced.put(
                        key,
                        coerceFilterValue(key, entry.getValue(), client.getPropertyDataType(key)));
            }
            sourceConfig.setFilter(coerced);
        }
    }

    /**
     * Coerces a filter value to the Java type the HugeGraph server matches against for the
     * property's type — config often supplies a string or a loosely-typed number. A value that
     * cannot be coerced (e.g. {@code "yes"} for a BOOLEAN) fails fast here instead of silently
     * matching nothing. DATE/BLOB/OBJECT are passed through unchanged (not sensibly filterable).
     */
    static Object coerceFilterValue(String key, Object value, DataType dataType) {
        if (value == null) {
            return null;
        }
        String raw = value.toString().trim();
        try {
            switch (dataType) {
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    }
                    if ("true".equalsIgnoreCase(raw)) {
                        return Boolean.TRUE;
                    }
                    if ("false".equalsIgnoreCase(raw)) {
                        return Boolean.FALSE;
                    }
                    throw new IllegalArgumentException("expected true or false");
                case BYTE:
                    return value instanceof Number
                            ? ((Number) value).byteValue()
                            : Byte.valueOf(raw);
                case INT:
                    return value instanceof Number
                            ? ((Number) value).intValue()
                            : Integer.valueOf(raw);
                case LONG:
                    return value instanceof Number
                            ? ((Number) value).longValue()
                            : Long.valueOf(raw);
                case FLOAT:
                    return value instanceof Number
                            ? ((Number) value).floatValue()
                            : Float.valueOf(raw);
                case DOUBLE:
                    return value instanceof Number
                            ? ((Number) value).doubleValue()
                            : Double.valueOf(raw);
                case TEXT:
                case UUID:
                    return raw;
                default:
                    return value;
            }
        } catch (RuntimeException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "filter value '%s' for property '%s' is not a valid %s.",
                            value, key, dataType),
                    e);
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
                            toSeaTunnelType(propertyDataType, Cardinality.SINGLE, propertyName)));
        }
        if (declaredArray && !serverIsMulti) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Property '%s' is declared as ARRAY in schema.fields but has "
                                    + "cardinality SINGLE on the server (type %s).",
                            propertyName, propertyDataType));
        }
        SeaTunnelDataType<?> expectedType =
                toSeaTunnelType(propertyDataType, cardinality, propertyName);
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

    private SeaTunnelDataType<?> toSeaTunnelType(
            DataType dataType, Cardinality cardinality, String propertyName) {
        return HugeGraphTypeConverter.toSeaTunnelType(dataType, cardinality, propertyName);
    }

    private void collectVertices(
            List<Vertex> vertices, Collector<SeaTunnelRow> output, LabelTableContext ctx) {
        SeaTunnelRowType outputRowType = ctx.getOutputRowType();
        for (Vertex vertex : vertices) {
            String id = String.valueOf(vertex.id());
            if (isAdjacentDuplicate(id)) {
                continue;
            }
            Object[] fields = new Object[outputRowType.getTotalFields()];
            fields[0] = id;
            fields[1] = vertex.label();
            fillProperties(vertex.properties(), fields, 2, ctx.getPropertyRowType());
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setTableId(ctx.getTableId());
            output.collect(row);
        }
    }

    private void collectEdges(
            List<Edge> edges, Collector<SeaTunnelRow> output, LabelTableContext ctx) {
        SeaTunnelRowType outputRowType = ctx.getOutputRowType();
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
            fillProperties(edge.properties(), fields, 6, ctx.getPropertyRowType());
            SeaTunnelRow row = new SeaTunnelRow(fields);
            row.setTableId(ctx.getTableId());
            output.collect(row);
        }
    }

    /**
     * The HugeGraph RocksDB backend emits one duplicate record at every internal 500-record scan
     * boundary when limit &gt;= 1000 (observed 2001 duplicates per 1M rows, all back-to-back).
     * Element IDs are unique within a label, so two consecutive identical IDs can only be that
     * server-side paging artifact — skip them with O(1) memory.
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
            Map<String, Object> properties,
            Object[] fields,
            int propertyOffset,
            SeaTunnelRowType propertyRowType) {
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
            case TINYINT:
                return ((Number) value).byteValue();
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
                // A server-serialized wall-clock string carries no zone, so time_zone cannot be
                // applied here without knowing the server's serialization zone — keep the value
                // verbatim (documented on the time_zone option). time_zone applies only to the
                // epoch/Date branches above.
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
