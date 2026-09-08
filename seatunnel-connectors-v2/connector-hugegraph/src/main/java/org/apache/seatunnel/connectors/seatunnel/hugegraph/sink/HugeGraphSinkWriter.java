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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer.BatchBuffer;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer.GraphElementEnvelope;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.EdgeMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.GraphDataMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.VertexMapper;

import org.apache.hugegraph.structure.GraphElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HugeGraphSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSinkWriter.class);

    private final HugeGraphSinkConfig sinkConfig;
    private final List<MappingEntry> mappingEntries;
    private final HugeGraphClient client;
    private final BatchBuffer buffer;
    private final String tablePath;

    // Holds the UPDATE_BEFORE row until its paired UPDATE_AFTER arrives (changelog streams
    // deliver them consecutively). This field is NOT checkpointable — the SeaTunnel SinkWriter
    // interface has no snapshotState(). If a checkpoint fires between UPDATE_BEFORE and
    // UPDATE_AFTER, prepareCommit() fails fast rather than risking a partially-applied mutation.
    private SeaTunnelRow pendingUpdateBefore;

    public HugeGraphSinkWriter(HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType) {
        this(sinkConfig, rowType, "", (SinkWriter.Context) null, 0);
    }

    public HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType, int subtaskIndex) {
        this(sinkConfig, rowType, "", (SinkWriter.Context) null, subtaskIndex);
    }

    public HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            String tablePath,
            int subtaskIndex) {
        this(sinkConfig, rowType, tablePath, (SinkWriter.Context) null, subtaskIndex);
    }

    public HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            String tablePath,
            SinkWriter.Context context) {
        this(sinkConfig, rowType, tablePath, context, context.getIndexOfSubtask());
    }

    private HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            String tablePath,
            SinkWriter.Context context,
            int subtaskIndex) {
        this(
                sinkConfig,
                rowType,
                tablePath,
                new HugeGraphClient(sinkConfig.getConnectionConfig()),
                context,
                subtaskIndex);
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType, HugeGraphClient client) {
        this(sinkConfig, rowType, "", client, null, 0);
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            HugeGraphClient client,
            int subtaskIndex) {
        this(sinkConfig, rowType, "", client, null, subtaskIndex);
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            HugeGraphClient client,
            SinkWriter.Context context,
            int subtaskIndex) {
        this(sinkConfig, rowType, "", client, context, subtaskIndex);
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            String tablePath,
            HugeGraphClient client,
            int subtaskIndex) {
        this(sinkConfig, rowType, tablePath, client, null, subtaskIndex);
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig,
            SeaTunnelRowType rowType,
            String tablePath,
            HugeGraphClient client,
            SinkWriter.Context context,
            int subtaskIndex) {
        this.sinkConfig = sinkConfig;
        this.tablePath = tablePath;
        this.sinkConfig.applyLegacyFieldSelection(rowType);
        this.client = client;
        try {
            // buildMappingEntries issues live schema lookups; if any fails the framework will not
            // call close() on this half-constructed writer, so release the client here.
            this.mappingEntries = buildMappingEntries(rowType);
        } catch (RuntimeException e) {
            try {
                this.client.close();
            } catch (RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        this.buffer =
                new BatchBuffer(
                        this.client,
                        sinkConfig.getBatchSize(),
                        sinkConfig.getBatchIntervalMs(),
                        sinkConfig.isBatchFailureFallback(),
                        sinkConfig.isCheckVertex(),
                        sinkConfig.getMaxInsertErrors(),
                        sinkConfig.getFailureDataPath(),
                        subtaskIndex);
        if (context != null) {
            context.registerFlushAction(buffer::flush);
        }
    }

    private List<MappingEntry> buildMappingEntries(SeaTunnelRowType rowType) {
        Map<String, Integer> originalFieldsIndex =
                IntStream.range(0, rowType.getTotalFields())
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        rowType::getFieldName,
                                        i -> i,
                                        (a, b) -> a,
                                        LinkedHashMap::new));
        Map<String, Integer> availableFieldsIndex = resolveLegacyFieldsIndex(originalFieldsIndex);

        List<MappingEntry> entries = new ArrayList<>();
        for (MappingConfig mapping : sinkConfig.getMappings()) {
            // Multi-table binding: when a mapping declares source_table, only activate it in the
            // writer whose tablePath matches. A mapping without source_table activates in every
            // writer — the single-table backward-compatible default.
            if (!mapping.appliesTo(tablePath)) {
                LOG.info(
                        "Mapping[{}/{}] source_table '{}' does not match writer table '{}'; "
                                + "skipping in this writer.",
                        mapping.getType(),
                        mapping.getLabel(),
                        mapping.getSourceTable(),
                        tablePath);
                continue;
            }
            Map<String, Integer> fieldsIndex = resolveFieldsIndex(mapping, availableFieldsIndex);
            GraphDataMapper mapper;
            if (mapping.getType() == LabelType.VERTEX) {
                if (mapping.getIdStrategy()
                        == org.apache.hugegraph.structure.constant.IdStrategy.AUTOMATIC) {
                    // AUTOMATIC ids are server-assigned and not derivable from the row, so there is
                    // no key to deduplicate on: under at-least-once, a replayed row inserts a NEW
                    // vertex each time (duplicates). Warn so the trade-off is visible; use
                    // PRIMARY_KEY / CUSTOMIZE_* for idempotent upserts.
                    LOG.warn(
                            "Mapping[VERTEX/{}] uses AUTOMATIC ids: under at-least-once delivery a "
                                    + "replayed row creates a duplicate vertex, and DELETE is not "
                                    + "supported. Use PRIMARY_KEY or CUSTOMIZE_* ids for idempotent "
                                    + "writes.",
                            mapping.getLabel());
                }
                mapper = new VertexMapper(mapping, fieldsIndex, client);
            } else {
                mapper = new EdgeMapper(mapping, fieldsIndex, client);
            }
            entries.add(new MappingEntry(mapping, mapper));
        }

        if (entries.isEmpty()) {
            // Multi-table mode: at least one mapping has source_table, but none matched this
            // writer's tablePath. This is a configuration error — the user intended multi-table
            // but the table path strings don't align. Fail fast with a diagnostic that shows
            // both sides so the user can reconcile them.
            boolean multiTable =
                    sinkConfig.getMappings().stream()
                            .anyMatch(
                                    m ->
                                            m.getSourceTable() != null
                                                    && !m.getSourceTable().isEmpty());
            if (multiTable) {
                List<String> configuredTables =
                        sinkConfig.getMappings().stream()
                                .map(MappingConfig::getSourceTable)
                                .filter(t -> t != null && !t.isEmpty())
                                .distinct()
                                .collect(Collectors.toList());
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "No mapping matched writer table '%s'. The configured "
                                        + "source_table value(s) are %s. Verify that each "
                                        + "source_table matches the CatalogTable.getTablePath() "
                                        + "produced by the upstream Source (in read-all mode this "
                                        + "is the HugeGraph label name).",
                                tablePath, configuredTables));
            }

            // Single-table mode (no source_table set on any mapping): no entries means the
            // row type carries no fields the mappings can use. This is unusual but not
            // necessarily a config error — a downstream project may legitimately sink only a
            // subset of fields. Log and continue as a no-op.
            LOG.warn(
                    "No mappings matched table '{}' in this writer. The writer will be a no-op: "
                            + "rows are acknowledged without writing to HugeGraph.",
                    tablePath);
        }
        return entries;
    }

    private Map<String, Integer> resolveLegacyFieldsIndex(
            Map<String, Integer> originalFieldsIndex) {
        if (sinkConfig.getSchemaConfig() == null) {
            return originalFieldsIndex;
        }

        List<String> selectedFields = sinkConfig.getSelectedFields();
        if (selectedFields != null && !selectedFields.isEmpty()) {
            Map<String, Integer> selected = new LinkedHashMap<>();
            for (String field : selectedFields) {
                Integer index = originalFieldsIndex.get(field);
                if (index != null) {
                    selected.put(field, index);
                }
            }
            return selected;
        }

        List<String> ignoredFields = sinkConfig.getIgnoredFields();
        if (ignoredFields != null && !ignoredFields.isEmpty()) {
            Set<String> ignored = new HashSet<>(ignoredFields);
            Map<String, Integer> selected = new LinkedHashMap<>();
            for (Map.Entry<String, Integer> entry : originalFieldsIndex.entrySet()) {
                if (!ignored.contains(entry.getKey())) {
                    selected.put(entry.getKey(), entry.getValue());
                }
            }
            return selected;
        }
        return originalFieldsIndex;
    }

    private Map<String, Integer> resolveFieldsIndex(
            MappingConfig mapping, Map<String, Integer> originalFieldsIndex) {
        // If no explicit properties, use all fields from the row
        if (mapping.getProperties().isEmpty()) {
            return new LinkedHashMap<>(originalFieldsIndex);
        }

        // Build index from explicit properties + id fields
        Map<String, Integer> result = new LinkedHashMap<>();

        for (String field : mapping.getProperties()) {
            Integer idx = originalFieldsIndex.get(field);
            if (idx != null) {
                result.put(field, idx);
            }
        }

        // New mappings always include fields required to build IDs. Legacy selected_fields keeps
        // its original strict filtering behavior for backward compatibility.
        if (sinkConfig.getSchemaConfig() == null && mapping.getIdFields() != null) {
            for (String field : mapping.getIdFields()) {
                Integer idx = originalFieldsIndex.get(field);
                if (idx != null) {
                    result.put(field, idx);
                }
            }
        }

        // For edges, include source/target idFields and sortKeys
        if (sinkConfig.getSchemaConfig() == null && mapping.getType() == LabelType.EDGE) {
            includeEdgeIdFields(mapping.getSourceConfig(), originalFieldsIndex, result);
            includeEdgeIdFields(mapping.getTargetConfig(), originalFieldsIndex, result);

            for (String field : mapping.getSortKeys()) {
                Integer idx = originalFieldsIndex.get(field);
                if (idx != null) {
                    result.put(field, idx);
                }
            }
        }

        return result;
    }

    private void includeEdgeIdFields(
            MappingConfig.SourceTargetConfig stConfig,
            Map<String, Integer> originalFieldsIndex,
            Map<String, Integer> result) {
        if (stConfig != null && stConfig.getIdFields() != null) {
            for (String field : stConfig.getIdFields()) {
                Integer idx = originalFieldsIndex.get(field);
                if (idx != null) {
                    result.put(field, idx);
                }
            }
        }
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        switch (row.getRowKind()) {
            case INSERT:
                handleUpsert(row);
                break;
            case UPDATE_AFTER:
                handleUpdate(pendingUpdateBefore, row);
                pendingUpdateBefore = null;
                break;
            case DELETE:
                handleDelete(row);
                break;
            case UPDATE_BEFORE:
                // Correlated with the immediately following UPDATE_AFTER (changelog contract) and
                // handled together in handleUpdate, so a key-changing update deletes the
                // pre-update element instead of leaving it orphaned.
                pendingUpdateBefore = row;
                break;
            default:
                LOG.warn("Unsupported row kind: {}", row.getRowKind());
                break;
        }
    }

    private void handleUpsert(SeaTunnelRow row) throws IOException {
        List<GraphElementEnvelope> vertexEnvelopes = new ArrayList<>();
        List<GraphElementEnvelope> edgeEnvelopes = new ArrayList<>();

        for (MappingEntry entry : mappingEntries) {
            // mapToEnvelopes returns 1 element normally, or N when unfold expands a list cell.
            for (GraphElementEnvelope envelope : mapToEnvelopes(entry, row)) {
                if (entry.config.getType() == LabelType.VERTEX) {
                    vertexEnvelopes.add(envelope);
                } else {
                    edgeEnvelopes.add(envelope);
                }
            }
        }

        for (GraphElementEnvelope envelope : vertexEnvelopes) {
            buffer.add(envelope);
        }
        for (GraphElementEnvelope envelope : edgeEnvelopes) {
            buffer.add(envelope);
        }
    }

    /**
     * unfold (one row → many elements) is only defined for the append/INSERT path. UPDATE/DELETE
     * would require diffing N old ids against N new ids per mapping, which is out of scope and
     * dangerous to get wrong, so reject a changelog row when any mapping enables unfold.
     */
    private void rejectUnfoldForChangelog(String rowKind) {
        for (MappingEntry entry : mappingEntries) {
            if (entry.mapper.isUnfoldEnabled()) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Mapping[%s/%s]: unfold is only supported for INSERT/append-only "
                                        + "jobs, but received a %s row. Disable unfold or run this "
                                        + "as an append-only job.",
                                entry.config.getType(), entry.config.getLabel(), rowKind));
            }
        }
    }

    /**
     * AUTOMATIC-id vertices have no client-derivable id (the server assigns it), so a DELETE cannot
     * identify the target. Reject with a clear message instead of the misleading "required ID field
     * is null" — there is no id field to be null. Package-private + static so it can be unit-tested
     * without constructing a real writer/client.
     */
    static void rejectAutomaticVertexDelete(List<MappingEntry> mappingEntries) {
        for (MappingEntry entry : mappingEntries) {
            if (entry.config.getType() == LabelType.VERTEX
                    && entry.config.getIdStrategy()
                            == org.apache.hugegraph.structure.constant.IdStrategy.AUTOMATIC) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Mapping[VERTEX/%s]: DELETE is not supported with AUTOMATIC IDs "
                                        + "because the target vertex cannot be identified from row "
                                        + "data. Use a PRIMARY_KEY or CUSTOMIZE_* id strategy if this "
                                        + "stream emits deletes.",
                                entry.config.getLabel()));
            }
        }
    }

    /**
     * Applies a changelog update transactionally with respect to the after-image validity.
     *
     * <p>The previous implementation issued the pre-update delete first and then upserted the
     * after-image; if the after-image mapping failed (null ID, unsupported type conversion, or an
     * AUTOMATIC-vertex mapping on update) the old vertex/edge had already been deleted, so the row
     * was lost. This implementation builds a full replacement plan — envelopes for every mapping's
     * after-image plus the set of superseded IDs — BEFORE touching the server. Any failure during
     * plan-building raises without any remote side effect, so the pre-update elements stay intact
     * and the source can replay the row after the config is fixed.
     *
     * <p>A superseded (old) element is deleted only when its mapping also produced a replacement
     * after-image. If the after-image is absent — the after row cannot be mapped for that mapping,
     * e.g. a null id field made {@code map()} return null — the old element is left untouched
     * rather than deleted-with-nothing-written; a real removal must arrive as a DELETE event.
     *
     * <p>Note: HugeGraph server DDL is non-transactional across a flush+delete pair, so a crash
     * between the two still leaves partial state; that is an inherent limitation of the REST API
     * and is handled by at-least-once replay from the upstream source.
     */
    private void handleUpdate(SeaTunnelRow before, SeaTunnelRow after) throws IOException {
        rejectUnfoldForChangelog("UPDATE");
        UpdatePlan plan = buildUpdatePlan(mappingEntries, before, after);
        executeUpdatePlan(plan);
    }

    /**
     * Package-private + static so the "no side effect on mapping failure" invariant can be pinned
     * by a unit test without constructing a real writer/client.
     */
    static UpdatePlan buildUpdatePlan(
            List<MappingEntry> mappingEntries, SeaTunnelRow before, SeaTunnelRow after) {
        List<GraphElementEnvelope> newVertices = new ArrayList<>();
        List<GraphElementEnvelope> newEdges = new ArrayList<>();
        List<Superseded> supersededVertices = new ArrayList<>();
        List<Superseded> supersededEdges = new ArrayList<>();

        Set<MappingEntry> producedAfterImage = Collections.newSetFromMap(new IdentityHashMap<>());
        for (MappingEntry entry : mappingEntries) {
            GraphElementEnvelope envelope = mapToEnvelope(entry, after, true);
            if (envelope == null) {
                continue;
            }
            producedAfterImage.add(entry);
            if (entry.config.getType() == LabelType.VERTEX) {
                newVertices.add(envelope);
            } else {
                newEdges.add(envelope);
            }
        }

        if (before != null) {
            for (MappingEntry entry : mappingEntries) {
                // Only delete the pre-update element when this mapping produced a replacement
                // after-image. If the after-image is absent (e.g. the after row has a null id
                // field so map() returned null), deleting the old element would drop it with
                // nothing written back — a silent data loss. Keeping the old element is the safe
                // choice; a genuine removal should arrive as a DELETE changelog event.
                if (!producedAfterImage.contains(entry)) {
                    continue;
                }
                Object oldId;
                Object newId;
                try {
                    oldId = entry.mapper.extractId(before);
                    newId = entry.mapper.extractId(after);
                } catch (Exception e) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                            String.format(
                                    "Mapping[%s/%s]: Failed to compute graph element ID for update",
                                    entry.config.getType(), entry.config.getLabel()),
                            e);
                }
                if (oldId == null || oldId.equals(newId)) {
                    continue;
                }
                Superseded s = new Superseded(entry, oldId);
                if (entry.config.getType() == LabelType.VERTEX) {
                    supersededVertices.add(s);
                } else {
                    supersededEdges.add(s);
                }
            }
        }

        return new UpdatePlan(newVertices, newEdges, supersededVertices, supersededEdges);
    }

    private void executeUpdatePlan(UpdatePlan plan) throws IOException {
        // Buffer new envelopes first — mirrors handleUpsert ordering for INSERT and ensures the
        // new elements are staged before any destructive operation.
        for (GraphElementEnvelope envelope : plan.newVertices) {
            buffer.add(envelope);
        }
        for (GraphElementEnvelope envelope : plan.newEdges) {
            buffer.add(envelope);
        }

        if (plan.supersededVertices.isEmpty() && plan.supersededEdges.isEmpty()) {
            // ID unchanged — the upsert alone updates the element in place and a vertex's
            // adjacent edges are preserved.
            return;
        }

        // Persist the new elements before issuing deletes. If flush fails, no delete happens, so
        // pre-update elements stay intact and the source will replay the row.
        // NOTE: HugeGraph server DDL is non-transactional across a flush+delete pair, so a crash
        // between the two leaves partial state; this is an inherent limitation of the REST API
        // and is handled by at-least-once replay from the upstream source. The SinkWriter
        // interface has no snapshotState(), so the connector cannot checkpoint the in-flight
        // mutation. prepareCommit() guards against a pending UPDATE_BEFORE crossing a checkpoint
        // boundary by failing fast.
        try {
            buffer.flush();
        } catch (IOException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to flush buffer before UPDATE cleanup",
                    e);
        }

        // Delete edges before vertices for topology safety (mirror handleDelete).
        for (Superseded s : plan.supersededEdges) {
            try {
                client.deleteEdge((String) s.oldId);
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        String.format(
                                "Mapping[%s/%s]: Failed to delete superseded edge on update",
                                s.entry.config.getType(), s.entry.config.getLabel()),
                        e);
            }
        }
        for (Superseded s : plan.supersededVertices) {
            try {
                if (sinkConfig.isDeleteVertexWithEdges()) {
                    client.deleteVertexWithEdges(s.oldId);
                } else {
                    client.deleteVertex(s.oldId);
                }
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        String.format(
                                "Mapping[%s/%s]: Failed to delete superseded vertex on update",
                                s.entry.config.getType(), s.entry.config.getLabel()),
                        e);
            }
        }
    }

    /**
     * Maps a row into an envelope for one mapping, or returns {@code null} if the mapper does not
     * produce an element for this row (e.g. a null ID field). Reject AUTOMATIC-vertex mappings on
     * update because the existing vertex cannot be identified.
     */
    static GraphElementEnvelope mapToEnvelope(
            MappingEntry entry, SeaTunnelRow row, boolean update) {
        if (update
                && entry.config.getType() == LabelType.VERTEX
                && entry.config.getIdStrategy()
                        == org.apache.hugegraph.structure.constant.IdStrategy.AUTOMATIC) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Mapping[VERTEX/%s]: UPDATE_AFTER is not supported with AUTOMATIC IDs because the existing vertex cannot be identified",
                            entry.config.getLabel()));
        }
        GraphElement element;
        try {
            element = entry.mapper.map(row);
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "Mapping[%s/%s]: Failed to map input row to graph element",
                            entry.config.getType(), entry.config.getLabel()),
                    e);
        }
        if (element == null) {
            return null;
        }
        return new GraphElementEnvelope(
                entry.config.getLabel(),
                entry.config.getType(),
                element,
                entry.config.getUpdateStrategies());
    }

    /**
     * INSERT/append-path mapping that supports unfold: returns one envelope normally, or N when a
     * mapping expands a list-valued id cell into multiple elements.
     */
    static List<GraphElementEnvelope> mapToEnvelopes(MappingEntry entry, SeaTunnelRow row) {
        List<GraphElement> elements;
        try {
            elements = entry.mapper.mapAll(row);
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    String.format(
                            "Mapping[%s/%s]: Failed to map input row to graph element(s)",
                            entry.config.getType(), entry.config.getLabel()),
                    e);
        }
        if (elements == null || elements.isEmpty()) {
            return Collections.emptyList();
        }
        List<GraphElementEnvelope> envelopes = new ArrayList<>(elements.size());
        for (GraphElement element : elements) {
            if (element == null) {
                continue;
            }
            envelopes.add(
                    new GraphElementEnvelope(
                            entry.config.getLabel(),
                            entry.config.getType(),
                            element,
                            entry.config.getUpdateStrategies()));
        }
        return envelopes;
    }

    static final class UpdatePlan {
        final List<GraphElementEnvelope> newVertices;
        final List<GraphElementEnvelope> newEdges;
        final List<Superseded> supersededVertices;
        final List<Superseded> supersededEdges;

        UpdatePlan(
                List<GraphElementEnvelope> newVertices,
                List<GraphElementEnvelope> newEdges,
                List<Superseded> supersededVertices,
                List<Superseded> supersededEdges) {
            this.newVertices = newVertices;
            this.newEdges = newEdges;
            this.supersededVertices = supersededVertices;
            this.supersededEdges = supersededEdges;
        }
    }

    static final class Superseded {
        final MappingEntry entry;
        final Object oldId;

        Superseded(MappingEntry entry, Object oldId) {
            this.entry = entry;
            this.oldId = oldId;
        }
    }

    private void handleDelete(SeaTunnelRow row) {
        rejectUnfoldForChangelog("DELETE");
        rejectAutomaticVertexDelete(mappingEntries);

        // Phase 1: build deletion plan — validate ALL mappings before touching the server.
        // Any failure here (null id, missing field) aborts with zero side effects.
        DeletePlan deletePlan = buildDeletePlan(row);

        // Phase 2: execute the plan atomically — flush pending inserts first, then execute all
        // deletions. The flush ensures earlier INSERT/UPSERT rows are persisted before their
        // elements are deleted (a DELETE arriving immediately after its own INSERT).
        try {
            buffer.flush();
        } catch (IOException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to flush buffer before DELETE operation",
                    e);
        }

        executeDeletePlan(deletePlan);
    }

    /**
     * Validates every mapping's ability to extract a delete ID from the row WITHOUT issuing any
     * remote operation. Returns the validated plan — if this method returns, every ID is valid and
     * the caller can safely execute them.
     */
    static DeletePlan buildDeletePlan(
            List<MappingEntry> mappingEntries, SeaTunnelRow row, HugeGraphSinkConfig sinkConfig) {
        List<DeleteTarget> edgeTargets = new ArrayList<>();
        List<DeleteTarget> vertexTargets = new ArrayList<>();

        for (MappingEntry entry : mappingEntries) {
            Object id = entry.mapper.extractId(row);
            if (id == null) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                        String.format(
                                "Mapping[%s/%s]: Cannot delete because a required ID field is null "
                                        + "or matches nullValues",
                                entry.config.getType(), entry.config.getLabel()));
            }
            if (entry.config.getType() == LabelType.VERTEX) {
                vertexTargets.add(
                        new DeleteTarget(entry, id, sinkConfig.isDeleteVertexWithEdges()));
            } else {
                edgeTargets.add(new DeleteTarget(entry, id, false));
            }
        }

        return new DeletePlan(edgeTargets, vertexTargets);
    }

    private DeletePlan buildDeletePlan(SeaTunnelRow row) {
        return buildDeletePlan(mappingEntries, row, sinkConfig);
    }

    private void executeDeletePlan(DeletePlan plan) {
        // Edges before vertices for topology safety.
        for (DeleteTarget target : plan.edgeTargets) {
            client.deleteEdge((String) target.id);
        }
        for (DeleteTarget target : plan.vertexTargets) {
            if (target.deleteWithEdges) {
                client.deleteVertexWithEdges(target.id);
            } else {
                client.deleteVertex(target.id);
            }
        }
    }

    static final class DeletePlan {
        final List<DeleteTarget> edgeTargets;
        final List<DeleteTarget> vertexTargets;

        DeletePlan(List<DeleteTarget> edgeTargets, List<DeleteTarget> vertexTargets) {
            this.edgeTargets = edgeTargets;
            this.vertexTargets = vertexTargets;
        }
    }

    static final class DeleteTarget {
        final MappingEntry entry;
        final Object id;
        final boolean deleteWithEdges;

        DeleteTarget(MappingEntry entry, Object id, boolean deleteWithEdges) {
            this.entry = entry;
            this.id = id;
            this.deleteWithEdges = deleteWithEdges;
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        // The SeaTunnel SinkWriter interface has no snapshotState() — the framework
        // provides no mechanism for a sink writer to persist in-flight state across a
        // checkpoint. A pending UPDATE_BEFORE means an update was split across the
        // checkpoint boundary: UPDATE_BEFORE arrived but its paired UPDATE_AFTER has
        // not yet been processed. On recovery the source replays from its own
        // checkpoint, which may or may not include both rows, so the mutation could be
        // partially applied (UPDATE_BEFORE replayed without its AFTER, or vice versa).
        //
        // Refuse to checkpoint in this state. Check BEFORE the flush try-catch so the
        // exception propagates directly rather than being wrapped as a flush failure.
        if (pendingUpdateBefore != null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Checkpoint requested between UPDATE_BEFORE and UPDATE_AFTER: "
                            + "UPDATE_BEFORE received but its paired UPDATE_AFTER has "
                            + "not yet arrived. The SeaTunnel SinkWriter interface does "
                            + "not support persisting in-flight mutation state across "
                            + "checkpoints. UPDATE_BEFORE and UPDATE_AFTER must arrive "
                            + "within the same checkpoint interval. "
                            + "Mitigations: (1) increase the checkpoint interval, "
                            + "(2) use INSERT-only mode if the source does not emit "
                            + "changelog events.");
        }

        try {
            buffer.flush();
        } catch (Exception e) {
            LOG.error("Failed to flush data during prepareCommit, failing checkpoint.", e);
            throw new RuntimeException("Failed to flush data during prepareCommit()", e);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        Exception failure = null;
        try {
            if (buffer != null) {
                buffer.close();
            }
        } catch (Exception e) {
            failure = e;
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (Exception closeFailure) {
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure != null) {
            throw new IOException("Failed to close HugeGraph sink writer", failure);
        }
    }

    /** Package-private test accessor. */
    List<MappingEntry> mappingEntries() {
        return mappingEntries;
    }

    static class MappingEntry {
        final MappingConfig config;
        final GraphDataMapper mapper;

        MappingEntry(MappingConfig config, GraphDataMapper mapper) {
            this.config = config;
            this.mapper = mapper;
        }
    }
}
