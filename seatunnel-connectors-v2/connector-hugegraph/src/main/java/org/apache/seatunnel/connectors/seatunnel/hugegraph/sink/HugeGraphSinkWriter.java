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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HugeGraphSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSinkWriter.class);

    private final HugeGraphSinkConfig sinkConfig;
    private final List<MappingEntry> mappingEntries;
    private final HugeGraphClient client;
    private final BatchBuffer buffer;

    public HugeGraphSinkWriter(HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType) {
        this(sinkConfig, rowType, new HugeGraphClient(sinkConfig.getConnectionConfig()));
    }

    HugeGraphSinkWriter(
            HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType, HugeGraphClient client) {
        this.sinkConfig = sinkConfig;
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
                        this.client, sinkConfig.getBatchSize(), sinkConfig.getBatchIntervalMs());
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
            Map<String, Integer> fieldsIndex = resolveFieldsIndex(mapping, availableFieldsIndex);
            GraphDataMapper mapper;
            if (mapping.getType() == LabelType.VERTEX) {
                mapper = new VertexMapper(mapping, fieldsIndex, client);
            } else {
                mapper = new EdgeMapper(mapping, fieldsIndex, client);
            }
            entries.add(new MappingEntry(mapping, mapper));
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
                handleUpsert(row, false);
                break;
            case UPDATE_AFTER:
                handleUpsert(row, true);
                break;
            case DELETE:
                handleDelete(row);
                break;
            case UPDATE_BEFORE:
                break;
            default:
                LOG.warn("Unsupported row kind: {}", row.getRowKind());
                break;
        }
    }

    private void handleUpsert(SeaTunnelRow row, boolean update) throws IOException {
        List<GraphElementEnvelope> vertexEnvelopes = new ArrayList<>();
        List<GraphElementEnvelope> edgeEnvelopes = new ArrayList<>();

        for (MappingEntry entry : mappingEntries) {
            try {
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
                GraphElement element = entry.mapper.map(row);
                if (element == null) {
                    continue;
                }
                GraphElementEnvelope envelope =
                        new GraphElementEnvelope(
                                entry.config.getLabel(), entry.config.getType(), row, element);
                if (entry.config.getType() == LabelType.VERTEX) {
                    vertexEnvelopes.add(envelope);
                } else {
                    edgeEnvelopes.add(envelope);
                }
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        String.format(
                                "Mapping[%s/%s]: Failed to map input row to graph element",
                                entry.config.getType(), entry.config.getLabel()),
                        e);
            }
        }

        for (GraphElementEnvelope envelope : vertexEnvelopes) {
            buffer.add(envelope);
        }
        for (GraphElementEnvelope envelope : edgeEnvelopes) {
            buffer.add(envelope);
        }
    }

    private void handleDelete(SeaTunnelRow row) {
        try {
            buffer.flush();
        } catch (IOException e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Failed to flush buffer before DELETE operation",
                    e);
        }

        List<MappingEntry> edgeEntries = new ArrayList<>();
        List<MappingEntry> vertexEntries = new ArrayList<>();
        for (MappingEntry entry : mappingEntries) {
            if (entry.config.getType() == LabelType.VERTEX) {
                vertexEntries.add(entry);
            } else {
                edgeEntries.add(entry);
            }
        }

        for (MappingEntry entry : edgeEntries) {
            try {
                Object id = entry.mapper.extractId(row);
                if (id == null) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format(
                                    "Mapping[%s/%s]: Cannot delete because a required ID field is null or matches nullValues",
                                    entry.config.getType(), entry.config.getLabel()));
                }
                client.deleteEdge((String) id);
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        String.format(
                                "Mapping[%s/%s]: Failed to delete graph element",
                                entry.config.getType(), entry.config.getLabel()),
                        e);
            }
        }

        for (MappingEntry entry : vertexEntries) {
            try {
                Object id = entry.mapper.extractId(row);
                if (id == null) {
                    throw new HugeGraphConnectorException(
                            HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                            String.format(
                                    "Mapping[%s/%s]: Cannot delete because a required ID field is null or matches nullValues",
                                    entry.config.getType(), entry.config.getLabel()));
                }
                if (sinkConfig.isDeleteVertexWithEdges()) {
                    client.deleteVertexWithEdges(id);
                } else {
                    client.deleteVertex(id);
                }
            } catch (Exception e) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                        String.format(
                                "Mapping[%s/%s]: Failed to delete graph element",
                                entry.config.getType(), entry.config.getLabel()),
                        e);
            }
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            buffer.flush();
        } catch (IOException e) {
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

    private static class MappingEntry {
        final MappingConfig config;
        final GraphDataMapper mapper;

        MappingEntry(MappingConfig config, GraphDataMapper mapper) {
            this.config = config;
            this.mapper = mapper;
        }
    }
}
