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

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer.BatchBuffer;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.EdgeMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.GraphDataMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.VertexMapper;

import org.apache.hugegraph.structure.GraphElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
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
    private final GraphDataMapper mapper;
    private final HugeGraphClient client;
    private final BatchBuffer buffer;

    public HugeGraphSinkWriter(HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType) {
        this.sinkConfig = sinkConfig;
        this.client = new HugeGraphClient(sinkConfig);
        this.mapper = getMapper(rowType);
        this.buffer =
                new BatchBuffer(
                        this.client, sinkConfig.getBatchSize(), sinkConfig.getBatchIntervalMs());
    }

    private GraphDataMapper getMapper(SeaTunnelRowType rowType) {
        SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();
        List<String> selectedFields = sinkConfig.getSelectedFields();
        List<String> ignoredFields = sinkConfig.getIgnoredFields();
        Map<String, Integer> originalFieldsIndex =
                IntStream.range(0, rowType.getTotalFields())
                        .boxed()
                        .collect(Collectors.toMap(rowType::getFieldName, i -> i));

        Map<String, Integer> finalFieldsIndex = new LinkedHashMap<>();

        if (selectedFields != null && !selectedFields.isEmpty()) {
            for (String field : selectedFields) {
                Integer originalIndex = originalFieldsIndex.get(field);
                if (originalIndex != null) {
                    finalFieldsIndex.put(field, originalIndex);
                }
            }
        } else if (ignoredFields != null && !ignoredFields.isEmpty()) {
            Set<String> ignoreSet = new HashSet<>(ignoredFields);
            for (Map.Entry<String, Integer> entry : originalFieldsIndex.entrySet()) {
                String fieldName = entry.getKey();
                Integer originalIndex = entry.getValue();

                if (!ignoreSet.contains(fieldName)) {
                    finalFieldsIndex.put(fieldName, originalIndex);
                }
            }
        } else {
            finalFieldsIndex = originalFieldsIndex;
        }

        if (schemaConfig.getType() == LabelType.VERTEX) {
            return new VertexMapper(schemaConfig, finalFieldsIndex, client);
        } else {
            return new EdgeMapper(schemaConfig, finalFieldsIndex, client);
        }
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                handleUpsert(row);
                break;
            case DELETE:
                handleDelete(row);
                break;
            case UPDATE_BEFORE:
                // The huge-client natively supports upsert operations for property updates, so
                // there is no need to handle this data manually.
                break;
            default:
                LOG.warn("Unsupported row kind: {}", row.getRowKind());
                break;
        }
    }

    private void handleUpsert(SeaTunnelRow row) throws IOException {
        try {
            GraphElement element = mapper.map(row);
            if (element == null) {
                LOG.warn("Cannot create graph element: required ID fields missing for row {}", row);
                return;
            }
            buffer.add(element);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    private void handleDelete(SeaTunnelRow row) {
        try {
            buffer.flush();
            if (sinkConfig.getSchemaConfig().getType() == LabelType.VERTEX) {
                Object vertexId = mapper.extractId(row);
                if (vertexId == null) {
                    LOG.warn("Cannot delete vertex: ID extraction failed for row {}", row);
                    return;
                }
                client.deleteVertexWithEdges(vertexId);
            } else {
                String edgeId = (String) mapper.extractId(row);
                if (edgeId == null) {
                    LOG.warn("Cannot delete edge: ID extraction failed for row {}", row);
                    return;
                }
                client.deleteEdge(edgeId);
            }
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.GRAPH_OPERATION_FAILED,
                    "Non-retryable error executing graph operation",
                    e);
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
        if (buffer != null) {
            buffer.close();
        }

        if (client != null) {
            client.close();
        }
    }
}
