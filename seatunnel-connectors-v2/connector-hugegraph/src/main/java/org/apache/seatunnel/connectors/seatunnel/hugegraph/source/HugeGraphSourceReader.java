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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSourceOptions.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HugeGraphSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSourceReader.class);

    private final SingleSplitReaderContext context;
    private final HugeGraphSourceConfig sourceConfig;
    private final SeaTunnelRowType rowType;
    private HugeGraphClient client;
    private int totalRead;

    public HugeGraphSourceReader(
            SingleSplitReaderContext context,
            HugeGraphSourceConfig sourceConfig,
            CatalogTable catalogTable) {
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.totalRead = 0;
    }

    // For testing: allows injecting a mock client
    HugeGraphSourceReader(
            SingleSplitReaderContext context,
            HugeGraphSourceConfig sourceConfig,
            CatalogTable catalogTable,
            HugeGraphClient client) {
        this(context, sourceConfig, catalogTable);
        this.client = client;
    }

    @Override
    public void open() throws Exception {
        if (this.client == null) {
            this.client =
                    new HugeGraphClient(
                            sourceConfig.getHost(),
                            sourceConfig.getPort(),
                            sourceConfig.getGraphName(),
                            sourceConfig.getGraphSpace(),
                            sourceConfig.getUsername(),
                            sourceConfig.getPassword(),
                            sourceConfig.getMaxRetries(),
                            sourceConfig.getRetryBackoffMs());
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            String label = sourceConfig.getLabel();
            int pageSize = sourceConfig.getPageSize();
            Integer limit = sourceConfig.getLimit();
            Set<String> selectedProperties = getSelectedProperties();

            if (sourceConfig.getType() == LabelType.VERTEX) {
                readVertices(output, label, pageSize, limit, selectedProperties);
            } else {
                readEdges(output, label, pageSize, limit, selectedProperties);
            }

            context.signalNoMoreElement();
        } catch (HugeGraphConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.READ_FAILED,
                    "Failed to read data from HugeGraph",
                    e);
        }
    }

    private Set<String> getSelectedProperties() {
        if (sourceConfig.getProperties() != null && !sourceConfig.getProperties().isEmpty()) {
            return sourceConfig.getProperties().stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        }
        return null;
    }

    private void readVertices(
            Collector<SeaTunnelRow> output,
            String label,
            int pageSize,
            Integer limit,
            Set<String> selectedProperties) {
        Iterator<Vertex> iterator = client.iterateVertices(label, pageSize);
        while (iterator.hasNext()) {
            if (limit != null && totalRead >= limit) {
                break;
            }
            Vertex vertex = iterator.next();
            SeaTunnelRow row = mapVertex(vertex, selectedProperties);
            if (row != null) {
                output.collect(row);
                totalRead++;
            }
        }
    }

    private void readEdges(
            Collector<SeaTunnelRow> output,
            String label,
            int pageSize,
            Integer limit,
            Set<String> selectedProperties) {
        Iterator<Edge> iterator = client.iterateEdges(label, pageSize);
        while (iterator.hasNext()) {
            if (limit != null && totalRead >= limit) {
                break;
            }
            Edge edge = iterator.next();
            SeaTunnelRow row = mapEdge(edge, selectedProperties);
            if (row != null) {
                output.collect(row);
                totalRead++;
            }
        }
    }

    private SeaTunnelRow mapVertex(Vertex vertex, Set<String> selectedProperties) {
        Map<String, Object> properties = vertex.properties();
        Object[] fields = new Object[rowType.getTotalFields()];

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            switch (fieldName) {
                case "id":
                    fields[i] = String.valueOf(vertex.id());
                    break;
                case "label":
                    fields[i] = vertex.label();
                    break;
                default:
                    if (selectedProperties != null
                            && !selectedProperties.contains(fieldName.toLowerCase())) {
                        fields[i] = null;
                    } else {
                        fields[i] =
                                convertPropertyValue(
                                        properties.get(fieldName), rowType.getFieldType(i));
                    }
                    break;
            }
        }

        return new SeaTunnelRow(fields);
    }

    private SeaTunnelRow mapEdge(Edge edge, Set<String> selectedProperties) {
        Map<String, Object> properties = edge.properties();
        Object[] fields = new Object[rowType.getTotalFields()];

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            switch (fieldName) {
                case "id":
                    fields[i] = edge.id();
                    break;
                case "label":
                    fields[i] = edge.label();
                    break;
                case "source_id":
                    fields[i] = String.valueOf(edge.sourceId());
                    break;
                case "target_id":
                    fields[i] = String.valueOf(edge.targetId());
                    break;
                default:
                    if (selectedProperties != null
                            && !selectedProperties.contains(fieldName.toLowerCase())) {
                        fields[i] = null;
                    } else {
                        fields[i] =
                                convertPropertyValue(
                                        properties.get(fieldName), rowType.getFieldType(i));
                    }
                    break;
            }
        }

        return new SeaTunnelRow(fields);
    }

    private static Object convertPropertyValue(Object value, SeaTunnelDataType<?> expectedType) {
        if (value == null) {
            return null;
        }
        if (expectedType instanceof ArrayType && value instanceof Collection) {
            return ((Collection<?>) value).toArray();
        }
        if (expectedType.equals(LocalTimeType.LOCAL_DATE_TYPE) && value instanceof Date) {
            return ((Date) value).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        }
        return value;
    }
}
