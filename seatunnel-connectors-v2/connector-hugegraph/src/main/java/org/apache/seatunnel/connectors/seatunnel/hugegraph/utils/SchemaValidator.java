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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/** Validates the SeaTunnel schema against the HugeGraph schema. */
public final class SchemaValidator {

    private final HugeGraphSinkConfig sinkConfig;
    private final SeaTunnelRowType rowType;
    private final HugeGraphClient client;

    public SchemaValidator(HugeGraphSinkConfig config, SeaTunnelRowType rowType) {
        this.sinkConfig = config;
        this.rowType = rowType;
        this.client = new HugeGraphClient(sinkConfig);
    }

    public void validateSchema() {
        try {
            SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();
            if (schemaConfig.getType() == LabelType.VERTEX) {
                validateVertex(schemaConfig);
            } else if (schemaConfig.getType() == LabelType.EDGE) {
                validateEdge(schemaConfig);
            } else {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        "Unsupported schema type: " + schemaConfig.getType());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            client.close();
        }
    }

    private void validateVertex(SchemaConfig schemaConfig) {
        String label = schemaConfig.getLabel();
        VertexLabel vertexLabel = this.client.getVertexLabel(label);
        if (vertexLabel == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format("Vertex label '%s' does not exist in HugeGraph.", label));
        }
        validateLabelProperties(label, schemaConfig, vertexLabel.properties());
    }

    private void validateEdge(SchemaConfig schemaConfig) {
        String label = schemaConfig.getLabel();
        EdgeLabel edgeLabel = this.client.getEdgeLabel(label);
        if (edgeLabel == null) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format("Edge label '%s' does not exist in HugeGraph.", label));
        }
        validateSourceTarget(schemaConfig, edgeLabel);
        validateLabelProperties(label, schemaConfig, edgeLabel.properties());
    }

    private void validateSourceTarget(SchemaConfig schemaConfig, EdgeLabel edgeLabel) {
        String label = schemaConfig.getLabel();
        String schemaSource = edgeLabel.sourceLabel();
        if (!schemaSource.equals(schemaConfig.getSourceConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "EdgeLabel[%s] sourceLabel mismatch: schema=%s, config=%s",
                            label, schemaSource, schemaConfig.getSourceConfig()));
        }

        String schemaTarget = edgeLabel.targetLabel();
        if (!schemaTarget.equals(schemaConfig.getTargetConfig().getLabel())) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "EdgeLabel[%s] sourceLabel mismatch: schema=%s, config=%s",
                            label, schemaSource, schemaConfig.getSourceConfig()));
        }
    }

    /**
     * Validates if the properties from SeaTunnelRowType are compatible with the HugeGraph schema.
     */
    private void validateLabelProperties(
            String label, SchemaConfig schemaConfig, Set<String> hugegraphProperties) {

        MappingConfig mappingConfig = schemaConfig.getMapping();
        Map<String, String> fieldMapping =
                mappingConfig == null || mappingConfig.getFieldMapping() == null
                        ? Collections.emptyMap()
                        : mappingConfig.getFieldMapping();

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String fieldName = rowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelType = rowType.getFieldType(i);
            String propertyName = fieldMapping.getOrDefault(fieldName, fieldName);

            // 1. Check if the property exists in HugeGraph
            if (!hugegraphProperties.contains(propertyName)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Property '%s' for label '%s' is defined in the connector config, but does not exist in the HugeGraph schema.",
                                propertyName, label));
            }

            // 2. Check for data type compatibility
            PropertyKey propertyKey = this.client.getPropertyKey(propertyName);
            DataType hugeGraphType = propertyKey.dataType();
            Cardinality cardinality = propertyKey.cardinality();

            if (!isCompatible(seaTunnelType, hugeGraphType, cardinality)) {
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Data type mismatch for property '%s' on label '%s'. "
                                        + "SeaTunnel type '%s' is not compatible with HugeGraph type '%s'.",
                                propertyName, label, seaTunnelType, hugeGraphType));
            }
        }
    }

    /** Checks if a SeaTunnelDataType is compatible with a HugeGraph DataType. */
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
                // Unsupported types are considered incompatible.
                return false;
        }
    }
}
