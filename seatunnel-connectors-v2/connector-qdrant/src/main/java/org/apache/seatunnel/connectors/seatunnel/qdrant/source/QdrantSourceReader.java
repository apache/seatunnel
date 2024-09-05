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

package org.apache.seatunnel.connectors.seatunnel.qdrant.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.WithVectorsSelectorFactory;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.qdrant.client.WithPayloadSelectorFactory.enable;
import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;

public class QdrantSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final QdrantParameters qdrantParameters;
    private final SingleSplitReaderContext context;
    private final TableSchema tableSchema;
    private final TablePath tablePath;
    private QdrantClient qdrantClient;

    public QdrantSourceReader(
            QdrantParameters qdrantParameters,
            SingleSplitReaderContext context,
            CatalogTable catalogTable) {
        this.qdrantParameters = qdrantParameters;
        this.context = context;
        this.tableSchema = catalogTable.getTableSchema();
        this.tablePath = catalogTable.getTablePath();
    }

    @Override
    public void open() throws Exception {
        qdrantClient = qdrantParameters.buildQdrantClient();
        qdrantClient.healthCheckAsync().get();
    }

    @Override
    public void close() {
        if (Objects.nonNull(qdrantClient)) {
            qdrantClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        int SCROLL_SIZE = 64;
        Points.ScrollPoints request =
                Points.ScrollPoints.newBuilder()
                        .setCollectionName(qdrantParameters.getCollectionName())
                        .setLimit(SCROLL_SIZE)
                        .setWithPayload(enable(true))
                        .setWithVectors(WithVectorsSelectorFactory.enable(true))
                        .build();

        while (true) {
            Points.ScrollResponse response = qdrantClient.scrollAsync(request).get();
            List<Points.RetrievedPoint> points = response.getResultList();

            for (Points.RetrievedPoint point : points) {
                SeaTunnelRow seaTunnelRow = convertToSeaTunnelRow(point);
                output.collect(seaTunnelRow);
            }

            Points.PointId offset = response.getNextPageOffset();

            if (!offset.hasNum() && !offset.hasUuid()) break;

            request = request.toBuilder().setOffset(offset).build();
        }

        context.signalNoMoreElement();
    }

    private SeaTunnelRow convertToSeaTunnelRow(Points.RetrievedPoint point) {
        SeaTunnelRowType typeInfo = tableSchema.toPhysicalRowDataType();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        Map<String, JsonWithInt.Value> payloadMap = point.getPayloadMap();
        Points.Vectors vectors = point.getVectors();
        Map<String, Points.Vector> vectorsMap = new HashMap<>();
        String DEFAULT_VECTOR_KEY = "default_vector";

        if (vectors.hasVector()) {
            vectorsMap.put(DEFAULT_VECTOR_KEY, vectors.getVector());
        } else if (vectors.hasVectors()) {
            vectorsMap = vectors.getVectors().getVectorsMap();
        }
        Object[] fields = new Object[typeInfo.getTotalFields()];
        String[] fieldNames = typeInfo.getFieldNames();
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            String fieldName = fieldNames[fieldIndex];

            if (isPrimaryKeyField(primaryKey, fieldName)) {
                Points.PointId id = point.getId();
                if (id.hasNum()) {
                    fields[fieldIndex] = id.getNum();
                } else if (id.hasUuid()) {
                    fields[fieldIndex] = id.getUuid();
                }
                continue;
            }
            JsonWithInt.Value value = payloadMap.get(fieldName);
            Points.Vector vector = vectorsMap.get(fieldName);
            switch (seaTunnelDataType.getSqlType()) {
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case STRING:
                    fields[fieldIndex] = value.getStringValue();
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = value.getBoolValue();
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    fields[fieldIndex] = value.getIntegerValue();
                    break;
                case FLOAT:
                case DECIMAL:
                case DOUBLE:
                    fields[fieldIndex] = value.getDoubleValue();
                    break;
                case BINARY_VECTOR:
                case FLOAT_VECTOR:
                case FLOAT16_VECTOR:
                case BFLOAT16_VECTOR:
                    List<Float> list = vector.getDataList();
                    Float[] vectorArray = new Float[list.size()];
                    list.toArray(vectorArray);
                    fields[fieldIndex] = BufferUtils.toByteBuffer(vectorArray);
                    break;
                default:
                    throw new QdrantConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType.getSqlType().name());
            }
        }

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
        seaTunnelRow.setTableId(tablePath.getFullName());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        return seaTunnelRow;
    }
}
