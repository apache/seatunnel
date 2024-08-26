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

package org.apache.seatunnel.connectors.seatunnel.qdrant.sink;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.qdrant.config.QdrantParameters;
import org.apache.seatunnel.connectors.seatunnel.qdrant.exception.QdrantConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.ValueFactory;
import io.qdrant.client.VectorFactory;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.qdrant.client.PointIdFactory.id;
import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;

public class QdrantBatchWriter {

    private final int batchSize;
    private final CatalogTable catalogTable;
    private final String collectionName;
    private final QdrantClient qdrantClient;

    private final List<Points.PointStruct> qdrantDataCache;
    private volatile int writeCount = 0;

    public QdrantBatchWriter(
            CatalogTable catalogTable, Integer batchSize, QdrantParameters params) {
        this.catalogTable = catalogTable;
        this.qdrantClient = params.buildQdrantClient();
        this.collectionName = params.getCollectionName();
        this.batchSize = batchSize;
        this.qdrantDataCache = new ArrayList<>(batchSize);
    }

    public void addToBatch(SeaTunnelRow element) {
        Points.PointStruct point = buildPoint(element);
        qdrantDataCache.add(point);
        writeCount++;
    }

    public boolean needFlush() {
        return this.writeCount >= this.batchSize;
    }

    public synchronized void flush() {
        if (CollectionUtils.isEmpty(this.qdrantDataCache)) {
            return;
        }
        upsert();
        this.qdrantDataCache.clear();
        this.writeCount = 0;
    }

    public void close() {
        this.qdrantClient.close();
    }

    private Points.PointStruct buildPoint(SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();

        Points.PointStruct.Builder point = Points.PointStruct.newBuilder();
        Points.NamedVectors.Builder namedVectors = Points.NamedVectors.newBuilder();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];
            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object value = element.getField(i);

            if (isPrimaryKeyField(primaryKey, fieldName)) {
                point.setId(pointId(fieldType, value));
                continue;
            }

            JsonWithInt.Value payloadValue = buildPayload(fieldType, value);
            if (payloadValue != null) {
                point.putPayload(fieldName, payloadValue);
                continue;
            }

            Points.Vector vector = buildVector(fieldType, value);
            if (vector != null) {
                namedVectors.putVectors(fieldName, vector);
            }
        }

        if (!point.hasId()) {
            point.setId(id(UUID.randomUUID()));
        }

        point.setVectors(Points.Vectors.newBuilder().setVectors(namedVectors).build());
        return point.build();
    }

    private void upsert() {
        try {
            qdrantClient
                    .upsertAsync(
                            Points.UpsertPoints.newBuilder()
                                    .setCollectionName(collectionName)
                                    .addAllPoints(qdrantDataCache)
                                    .build())
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Upsert failed", e);
        }
    }

    public static Points.PointId pointId(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case INT:
                return id(Integer.parseInt(value.toString()));
            case STRING:
                return id(UUID.fromString(value.toString()));
            default:
                throw new QdrantConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unexpected value type for point ID: " + sqlType.name());
        }
    }

    public static JsonWithInt.Value buildPayload(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case SMALLINT:
            case INT:
            case BIGINT:
                return ValueFactory.value(Integer.parseInt(value.toString()));
            case FLOAT:
            case DOUBLE:
                return ValueFactory.value(Long.parseLong(value.toString()));
            case STRING:
            case DATE:
                return ValueFactory.value(value.toString());
            case BOOLEAN:
                return ValueFactory.value(Boolean.parseBoolean(value.toString()));
            default:
                return null;
        }
    }

    public static Points.Vector buildVector(SeaTunnelDataType<?> fieldType, Object value) {
        SqlType sqlType = fieldType.getSqlType();
        switch (sqlType) {
            case FLOAT_VECTOR:
            case FLOAT16_VECTOR:
            case BFLOAT16_VECTOR:
            case BINARY_VECTOR:
                ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                Float[] floats = BufferUtils.toFloatArray(floatVectorBuffer);
                return VectorFactory.vector(Arrays.stream(floats).collect(Collectors.toList()));
            default:
                return null;
        }
    }
}
