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

package org.apache.seatunnel.connectors.seatunnel.weaviate.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.connectors.seatunnel.weaviate.config.WeaviateParameters;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectionErrorCode;
import org.apache.seatunnel.connectors.seatunnel.weaviate.exception.WeaviateConnectorException;
import org.apache.seatunnel.connectors.seatunnel.weaviate.state.WeaviateCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.weaviate.state.WeaviateSinkState;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.data.model.WeaviateObject;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.table.catalog.PrimaryKey.isPrimaryKeyField;
import static org.apache.seatunnel.api.table.type.SqlType.BFLOAT16_VECTOR;
import static org.apache.seatunnel.api.table.type.SqlType.BINARY_VECTOR;
import static org.apache.seatunnel.api.table.type.SqlType.FLOAT16_VECTOR;
import static org.apache.seatunnel.api.table.type.SqlType.FLOAT_VECTOR;
import static org.apache.seatunnel.api.table.type.SqlType.SPARSE_FLOAT_VECTOR;
import static org.apache.seatunnel.connectors.seatunnel.weaviate.convert.WeaviateConverter.convertBySeaTunnelType;

@Slf4j
public class WeaviateSinkWriter
        implements SinkWriter<SeaTunnelRow, WeaviateCommitInfo, WeaviateSinkState> {

    private final Context context;
    private final CatalogTable catalogTable;

    private final WeaviateParameters parameters;
    private ObjectsBatcher batchWriter;

    public WeaviateSinkWriter(
            Context context,
            CatalogTable catalogTable,
            WeaviateParameters parameters,
            List<WeaviateSinkState> sinkStates) {
        this.context = context;
        this.parameters = parameters;
        this.catalogTable = catalogTable;
        Config config =
                new Config(
                        catalogTable.getTablePath().getTableName(),
                        parameters.getUrl(),
                        parameters.getHeader(),
                        parameters.getConnectionTimeout(),
                        parameters.getReadTimeout(),
                        parameters.getWriteTimeout());
        // Set batch config, now default
        ObjectsBatcher.BatchRetriesConfig batchRetriesConfig =
                ObjectsBatcher.BatchRetriesConfig.defaultConfig().build();
        ObjectsBatcher.AutoBatchConfig autoBatchConfig =
                ObjectsBatcher.AutoBatchConfig.defaultConfig().build();
        this.batchWriter =
                new WeaviateClient(config)
                        .batch()
                        .objectsAutoBatcher(batchRetriesConfig, autoBatchConfig);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        WeaviateObject weaviateObject = buildWeaviateData(element);
        batchWriter.withObjects(weaviateObject);
    }

    @Override
    public Optional<WeaviateCommitInfo> prepareCommit() throws IOException {
        batchWriter.flush();
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (batchWriter != null) {
            batchWriter.flush();
        }
    }

    private WeaviateObject buildWeaviateData(SeaTunnelRow element) {
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        PrimaryKey primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        Map<String, Float[]> vectors = new HashMap<>();

        WeaviateObject.WeaviateObjectBuilder builder = WeaviateObject.builder();
        Map<String, Object> properties = new HashMap<>();
        for (int i = 0; i < seaTunnelRowType.getFieldNames().length; i++) {
            String fieldName = seaTunnelRowType.getFieldNames()[i];

            if (parameters.getEnableAutoId() && isPrimaryKeyField(primaryKey, fieldName)) {
                continue;
            }

            SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(i);
            Object value = element.getField(i);
            if (null == value) {
                throw new WeaviateConnectorException(
                        WeaviateConnectionErrorCode.WEAVIATE_PROPERTY_NOT_FOUND, fieldName);
            }

            if (fieldType.getSqlType() == FLOAT_VECTOR) {
                if (!parameters.getEnableVector()) {
                    ByteBuffer floatVectorBuffer = (ByteBuffer) value;
                    vectors.put(fieldName, BufferUtils.toFloatArray(floatVectorBuffer));
                }
            } else if (fieldType.getSqlType() == BINARY_VECTOR
                    || fieldType.getSqlType() == BFLOAT16_VECTOR
                    || fieldType.getSqlType() == FLOAT16_VECTOR
                    || fieldType.getSqlType() == SPARSE_FLOAT_VECTOR) {
                log.warn("{} {} of vector is not supported!", fieldName, fieldType.getSqlType());
            }

            properties.put(fieldName, convertBySeaTunnelType(fieldType, value));
        }
        WeaviateObject build =
                builder.className(catalogTable.getTablePath().getTableName())
                        .properties(properties)
                        .build();
        if (parameters.getEnableVector()) {
            build.setVectors(vectors);
        }
        return build;
    }
}
