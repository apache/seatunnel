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

package org.apache.seatunnel.connectors.seatunnel.milvus.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorException;

import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.embedding.EmbeddingResult;
import com.theokanning.openai.service.OpenAiService;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.DescCollResponseWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_DATA_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.milvus.exception.MilvusConnectorErrorCode.RESPONSE_FAILED;

public class MilvusSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final MilvusServiceClient milvusClient;

    private final MilvusSinkConfig milvusSinkConfig;

    private OpenAiService service;

    private final List<FieldType> metaFields;

    public MilvusSinkWriter(MilvusSinkConfig milvusSinkConfig) {
        this.milvusSinkConfig = milvusSinkConfig;
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withHost(milvusSinkConfig.getMilvusHost())
                        .withPort(milvusSinkConfig.getMilvusPort())
                        .withAuthorization(
                                milvusSinkConfig.getUserName(), milvusSinkConfig.getPassword())
                        .build();
        milvusClient = new MilvusServiceClient(connectParam);

        handleResponseStatus(
                milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withCollectionName(milvusSinkConfig.getCollectionName())
                                .build()));

        R<DescribeCollectionResponse> describeCollectionResponseR =
                milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withCollectionName(milvusSinkConfig.getCollectionName())
                                .build());

        handleResponseStatus(describeCollectionResponseR);

        DescCollResponseWrapper wrapper =
                new DescCollResponseWrapper(describeCollectionResponseR.getData());

        this.metaFields = wrapper.getFields();

        if (milvusSinkConfig.getEmbeddingsFields() != null) {
            service = new OpenAiService(milvusSinkConfig.getOpenaiApiKey());
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        List<InsertParam.Field> fields = new ArrayList<>();

        InsertParam.Builder builder = InsertParam.newBuilder();

        builder = builder.withCollectionName(milvusSinkConfig.getCollectionName());

        for (int i = 0; i < this.metaFields.size(); i++) {

            FieldType fieldType = this.metaFields.get(i);

            Object field = element.getField(i);
            if (fieldType.isPrimaryKey()) {
                if (!(field instanceof Long)
                        && !(field instanceof Integer)
                        && !(field instanceof Byte)
                        && !(field instanceof Short)
                        && !(field instanceof String)) {
                    throw new MilvusConnectorException(
                            ILLEGAL_ARGUMENT, "Primary key field only supports number and string.");
                }
            }

            if (milvusSinkConfig.getPartitionField() != null
                    && milvusSinkConfig.getPartitionField().equals(fieldType.getName())) {
                builder.withPartitionName(String.valueOf(field));
            }
            if (milvusSinkConfig.getEmbeddingsFields() != null) {
                List<String> embeddingsFields =
                        Arrays.asList(milvusSinkConfig.getEmbeddingsFields().split(","));
                if (embeddingsFields.contains(fieldType.getName())) {
                    if (fieldType.getDataType() != DataType.BinaryVector
                            && fieldType.getDataType() != DataType.FloatVector) {
                        throw new MilvusConnectorException(
                                ILLEGAL_ARGUMENT, "Vector field only supports binary and float.");
                    }
                    EmbeddingResult embeddings =
                            service.createEmbeddings(
                                    EmbeddingRequest.builder()
                                            .model(milvusSinkConfig.getOpenaiEngine())
                                            .input(Collections.singletonList(String.valueOf(field)))
                                            .build());
                    List<Double> embedding = embeddings.getData().get(0).getEmbedding();
                    List<Float> collect =
                            embedding.stream().map(Double::floatValue).collect(Collectors.toList());
                    InsertParam.Field insertField =
                            new InsertParam.Field(
                                    fieldType.getName(), Collections.singletonList(collect));
                    fields.add(insertField);
                    continue;
                }
            }

            judgmentParameterType(fieldType, field);

            InsertParam.Field insertField =
                    new InsertParam.Field(fieldType.getName(), Collections.singletonList(field));
            fields.add(insertField);
        }

        InsertParam build = builder.withFields(fields).build();

        handleResponseStatus(milvusClient.insert(build));
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        milvusClient.flush(
                FlushParam.newBuilder()
                        .addCollectionName(milvusSinkConfig.getCollectionName())
                        .build());
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
        milvusClient.close();
        service.shutdownExecutor();
    }

    private void handleResponseStatus(R<?> r) {
        if (r.getStatus() != R.Status.Success.getCode()) {
            throw new MilvusConnectorException(RESPONSE_FAILED, r.getMessage(), r.getException());
        }
    }

    private void judgmentParameterType(FieldType fieldType, Object value) {
        switch (fieldType.getDataType()) {
            case Bool:
                if (!(value instanceof Boolean)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            case Int8:
            case Int16:
            case Int32:
                if (!(value instanceof Integer)
                        && !(value instanceof Byte)
                        && !(value instanceof Short)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            case Int64:
                if (!(value instanceof Long)
                        && !(value instanceof Integer)
                        && !(value instanceof Byte)
                        && !(value instanceof Short)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            case Float:
                if (!(value instanceof Float)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            case Double:
                if (!(value instanceof Float) && !(value instanceof Double)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            case VarChar:
                if (!(value instanceof String)) {
                    throw new MilvusConnectorException(
                            UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
                }
                break;
            default:
                throw new MilvusConnectorException(
                        UNSUPPORTED_DATA_TYPE, UNSUPPORTED_DATA_TYPE.getDescription());
        }
    }
}
