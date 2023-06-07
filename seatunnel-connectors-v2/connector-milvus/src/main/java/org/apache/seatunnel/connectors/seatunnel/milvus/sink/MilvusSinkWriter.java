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
import org.apache.seatunnel.connectors.seatunnel.milvus.config.MilvusOptions;

import com.theokanning.openai.embedding.EmbeddingRequest;
import com.theokanning.openai.embedding.EmbeddingResult;
import com.theokanning.openai.service.OpenAiService;
import io.milvus.client.MilvusServiceClient;
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

public class MilvusSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final MilvusServiceClient milvusClient;

    private final MilvusOptions milvusOptions;

    private OpenAiService service;

    private final List<FieldType> fields;

    public MilvusSinkWriter(MilvusOptions milvusOptions) {
        this.milvusOptions = milvusOptions;
        ConnectParam connectParam =
                ConnectParam.newBuilder()
                        .withHost(milvusOptions.getMilvusHost())
                        .withPort(milvusOptions.getMilvusPort())
                        .withAuthorization(milvusOptions.getUserName(), milvusOptions.getPassword())
                        .build();
        milvusClient = new MilvusServiceClient(connectParam);

        handleResponseStatus(
                milvusClient.hasCollection(
                        HasCollectionParam.newBuilder()
                                .withCollectionName(milvusOptions.getCollectionName())
                                .build()));

        R<DescribeCollectionResponse> describeCollectionResponseR =
                milvusClient.describeCollection(
                        DescribeCollectionParam.newBuilder()
                                .withCollectionName(milvusOptions.getCollectionName())
                                .build());

        handleResponseStatus(describeCollectionResponseR);

        DescCollResponseWrapper wrapper =
                new DescCollResponseWrapper(describeCollectionResponseR.getData());

        fields = wrapper.getFields();

        if (milvusOptions.getEmbeddingsFields() != null) {
            service = new OpenAiService(milvusOptions.getOpenaiApiKey());
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {

        List<InsertParam.Field> fields = new ArrayList<>();

        InsertParam.Builder builder = InsertParam.newBuilder();

        builder = builder.withCollectionName(milvusOptions.getCollectionName());

        for (int i = 0; i < this.fields.size(); i++) {
            if (milvusOptions.getPartitionField() != null
                    && milvusOptions.getPartitionField().equals(this.fields.get(i).getName())) {
                builder.withPartitionName(String.valueOf(element.getField(i)));
            }
            if (milvusOptions.getEmbeddingsFields() != null) {
                List<String> embeddingsFields =
                        Arrays.asList(milvusOptions.getEmbeddingsFields().split(","));
                if (embeddingsFields.contains(this.fields.get(i).getName())) {
                    EmbeddingResult embeddings =
                            service.createEmbeddings(
                                    EmbeddingRequest.builder()
                                            .model(milvusOptions.getOpenaiEngine())
                                            .input(
                                                    Collections.singletonList(
                                                            String.valueOf(element.getField(i))))
                                            .build());
                    List<Double> embedding = embeddings.getData().get(0).getEmbedding();
                    List<Float> collect =
                            embedding.stream().map(Double::floatValue).collect(Collectors.toList());
                    InsertParam.Field field =
                            new InsertParam.Field(
                                    this.fields.get(i).getName(),
                                    Collections.singletonList(collect));
                    fields.add(field);
                    continue;
                }
            }
            InsertParam.Field field =
                    new InsertParam.Field(
                            this.fields.get(i).getName(),
                            Collections.singletonList(element.getField(i)));
            fields.add(field);
        }

        if (milvusOptions.getPartitionField() != null) {
            builder.withPartitionName(milvusOptions.getPartitionField());
        }

        InsertParam build = builder.withFields(fields).build();

        handleResponseStatus(milvusClient.insert(build));
    }

    @Override
    public List<Void> snapshotState(long checkpointId) throws IOException {
        milvusClient.flush(
                FlushParam.newBuilder()
                        .addCollectionName(milvusOptions.getCollectionName())
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
            throw new RuntimeException(r.getMessage());
        }
    }
}
