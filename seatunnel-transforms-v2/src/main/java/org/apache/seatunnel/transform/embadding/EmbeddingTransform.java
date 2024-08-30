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

package org.apache.seatunnel.transform.embadding;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.embadding.processor.Model;
import org.apache.seatunnel.transform.embadding.processor.doubao.DoubaoModel;
import org.apache.seatunnel.transform.embadding.processor.openai.OpenAIModel;
import org.apache.seatunnel.transform.embadding.processor.qianfan.QianfanModel;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddingTransform extends MultipleFieldOutputTransform {

    private final ReadonlyConfig config;
    private List<String> fieldNames;
    private List<Integer> fieldOriginalIndexes;
    private Model model;
    private Integer dimension;

    public EmbeddingTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        initOutputFields(
                inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                config.get(EmbeddingTransformConfig.VECTORIZATION_FIELDS));
    }

    @Override
    public void open() {
        // Initialize model
        EmbeddingModelProvider provider =
                config.get(EmbeddingTransformConfig.EMBEDDING_MODEL_PROVIDER);
        try {
            switch (provider) {
                case OPENAI:
                    model =
                            new OpenAIModel(
                                    config.get(EmbeddingTransformConfig.API_KEY),
                                    config.get(EmbeddingTransformConfig.MODEL),
                                    config.get(EmbeddingTransformConfig.API_PATH),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case DOUBAO:
                    model =
                            new DoubaoModel(
                                    config.get(EmbeddingTransformConfig.API_KEY),
                                    config.get(EmbeddingTransformConfig.MODEL),
                                    config.get(EmbeddingTransformConfig.API_PATH),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case QIANFAN:
                    model =
                            new QianfanModel(
                                    config.get(EmbeddingTransformConfig.API_KEY),
                                    config.get(EmbeddingTransformConfig.SECRET_KEY),
                                    config.get(EmbeddingTransformConfig.MODEL),
                                    config.get(EmbeddingTransformConfig.API_PATH),
                                    config.get(EmbeddingTransformConfig.OAUTH_PATH),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported model provider: " + provider);
            }
            // Initialize dimension
            dimension = model.dimension();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize model", e);
        }
    }

    private void initOutputFields(SeaTunnelRowType inputRowType, Map<String, String> fields) {
        List<String> fieldNames = new ArrayList<>();
        List<Integer> fieldOriginalIndexes = new ArrayList<>();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String srcField = field.getValue();
            int srcFieldIndex;
            try {
                srcFieldIndex = inputRowType.indexOf(srcField);
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(getPluginName(), srcField);
            }
            fieldNames.add(field.getKey());
            fieldOriginalIndexes.add(srcFieldIndex);
        }
        this.fieldNames = fieldNames;
        this.fieldOriginalIndexes = fieldOriginalIndexes;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        try {
            Object[] fieldArray = new Object[fieldOriginalIndexes.size()];
            for (int i = 0; i < fieldOriginalIndexes.size(); i++) {
                fieldArray[i] = inputRow.getField(fieldOriginalIndexes.get(i));
            }
            List<ByteBuffer> vectorization = model.vectorization(fieldArray);
            return vectorization.toArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to data vectorization", e);
        }
    }

    @Override
    protected Column[] getOutputColumns() {
        Column[] columns = new Column[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            columns[i] =
                    PhysicalColumn.of(
                            fieldNames.get(i),
                            VectorType.VECTOR_FLOAT_TYPE,
                            null,
                            dimension,
                            true,
                            "",
                            "");
        }
        return columns;
    }

    @Override
    public String getPluginName() {
        return "Embedding";
    }

    @SneakyThrows
    @Override
    public void close() {
        if (model != null) {
            model.close();
        }
    }
}
