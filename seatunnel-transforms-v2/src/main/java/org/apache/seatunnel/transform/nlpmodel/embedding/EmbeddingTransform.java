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

package org.apache.seatunnel.transform.nlpmodel.embedding;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;
import org.apache.seatunnel.transform.nlpmodel.ModelTransformConfig;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalFieldValue;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.Model;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.amazon.BedrockModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.openai.OpenAIModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.qianfan.QianfanModel;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.zhipu.ZhipuModel;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransformConfig;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class EmbeddingTransform extends MultipleFieldOutputTransform {

    private final ReadonlyConfig config;
    private List<Integer> fieldOriginalIndexes;
    private transient Model model;
    private Integer dimension;
    private boolean isMultimodalFields = false;
    private Map<Integer, FieldSpec> fieldSpecMap;
    private List<String> fieldNames;

    private final Map<String, TreeMap<Long, byte[]>> binaryFileCache = new ConcurrentHashMap<>();
    private final Map<String, Long> partIndexMap = new ConcurrentHashMap<>();

    public EmbeddingTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        initOutputFields(inputCatalogTable.getTableSchema().toPhysicalRowDataType(), config);
    }

    private void tryOpen() {
        if (model == null) {
            open();
        }
    }

    @Override
    public void open() {
        ModelProvider provider = config.get(ModelTransformConfig.MODEL_PROVIDER);
        String apiPath =
                provider.usedEmbeddingPath(
                        config.get(ModelTransformConfig.API_PATH), isMultimodalFields);
        try {
            switch (provider) {
                case CUSTOM:
                    // load custom_config from the configuration
                    ReadonlyConfig customConfig =
                            config.getOptional(
                                            ModelTransformConfig.CustomRequestConfig.CUSTOM_CONFIG)
                                    .map(ReadonlyConfig::fromMap)
                                    .orElseThrow(
                                            () ->
                                                    new IllegalArgumentException(
                                                            "Custom config can't be null"));
                    model =
                            new CustomModel(
                                    config.get(ModelTransformConfig.MODEL),
                                    apiPath,
                                    customConfig.get(
                                            LLMTransformConfig.CustomRequestConfig
                                                    .CUSTOM_REQUEST_HEADERS),
                                    customConfig.get(
                                            ModelTransformConfig.CustomRequestConfig
                                                    .CUSTOM_REQUEST_BODY),
                                    customConfig.get(
                                            LLMTransformConfig.CustomRequestConfig
                                                    .CUSTOM_RESPONSE_PARSE),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case OPENAI:
                    model =
                            new OpenAIModel(
                                    config.get(ModelTransformConfig.API_KEY),
                                    config.get(ModelTransformConfig.MODEL),
                                    apiPath,
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case DOUBAO:
                    model =
                            new DoubaoModel(
                                    config.get(ModelTransformConfig.API_KEY),
                                    config.get(ModelTransformConfig.MODEL),
                                    apiPath,
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER),
                                    isMultimodalFields);
                    break;
                case QIANFAN:
                    model =
                            new QianfanModel(
                                    config.get(ModelTransformConfig.API_KEY),
                                    config.get(ModelTransformConfig.SECRET_KEY),
                                    config.get(ModelTransformConfig.MODEL),
                                    apiPath,
                                    config.get(ModelTransformConfig.OAUTH_PATH),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));

                    break;
                case ZHIPU:
                    model =
                            new ZhipuModel(
                                    config.get(ModelTransformConfig.API_KEY),
                                    config.get(ModelTransformConfig.MODEL),
                                    apiPath,
                                    config.get(ModelTransformConfig.DIMENSION),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case AMAZON:
                    model =
                            new BedrockModel(
                                    config.get(ModelTransformConfig.API_KEY),
                                    config.get(ModelTransformConfig.SECRET_KEY),
                                    config.get(ModelTransformConfig.AWS_REGION),
                                    config.get(ModelTransformConfig.API_PATH),
                                    config.get(ModelTransformConfig.MODEL),
                                    config.get(ModelTransformConfig.DIMENSION),
                                    config.get(
                                            EmbeddingTransformConfig
                                                    .SINGLE_VECTORIZED_INPUT_NUMBER));
                    break;
                case LOCAL:
                default:
                    throw new IllegalArgumentException("Unsupported model provider: " + provider);
            }
            if (isMultimodalFields && !(model instanceof MultimodalModel)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Model provider: %s does not support multimodal embedding",
                                provider));
            }
            dimension = model.dimension();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize model", e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void initOutputFields(SeaTunnelRowType inputRowType, ReadonlyConfig config) {
        Map<Integer, FieldSpec> fieldSpecMap = new HashMap<>();
        List<String> fieldNames = new ArrayList<>();
        Map<String, Object> fieldsConfig =
                config.get(EmbeddingTransformConfig.VECTORIZATION_FIELDS);
        if (fieldsConfig == null || fieldsConfig.isEmpty()) {
            throw new IllegalArgumentException("vectorization_fields configuration is required");
        }

        for (Map.Entry<String, Object> field : fieldsConfig.entrySet()) {
            FieldSpec fieldSpec = new FieldSpec(field);
            log.info("Field spec: {}", fieldSpec.toString());
            String srcField = fieldSpec.getFieldName();
            int srcFieldIndex;
            try {
                srcFieldIndex = inputRowType.indexOf(srcField);
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(getPluginName(), srcField);
            }
            if (fieldSpec.isMultimodalField()) {
                isMultimodalFields = true;
            }
            fieldSpecMap.put(srcFieldIndex, fieldSpec);
            fieldNames.add(field.getKey());
        }
        this.fieldSpecMap = fieldSpecMap;
        this.fieldNames = fieldNames;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        tryOpen();
        try {
            if (MetadataUtil.isBinaryFormat(inputRow)) {
                return vectorizationBinaryRow(inputRow);
            }
            Set<Integer> fieldOriginalIndexes = fieldSpecMap.keySet();
            Object[] fieldValues = new Object[fieldOriginalIndexes.size()];
            List<ByteBuffer> vectorization;
            int i = 0;

            for (Integer fieldOriginalIndex : fieldOriginalIndexes) {
                FieldSpec fieldSpec = fieldSpecMap.get(fieldOriginalIndex);
                Object value = inputRow.getField(fieldOriginalIndex);
                fieldValues[i++] =
                        isMultimodalFields ? new MultimodalFieldValue(fieldSpec, value) : value;
            }

            vectorization = model.vectorization(fieldValues);
            return vectorization.toArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to data vectorization", e);
        }
    }

    @Override
    @VisibleForTesting
    public Column[] getOutputColumns() {
        tryOpen();
        log.info("getOutputColumns: {}", fieldNames);
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

    public boolean isMultimodalFields() {
        return isMultimodalFields;
    }

    /** Process a row in binary format: [data, relativePath, partIndex] */
    private Object[] vectorizationBinaryRow(SeaTunnelRowAccessor inputRow) throws Exception {

        byte[] completeData = processBinaryRow(inputRow);
        if (completeData == null) {
            return null;
        }
        Set<Integer> fieldOriginalIndexes = fieldSpecMap.keySet();
        Object[] fieldValues = new Object[fieldOriginalIndexes.size()];
        int i = 0;

        for (Integer fieldOriginalIndex : fieldOriginalIndexes) {
            FieldSpec fieldSpec = fieldSpecMap.get(fieldOriginalIndex);
            if (fieldSpec.isBinary()) {
                fieldValues[i++] = new MultimodalFieldValue(fieldSpec, completeData);
            } else {
                log.warn(
                        "Non-binary field {} configured in binary format data",
                        fieldSpec.getFieldName());
                fieldValues[i++] = null;
            }
        }

        try {
            return model.vectorization(fieldValues).toArray();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to vectorize binary data for file: " + inputRow.toString(), e);
        }
    }

    private byte[] processBinaryRow(SeaTunnelRowAccessor inputRow) throws Exception {
        byte[] data = (byte[]) inputRow.getField(0);
        String relativePath = (String) inputRow.getField(1);
        long partIndex = (long) inputRow.getField(2);

        if (partIndex != -1) {
            checkPartOrder(relativePath, partIndex);
        }
        cacheBinaryChunk(relativePath, partIndex, data);
        if (MetadataUtil.isComplete(inputRow)) {
            byte[] completeFile = assembleCompleteFile(relativePath);
            cleanupFileCache(relativePath);
            log.info(
                    "Assembled complete file: {}, size: {} bytes",
                    relativePath,
                    completeFile.length);
            return completeFile;
        }
        return null;
    }

    /** Validate that partIndex is in correct order for the given file */
    private void checkPartOrder(String relativePath, long partIndex) throws Exception {
        Long lastPartIndex = partIndexMap.getOrDefault(relativePath, -1L);
        if (partIndex - 1 != lastPartIndex) {
            throw new Exception("Last order is " + lastPartIndex + ", but get " + partIndex);
        }
        partIndexMap.put(relativePath, partIndex);
    }

    private void cacheBinaryChunk(String relativePath, long partIndex, byte[] data) {
        if (partIndex >= 0) {
            binaryFileCache
                    .computeIfAbsent(relativePath, k -> new TreeMap<>())
                    .put(partIndex, data);
        }
    }

    private byte[] assembleCompleteFile(String relativePath) {
        TreeMap<Long, byte[]> chunks = binaryFileCache.get(relativePath);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            for (Map.Entry<Long, byte[]> entry : chunks.entrySet()) {
                byte[] chunk = entry.getValue();
                if (chunk.length > 0) {
                    outputStream.write(chunk);
                }
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to assemble complete file: " + relativePath, e);
        }
    }

    private void cleanupFileCache(String relativePath) {
        binaryFileCache.remove(relativePath);
        partIndexMap.remove(relativePath);
        log.info("Cleaned up cache and partIndex tracking for file: {}", relativePath);
    }

    @SneakyThrows
    @Override
    public void close() {
        if (model != null) {
            model.close();
        }
        binaryFileCache.clear();
        partIndexMap.clear();
    }
}
