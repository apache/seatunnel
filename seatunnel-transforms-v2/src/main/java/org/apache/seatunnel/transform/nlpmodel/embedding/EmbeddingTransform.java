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
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.api.transform.SeaTunnelBatchTransform;
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
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class EmbeddingTransform extends MultipleFieldOutputTransform
        implements SeaTunnelBatchTransform<SeaTunnelRow, EmbeddingTransform.EmbeddingBatchState> {

    private static final Serializer<EmbeddingBatchState> STATE_SERIALIZER =
            new DefaultSerializer<>();

    private final ReadonlyConfig config;
    private final int processBatchSize;
    private final List<BufferedInput> bufferedInputs = new ArrayList<>();
    private final List<SeaTunnelRow> readyOutputs = new ArrayList<>();
    private final Map<String, TreeMap<Long, byte[]>> binaryFileCache = new ConcurrentHashMap<>();
    private final Map<String, Long> partIndexMap = new ConcurrentHashMap<>();

    private transient Model model;
    private Integer dimension;
    private boolean multimodalFields;
    private Map<Integer, FieldSpec> fieldSpecMap;
    private List<String> fieldNames;

    public EmbeddingTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.processBatchSize = config.get(ModelTransformConfig.PROCESS_BATCH_SIZE);
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
                        config.get(ModelTransformConfig.API_PATH), multimodalFields);
        try {
            switch (provider) {
                case CUSTOM:
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
                                    multimodalFields);
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
            if (multimodalFields && !(model instanceof MultimodalModel)) {
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
        Map<Integer, FieldSpec> configuredFieldSpecMap = new LinkedHashMap<>();
        List<String> configuredFieldNames = new ArrayList<>();
        Map<String, Object> fieldsConfig =
                config.get(EmbeddingTransformConfig.VECTORIZATION_FIELDS);
        if (fieldsConfig == null || fieldsConfig.isEmpty()) {
            throw new IllegalArgumentException("vectorization_fields configuration is required");
        }

        for (Map.Entry<String, Object> field : fieldsConfig.entrySet()) {
            FieldSpec fieldSpec = new FieldSpec(field);
            String srcField = fieldSpec.getFieldName();
            int srcFieldIndex;
            try {
                srcFieldIndex = inputRowType.indexOf(srcField);
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(getPluginName(), srcField);
            }
            if (fieldSpec.isMultimodalField()) {
                multimodalFields = true;
            }
            configuredFieldSpecMap.put(srcFieldIndex, fieldSpec);
            configuredFieldNames.add(field.getKey());
            log.info("Field spec: {}", fieldSpec);
        }
        this.fieldSpecMap = configuredFieldSpecMap;
        this.fieldNames = configuredFieldNames;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        tryOpen();
        try {
            if (MetadataUtil.isBinaryFormat(inputRow)) {
                return vectorizePreparedFields(createBinaryFieldValues(processBinaryRow(inputRow)));
            }
            return vectorizePreparedFields(extractFieldValues(inputRow));
        } catch (Exception e) {
            throw new RuntimeException("Failed to data vectorization", e);
        }
    }

    @Override
    public void collect(SeaTunnelRow row) {
        SeaTunnelRow bufferedRow = row.copy();
        try {
            if (MetadataUtil.isBinaryFormat(bufferedRow)) {
                byte[] completeData = processBinaryRow(new SeaTunnelRowAccessor(bufferedRow));
                if (completeData != null) {
                    bufferedInputs.add(new BufferedInput(bufferedRow, completeData));
                }
            } else {
                bufferedInputs.add(new BufferedInput(bufferedRow, null));
            }
            processBufferedInputs(false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect row for embedding batch transform", e);
        }
    }

    @Override
    public List<SeaTunnelRow> drainOutput() {
        if (readyOutputs.isEmpty()) {
            return Collections.emptyList();
        }
        List<SeaTunnelRow> outputs = new ArrayList<>(readyOutputs);
        readyOutputs.clear();
        return outputs;
    }

    @Override
    public List<SeaTunnelRow> flush() {
        processBufferedInputs(true);
        return drainOutput();
    }

    @Override
    public List<EmbeddingBatchState> snapshotState(long checkpointId) {
        if (bufferedInputs.isEmpty() && binaryFileCache.isEmpty() && partIndexMap.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
                new EmbeddingBatchState(
                        copyBufferedInputs(bufferedInputs),
                        copyBinaryFileCache(binaryFileCache),
                        new LinkedHashMap<>(partIndexMap)));
    }

    @Override
    public void restoreState(List<EmbeddingBatchState> states) {
        bufferedInputs.clear();
        binaryFileCache.clear();
        partIndexMap.clear();
        for (EmbeddingBatchState state : states) {
            bufferedInputs.addAll(copyBufferedInputs(state.getBufferedInputs()));
            mergeBinaryFileCache(binaryFileCache, state.getBinaryFileCache());
            partIndexMap.putAll(state.getPartIndexMap());
        }
    }

    @Override
    public Optional<Serializer<EmbeddingBatchState>> getStateSerializer() {
        return Optional.of(STATE_SERIALIZER);
    }

    @Override
    public boolean hasBufferedData() {
        return !bufferedInputs.isEmpty() || !binaryFileCache.isEmpty();
    }

    @Override
    public int getBufferSize() {
        return bufferedInputs.size();
    }

    @Override
    @VisibleForTesting
    public Column[] getOutputColumns() {
        tryOpen();
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
        return multimodalFields;
    }

    @SneakyThrows
    @Override
    public void close() {
        if (model != null) {
            model.close();
        }
        bufferedInputs.clear();
        readyOutputs.clear();
        binaryFileCache.clear();
        partIndexMap.clear();
    }

    private void processBufferedInputs(boolean forceFlush) {
        while (!bufferedInputs.isEmpty()
                && (forceFlush || bufferedInputs.size() >= processBatchSize)) {
            tryOpen();
            getProducedCatalogTable();
            int currentBatchSize =
                    forceFlush
                            ? Math.min(processBatchSize, bufferedInputs.size())
                            : processBatchSize;
            List<BufferedInput> batchInputs =
                    new ArrayList<>(bufferedInputs.subList(0, currentBatchSize));
            bufferedInputs.subList(0, currentBatchSize).clear();
            readyOutputs.addAll(vectorizeBatch(batchInputs));
        }
    }

    private List<SeaTunnelRow> vectorizeBatch(List<BufferedInput> batchInputs) {
        List<Object[]> rowFieldValuesList = new ArrayList<>(batchInputs.size());
        List<Object> mergedFieldValues = new ArrayList<>();
        for (BufferedInput batchInput : batchInputs) {
            Object[] rowFieldValues =
                    batchInput.isBinary()
                            ? createBinaryFieldValues(batchInput.getCompleteBinaryData())
                            : extractFieldValues(new SeaTunnelRowAccessor(batchInput.getRow()));
            rowFieldValuesList.add(rowFieldValues);
            mergedFieldValues.addAll(Arrays.asList(rowFieldValues));
        }

        try {
            List<ByteBuffer> embeddings = model.vectorization(mergedFieldValues.toArray());
            if (embeddings.size() != mergedFieldValues.size()) {
                throw new IllegalStateException(
                        String.format(
                                "Expected %s vectors but model returned %s vectors",
                                mergedFieldValues.size(), embeddings.size()));
            }
            List<SeaTunnelRow> outputs = new ArrayList<>(batchInputs.size());
            int embeddingIndex = 0;
            for (int i = 0; i < batchInputs.size(); i++) {
                Object[] rowOutputValues = new Object[rowFieldValuesList.get(i).length];
                for (int j = 0; j < rowOutputValues.length; j++) {
                    rowOutputValues[j] = embeddings.get(embeddingIndex++);
                }
                outputs.add(buildOutputRow(batchInputs.get(i).getRow(), rowOutputValues));
            }
            return outputs;
        } catch (Exception e) {
            throw new RuntimeException("Failed to process embedding batch", e);
        }
    }

    private SeaTunnelRow buildOutputRow(SeaTunnelRow inputRow, Object[] fieldValues) {
        SeaTunnelRow outputRow = getRowContainerGenerator().apply(inputRow);
        int[] fieldsIndex = getFieldsIndex();
        for (int i = 0; i < fieldValues.length; i++) {
            outputRow.setField(fieldsIndex[i], fieldValues[i]);
        }
        return outputRow;
    }

    private Object[] extractFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] fieldValues = new Object[fieldSpecMap.size()];
        int i = 0;
        for (Map.Entry<Integer, FieldSpec> entry : fieldSpecMap.entrySet()) {
            Object value = inputRow.getField(entry.getKey());
            fieldValues[i++] =
                    multimodalFields ? new MultimodalFieldValue(entry.getValue(), value) : value;
        }
        return fieldValues;
    }

    private Object[] vectorizePreparedFields(Object[] fieldValues) throws IOException {
        if (fieldValues == null) {
            return null;
        }
        return model.vectorization(fieldValues).toArray();
    }

    private Object[] createBinaryFieldValues(byte[] completeData) {
        if (completeData == null) {
            return null;
        }
        Object[] fieldValues = new Object[fieldSpecMap.size()];
        int i = 0;
        for (FieldSpec fieldSpec : fieldSpecMap.values()) {
            if (fieldSpec.isBinary()) {
                fieldValues[i++] = new MultimodalFieldValue(fieldSpec, completeData);
            } else {
                log.warn(
                        "Non-binary field {} configured in binary format data",
                        fieldSpec.getFieldName());
                fieldValues[i++] = null;
            }
        }
        return fieldValues;
    }

    private byte[] processBinaryRow(SeaTunnelRowAccessor inputRow) throws Exception {
        byte[] data = (byte[]) inputRow.getField(0);
        String relativePath = (String) inputRow.getField(1);
        long partIndex = (long) inputRow.getField(2);

        if (partIndex >= 0) {
            checkPartOrder(relativePath, partIndex);
            cacheBinaryChunk(relativePath, partIndex, data);
        }
        if (!MetadataUtil.isComplete(inputRow)) {
            return null;
        }
        if (partIndex < 0) {
            return data;
        }
        byte[] completeFile = assembleCompleteFile(relativePath);
        cleanupFileCache(relativePath);
        log.info("Assembled complete file: {}, size: {} bytes", relativePath, completeFile.length);
        return completeFile;
    }

    private void checkPartOrder(String relativePath, long partIndex) throws Exception {
        Long lastPartIndex = partIndexMap.getOrDefault(relativePath, -1L);
        if (partIndex - 1 != lastPartIndex) {
            throw new Exception("Last order is " + lastPartIndex + ", but get " + partIndex);
        }
        partIndexMap.put(relativePath, partIndex);
    }

    private void cacheBinaryChunk(String relativePath, long partIndex, byte[] data) {
        binaryFileCache.computeIfAbsent(relativePath, key -> new TreeMap<>()).put(partIndex, data);
    }

    private byte[] assembleCompleteFile(String relativePath) {
        TreeMap<Long, byte[]> chunks = binaryFileCache.get(relativePath);
        if (chunks == null || chunks.isEmpty()) {
            throw new IllegalStateException("Missing binary chunks for file: " + relativePath);
        }
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

    private List<BufferedInput> copyBufferedInputs(List<BufferedInput> inputs) {
        List<BufferedInput> copiedInputs = new ArrayList<>(inputs.size());
        for (BufferedInput input : inputs) {
            copiedInputs.add(input.copy());
        }
        return copiedInputs;
    }

    private Map<String, TreeMap<Long, byte[]>> copyBinaryFileCache(
            Map<String, TreeMap<Long, byte[]>> source) {
        Map<String, TreeMap<Long, byte[]>> copied = new LinkedHashMap<>();
        mergeBinaryFileCache(copied, source);
        return copied;
    }

    private void mergeBinaryFileCache(
            Map<String, TreeMap<Long, byte[]>> target, Map<String, TreeMap<Long, byte[]>> source) {
        for (Map.Entry<String, TreeMap<Long, byte[]>> entry : source.entrySet()) {
            TreeMap<Long, byte[]> chunks =
                    target.computeIfAbsent(entry.getKey(), key -> new TreeMap<>());
            for (Map.Entry<Long, byte[]> chunkEntry : entry.getValue().entrySet()) {
                chunks.put(chunkEntry.getKey(), copyBytes(chunkEntry.getValue()));
            }
        }
    }

    private byte[] copyBytes(byte[] value) {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @VisibleForTesting
    void setModel(Model model) {
        this.model = model;
    }

    static final class BufferedInput implements Serializable {
        private static final long serialVersionUID = 1L;

        private final SeaTunnelRow row;
        private final byte[] completeBinaryData;

        private BufferedInput(SeaTunnelRow row, byte[] completeBinaryData) {
            this.row = row;
            this.completeBinaryData = completeBinaryData;
        }

        public SeaTunnelRow getRow() {
            return row;
        }

        public byte[] getCompleteBinaryData() {
            return completeBinaryData;
        }

        public boolean isBinary() {
            return completeBinaryData != null;
        }

        public BufferedInput copy() {
            return new BufferedInput(
                    row.copy(),
                    completeBinaryData == null
                            ? null
                            : Arrays.copyOf(completeBinaryData, completeBinaryData.length));
        }
    }

    public static final class EmbeddingBatchState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<BufferedInput> bufferedInputs;
        private final Map<String, TreeMap<Long, byte[]>> binaryFileCache;
        private final Map<String, Long> partIndexMap;

        public EmbeddingBatchState(
                List<BufferedInput> bufferedInputs,
                Map<String, TreeMap<Long, byte[]>> binaryFileCache,
                Map<String, Long> partIndexMap) {
            this.bufferedInputs = bufferedInputs;
            this.binaryFileCache = binaryFileCache;
            this.partIndexMap = partIndexMap;
        }

        public List<BufferedInput> getBufferedInputs() {
            return bufferedInputs;
        }

        public Map<String, TreeMap<Long, byte[]>> getBinaryFileCache() {
            return binaryFileCache;
        }

        public Map<String, Long> getPartIndexMap() {
            return partIndexMap;
        }
    }
}
