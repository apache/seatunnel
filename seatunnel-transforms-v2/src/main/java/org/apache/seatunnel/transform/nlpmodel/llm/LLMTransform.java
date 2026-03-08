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

package org.apache.seatunnel.transform.nlpmodel.llm;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.transform.SeaTunnelBatchTransform;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;
import org.apache.seatunnel.transform.nlpmodel.ModelTransformConfig;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.Model;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.kimiai.KimiAIModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.microsoft.MicrosoftModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.openai.OpenAIModel;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class LLMTransform extends SingleFieldOutputTransform
        implements SeaTunnelBatchTransform<SeaTunnelRow, LLMTransform.LLMBatchState> {

    private static final Serializer<LLMBatchState> STATE_SERIALIZER = new DefaultSerializer<>();

    private final ReadonlyConfig config;
    private final SeaTunnelDataType<?> outputDataType;
    private final int processBatchSize;
    private final List<SeaTunnelRow> bufferedRows = new ArrayList<>();
    private final List<SeaTunnelRow> readyOutputs = new ArrayList<>();

    private Model model;

    public LLMTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.processBatchSize = config.get(ModelTransformConfig.PROCESS_BATCH_SIZE);
        this.outputDataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "output", config.get(LLMTransformConfig.OUTPUT_DATA_TYPE).toString());
        getProducedCatalogTable();
    }

    private void tryOpen() {
        if (model == null) {
            open();
        }
    }

    @Override
    public String getPluginName() {
        return "LLM";
    }

    @Override
    public void open() {
        ModelProvider provider = config.get(ModelTransformConfig.MODEL_PROVIDER);
        switch (provider) {
            case CUSTOM:
                ReadonlyConfig customConfig =
                        config.getOptional(ModelTransformConfig.CustomRequestConfig.CUSTOM_CONFIG)
                                .map(ReadonlyConfig::fromMap)
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        "Custom config can't be null"));
                model =
                        new CustomModel(
                                inputCatalogTable.getSeaTunnelRowType(),
                                outputDataType.getSqlType(),
                                config.get(LLMTransformConfig.INFERENCE_COLUMNS),
                                config.get(LLMTransformConfig.PROMPT),
                                config.get(LLMTransformConfig.MODEL),
                                provider.usedLLMPath(config.get(LLMTransformConfig.API_PATH)),
                                customConfig.get(
                                        LLMTransformConfig.CustomRequestConfig
                                                .CUSTOM_REQUEST_HEADERS),
                                customConfig.get(
                                        LLMTransformConfig.CustomRequestConfig.CUSTOM_REQUEST_BODY),
                                customConfig.get(
                                        LLMTransformConfig.CustomRequestConfig
                                                .CUSTOM_RESPONSE_PARSE));
                break;
            case MICROSOFT:
                model =
                        new MicrosoftModel(
                                inputCatalogTable.getSeaTunnelRowType(),
                                outputDataType.getSqlType(),
                                config.get(LLMTransformConfig.INFERENCE_COLUMNS),
                                config.get(LLMTransformConfig.PROMPT),
                                config.get(LLMTransformConfig.MODEL),
                                config.get(LLMTransformConfig.API_KEY),
                                provider.usedLLMPath(config.get(LLMTransformConfig.API_PATH)));
                break;
            case DEEPSEEK:
            case OPENAI:
            case DOUBAO:
            case ZHIPU:
                model =
                        new OpenAIModel(
                                inputCatalogTable.getSeaTunnelRowType(),
                                outputDataType.getSqlType(),
                                config.get(LLMTransformConfig.INFERENCE_COLUMNS),
                                config.get(LLMTransformConfig.PROMPT),
                                config.get(LLMTransformConfig.MODEL),
                                config.get(LLMTransformConfig.API_KEY),
                                provider.usedLLMPath(config.get(LLMTransformConfig.API_PATH)));
                break;
            case KIMIAI:
                model =
                        new KimiAIModel(
                                inputCatalogTable.getSeaTunnelRowType(),
                                outputDataType.getSqlType(),
                                config.get(LLMTransformConfig.INFERENCE_COLUMNS),
                                config.get(LLMTransformConfig.PROMPT),
                                config.get(LLMTransformConfig.MODEL),
                                config.get(LLMTransformConfig.API_KEY),
                                provider.usedLLMPath(config.get(LLMTransformConfig.API_PATH)));
                break;
            case QIANFAN:
            default:
                throw new IllegalArgumentException("Unsupported model provider: " + provider);
        }
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        tryOpen();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(inputRow.getFields());
        try {
            List<String> values = model.inference(Collections.singletonList(seaTunnelRow));
            return convertOutputValue(values.get(0));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to inference model with row %s", seaTunnelRow), e);
        }
    }

    @Override
    public void collect(SeaTunnelRow row) {
        tryOpen();
        bufferedRows.add(row.copy());
        processBufferedRows(false);
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
        processBufferedRows(true);
        return drainOutput();
    }

    @Override
    public List<LLMBatchState> snapshotState(long checkpointId) {
        if (bufferedRows.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new LLMBatchState(copyRows(bufferedRows)));
    }

    @Override
    public void restoreState(List<LLMBatchState> states) {
        bufferedRows.clear();
        for (LLMBatchState state : states) {
            bufferedRows.addAll(copyRows(state.getBufferedRows()));
        }
    }

    @Override
    public Optional<Serializer<LLMBatchState>> getStateSerializer() {
        return Optional.of(STATE_SERIALIZER);
    }

    @Override
    public boolean hasBufferedData() {
        return !bufferedRows.isEmpty();
    }

    @Override
    public int getBufferSize() {
        return bufferedRows.size();
    }

    @Override
    protected Column getOutputColumn() {
        String customFieldName = config.get(LLMTransformConfig.OUTPUT_COLUMN_NAME);
        String[] fieldNames = inputCatalogTable.getTableSchema().getFieldNames();
        boolean isExist = Arrays.asList(fieldNames).contains(customFieldName);
        if (isExist) {
            throw new IllegalArgumentException(
                    String.format("llm inference field name %s already exists", customFieldName));
        }
        return PhysicalColumn.of(
                customFieldName, outputDataType, (Long) null, true, null, "Output column of LLM");
    }

    @SneakyThrows
    @Override
    public void close() {
        if (model != null) {
            model.close();
        }
        bufferedRows.clear();
        readyOutputs.clear();
    }

    private void processBufferedRows(boolean forceFlush) {
        while (!bufferedRows.isEmpty() && (forceFlush || bufferedRows.size() >= processBatchSize)) {
            int currentBatchSize =
                    forceFlush ? Math.min(processBatchSize, bufferedRows.size()) : processBatchSize;
            List<SeaTunnelRow> batchRows =
                    new ArrayList<>(bufferedRows.subList(0, currentBatchSize));
            bufferedRows.subList(0, currentBatchSize).clear();
            readyOutputs.addAll(inferenceBatch(batchRows));
        }
    }

    private List<SeaTunnelRow> inferenceBatch(List<SeaTunnelRow> batchRows) {
        try {
            List<String> values = model.inference(batchRows);
            if (values.size() != batchRows.size()) {
                throw new IllegalStateException(
                        String.format(
                                "Expected %s outputs but model returned %s outputs",
                                batchRows.size(), values.size()));
            }
            List<SeaTunnelRow> outputs = new ArrayList<>(batchRows.size());
            for (int i = 0; i < batchRows.size(); i++) {
                outputs.add(buildOutputRow(batchRows.get(i), values.get(i)));
            }
            return outputs;
        } catch (Exception e) {
            throw new RuntimeException("Failed to inference batch rows with LLM transform", e);
        }
    }

    private SeaTunnelRow buildOutputRow(SeaTunnelRow inputRow, String value) {
        SeaTunnelRow outputRow = getRowContainerGenerator().apply(inputRow);
        outputRow.setField(getFieldIndex(), convertOutputValue(value));
        return outputRow;
    }

    private Object convertOutputValue(String value) {
        switch (outputDataType.getSqlType()) {
            case STRING:
                return String.valueOf(value);
            case INT:
                return Integer.parseInt(value);
            case BIGINT:
                return Long.parseLong(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            default:
                throw new IllegalArgumentException(
                        "Unsupported output data type: " + outputDataType);
        }
    }

    private List<SeaTunnelRow> copyRows(List<SeaTunnelRow> rows) {
        List<SeaTunnelRow> copiedRows = new ArrayList<>(rows.size());
        for (SeaTunnelRow row : rows) {
            copiedRows.add(row.copy());
        }
        return copiedRows;
    }

    @VisibleForTesting
    void setModel(Model model) {
        this.model = model;
    }

    public static final class LLMBatchState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<SeaTunnelRow> bufferedRows;

        public LLMBatchState(List<SeaTunnelRow> bufferedRows) {
            this.bufferedRows = bufferedRows;
        }

        public List<SeaTunnelRow> getBufferedRows() {
            return bufferedRows;
        }
    }
}
