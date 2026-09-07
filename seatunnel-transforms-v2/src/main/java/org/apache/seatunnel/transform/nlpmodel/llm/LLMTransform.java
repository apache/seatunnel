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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;
import org.apache.seatunnel.transform.nlpmodel.ModelTransformConfig;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.Model;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.kimiai.KimiAIModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.microsoft.MicrosoftModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.openai.OpenAIModel;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LLMTransform extends SingleFieldOutputTransform {
    private static final long serialVersionUID = 4711686225005641485L;

    private final ReadonlyConfig config;
    private final SeaTunnelDataType<?> outputDataType;
    private final boolean strictBooleanOutput;
    private Model model;

    public LLMTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.outputDataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "output", config.get(LLMTransformConfig.OUTPUT_DATA_TYPE).toString());
        this.strictBooleanOutput = config.get(LLMTransformConfig.STRICT_BOOLEAN_OUTPUT);
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
                // load custom_config from the configuration
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
        List<String> values;
        try {
            values = model.inference(Collections.singletonList(seaTunnelRow));
        } catch (Exception e) {
            throw inferenceFailure(seaTunnelRow, e);
        }
        // Keep validation outside legacy error wrapping so rejected output does not expose the row.
        if (strictBooleanOutput && outputDataType.getSqlType() == SqlType.BOOLEAN) {
            return parseStrictBooleanOutput(values);
        }
        try {
            switch (outputDataType.getSqlType()) {
                case STRING:
                    return String.valueOf(values.get(0));
                case INT:
                    return Integer.parseInt(values.get(0));
                case BIGINT:
                    return Long.parseLong(values.get(0));
                case DOUBLE:
                    return Double.parseDouble(values.get(0));
                case BOOLEAN:
                    return Boolean.parseBoolean(values.get(0));
                default:
                    throw new IllegalArgumentException(
                            "Unsupported output data type: " + outputDataType);
            }
        } catch (Exception e) {
            throw inferenceFailure(seaTunnelRow, e);
        }
    }

    private boolean parseStrictBooleanOutput(List<String> values) {
        String value = values != null && values.size() == 1 ? values.get(0) : null;
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw TransformCommonError.validationFailed(
                "LLM strict_boolean_output requires exactly one non-null true or false result "
                        + "for output_data_type=BOOLEAN, without surrounding whitespace");
    }

    private RuntimeException inferenceFailure(SeaTunnelRow row, Exception cause) {
        return new RuntimeException(
                String.format("Failed to inference model with row %s", row), cause);
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
    }
}
