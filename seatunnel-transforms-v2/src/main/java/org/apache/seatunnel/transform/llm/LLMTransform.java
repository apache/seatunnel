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

package org.apache.seatunnel.transform.llm;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;
import org.apache.seatunnel.transform.llm.model.Model;
import org.apache.seatunnel.transform.llm.model.openai.OpenAIModel;

import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.List;

public class LLMTransform extends SingleFieldOutputTransform {
    private final ReadonlyConfig config;
    private final SeaTunnelDataType<?> outputDataType;
    private Model model;

    public LLMTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.outputDataType =
                SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                        "output", config.get(LLMTransformConfig.OUTPUT_DATA_TYPE).toString());
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
        ModelProvider provider = config.get(LLMTransformConfig.MODEL_PROVIDER);
        if (provider.equals(ModelProvider.OPENAI)) {
            model =
                    new OpenAIModel(
                            inputCatalogTable.getSeaTunnelRowType(),
                            outputDataType.getSqlType(),
                            config.get(LLMTransformConfig.PROMPT),
                            config.get(LLMTransformConfig.MODEL),
                            config.get(LLMTransformConfig.API_KEY),
                            config.get(LLMTransformConfig.OPENAI_API_PATH));
        } else {
            throw new IllegalArgumentException("Unsupported model provider: " + provider);
        }
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        tryOpen();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(inputRow.getFields());
        try {
            List<String> values = model.inference(Collections.singletonList(seaTunnelRow));
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
            throw new RuntimeException(
                    String.format("Failed to inference model with row %s", seaTunnelRow), e);
        }
    }

    @Override
    protected Column getOutputColumn() {
        return PhysicalColumn.of(
                "llm_output", outputDataType, (Long) null, true, null, "Output column of LLM");
    }

    @SneakyThrows
    @Override
    public void close() {
        if (model != null) {
            model.close();
        }
    }
}
