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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;
import org.apache.seatunnel.transform.nlpmodel.llm.LLMTransformConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class EmbeddingTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "Embedding";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        EmbeddingTransformConfig.MODEL_PROVIDER,
                        EmbeddingTransformConfig.MODEL,
                        EmbeddingTransformConfig.VECTORIZATION_FIELDS)
                .optional(
                        EmbeddingTransformConfig.API_PATH,
                        EmbeddingTransformConfig.SINGLE_VECTORIZED_INPUT_NUMBER,
                        EmbeddingTransformConfig.PROCESS_BATCH_SIZE)
                .conditional(
                        EmbeddingTransformConfig.MODEL_PROVIDER,
                        Lists.newArrayList(ModelProvider.OPENAI, ModelProvider.DOUBAO),
                        EmbeddingTransformConfig.API_KEY)
                .conditional(
                        EmbeddingTransformConfig.MODEL_PROVIDER,
                        ModelProvider.QIANFAN,
                        EmbeddingTransformConfig.API_KEY,
                        EmbeddingTransformConfig.SECRET_KEY,
                        EmbeddingTransformConfig.OAUTH_PATH)
                .conditional(
                        LLMTransformConfig.MODEL_PROVIDER,
                        ModelProvider.CUSTOM,
                        LLMTransformConfig.CustomRequestConfig.CUSTOM_CONFIG)
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new EmbeddingMultiCatalogTransform(
                        context.getCatalogTables(), context.getOptions());
    }
}
