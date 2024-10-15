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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.nlpmodel.ModelProvider;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class LLMTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "LLM";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        LLMTransformConfig.MODEL_PROVIDER,
                        LLMTransformConfig.MODEL,
                        LLMTransformConfig.PROMPT)
                .optional(
                        LLMTransformConfig.API_PATH,
                        LLMTransformConfig.OUTPUT_DATA_TYPE,
                        LLMTransformConfig.PROCESS_BATCH_SIZE)
                .conditional(
                        LLMTransformConfig.MODEL_PROVIDER,
                        Lists.newArrayList(
                                ModelProvider.OPENAI,
                                ModelProvider.DOUBAO,
                                ModelProvider.MICROSOFT),
                        LLMTransformConfig.API_KEY)
                .conditional(
                        LLMTransformConfig.MODEL_PROVIDER,
                        ModelProvider.QIANFAN,
                        LLMTransformConfig.API_KEY,
                        LLMTransformConfig.SECRET_KEY,
                        LLMTransformConfig.OAUTH_PATH)
                .conditional(
                        LLMTransformConfig.MODEL_PROVIDER,
                        ModelProvider.CUSTOM,
                        LLMTransformConfig.CustomRequestConfig.CUSTOM_CONFIG)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        CatalogTable catalogTable = context.getCatalogTables().get(0);
        return () -> new LLMTransform(context.getOptions(), catalogTable);
    }
}
