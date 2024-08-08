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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;

public class LLMTransformConfig implements Serializable {

    public static final Option<ModelProvider> MODEL_PROVIDER =
            Options.key("model_provider")
                    .enumType(ModelProvider.class)
                    .noDefaultValue()
                    .withDescription("The model provider of LLM");

    public static final Option<SqlType> OUTPUT_DATA_TYPE =
            Options.key("output_data_type")
                    .enumType(SqlType.class)
                    .defaultValue(SqlType.STRING)
                    .withDescription("The output data type of LLM");

    public static final Option<String> PROMPT =
            Options.key("prompt")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prompt of LLM");

    public static final Option<String> MODEL =
            Options.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The model of LLM, eg: if the model provider is OpenAI, the model should be gpt-3.5-turbo/gpt-4o-mini, etc.");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key of LLM");

    public static final Option<Integer> INFERENCE_BATCH_SIZE =
            Options.key("inference_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The row batch size of each inference");

    // OPENAI specific options
    public static final Option<String> OPENAI_API_PATH =
            Options.key("openai.api_path")
                    .stringType()
                    .defaultValue("https://api.openai.com/v1/chat/completions")
                    .withDescription("The API path of OpenAI");
}
