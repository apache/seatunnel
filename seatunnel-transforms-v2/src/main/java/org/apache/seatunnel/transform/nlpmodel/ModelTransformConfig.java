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

package org.apache.seatunnel.transform.nlpmodel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;
import java.util.Map;

public class ModelTransformConfig implements Serializable {

    public static final Option<ModelProvider> MODEL_PROVIDER =
            Options.key("model_provider")
                    .enumType(ModelProvider.class)
                    .noDefaultValue()
                    .withDescription("The model provider of LLM/Embedding");

    public static final Option<SqlType> OUTPUT_DATA_TYPE =
            Options.key("output_data_type")
                    .enumType(SqlType.class)
                    .defaultValue(SqlType.STRING)
                    .withDescription("The output data type of LLM");

    public static final Option<String> MODEL =
            Options.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The model of LLM/Embedding, eg: if the model provider is OpenAI LLM, the model should be gpt-3.5-turbo/gpt-4o-mini, etc.");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key of LLM/Embedding");

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Secret key of LLM/Embedding");

    public static final Option<String> API_PATH =
            Options.key("api_path")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("openai.api_path")
                    .withDescription("The API of LLM/Embedding");

    public static final Option<String> OAUTH_PATH =
            Options.key("oauth_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Oauth path of LLM/Embedding");

    public static final Option<Integer> PROCESS_BATCH_SIZE =
            Options.key("process_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withFallbackKeys("inference_batch_size")
                    .withDescription("The row batch size of each process");

    public static class CustomRequestConfig {

        // Custom response parsing
        public static final Option<Map<String, Object>> CUSTOM_CONFIG =
                Options.key("custom_config")
                        .type(new TypeReference<Map<String, Object>>() {})
                        .noDefaultValue()
                        .withDescription("The custom config of the custom model.");

        public static final Option<String> CUSTOM_RESPONSE_PARSE =
                Options.key("custom_response_parse")
                        .stringType()
                        .noDefaultValue()
                        .withDescription(
                                "The response parse of the custom model. You can use Jsonpath to parse the return object you want to parse. eg: $.choices[*].message.content");

        public static final Option<Map<String, String>> CUSTOM_REQUEST_HEADERS =
                Options.key("custom_request_headers")
                        .mapType()
                        .noDefaultValue()
                        .withDescription("The custom request headers of the custom model.");

        public static final Option<Map<String, Object>> CUSTOM_REQUEST_BODY =
                Options.key("custom_request_body")
                        .type(new TypeReference<Map<String, Object>>() {})
                        .noDefaultValue()
                        .withDescription(
                                "The custom request body of the custom model."
                                        + "1. ${model} placeholder for selecting model name."
                                        + "2. ${input} placeholder for Determine input type. eg: [\"${input}\"]"
                                        + "3. ${prompt} placeholder for LLM model "
                                        + "4. ...");
    }
}
