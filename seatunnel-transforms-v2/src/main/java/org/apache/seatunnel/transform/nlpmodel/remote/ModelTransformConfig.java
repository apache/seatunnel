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

package org.apache.seatunnel.transform.nlpmodel.remote;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;

public class ModelTransformConfig implements Serializable {

    public static final Option<ModelProvider> MODEL_PROVIDER =
            Options.key("model_provider")
                    .enumType(ModelProvider.class)
                    .noDefaultValue()
                    .withDescription("The model provider of LLM/Embedding");

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
}
