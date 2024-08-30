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

package org.apache.seatunnel.transform.embadding;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.Map;

public class EmbeddingTransformConfig implements Serializable {

    public static final Option<EmbeddingModelProvider> EMBEDDING_MODEL_PROVIDER =
            Options.key("embedding_model_provider")
                    .enumType(EmbeddingModelProvider.class)
                    .noDefaultValue()
                    .withDescription("The model provider of embedding");

    public static final Option<String> API_KEY =
            Options.key("api_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API key of Embedding");

    public static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Secret key of Embedding");

    public static final Option<Integer> SINGLE_VECTORIZED_INPUT_NUMBER =
            Options.key("single_vectorized_input_number")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of single vectorized inputs, default is 1 , which means 1 inputs will be vectorized in one request , eg: qianfan only allows a maximum of 16 simultaneous messages, depending on your own settings, etc");

    public static final Option<Integer> VECTORIZATION_BATCH_SIZE =
            Options.key("vectorization_batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The row batch size of each vectorization");

    public static final Option<Map<String, String>> VECTORIZATION_FIELDS =
            Options.key("vectorization_fields")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the field vectorization relationship between input and output");

    public static final Option<String> MODEL =
            Options.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The model of Embedding, eg: if the model provider is OpenAI, the model should be text-embedding-3-small/text-embedding-3-large, etc.");

    public static final Option<String> API_PATH =
            Options.key("api_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The API of Embedding");

    public static final Option<String> OAUTH_PATH =
            Options.key("oauth_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Oauth path of Embedding");
}
