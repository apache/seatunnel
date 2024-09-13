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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.transform.nlpmodel.ModelTransformConfig;

import java.util.List;

public class LLMTransformConfig extends ModelTransformConfig {

    public static final Option<String> PROMPT =
            Options.key("prompt")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The prompt of LLM");

    public static final Option<List<String>> INFERENCE_COLUMNS =
            Options.key("inference_columns")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The row projection field of each inference");

    public static final Option<String> OUTPUT_COLUMN_NAME =
            Options.key("output_column_name")
                    .stringType()
                    .defaultValue("llm_output")
                    .withDescription("custom field name for the llm output data");

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
                    .withDescription("The API path of OpenAI LLM");
}
