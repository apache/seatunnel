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

import org.apache.commons.lang3.StringUtils;

public enum ModelProvider {
    OPENAI("https://api.openai.com/v1/chat/completions", "https://api.openai.com/v1/embeddings"),
    DOUBAO(
            "https://ark.cn-beijing.volces.com/api/v3/chat/completions",
            "https://ark.cn-beijing.volces.com/api/v3/embeddings"),
    QIANFAN("", "https://aip.baidubce.com/rpc/2.0/ai_custom/v1/wenxinworkshop/embeddings"),
    KIMIAI("https://api.moonshot.cn/v1/chat/completions", ""),
    DEEPSEEK("https://api.deepseek.com/chat/completions", ""),
    MICROSOFT("", ""),
    CUSTOM("", ""),
    LOCAL("", "");

    private final String LLMProviderPath;
    private final String EmbeddingProviderPath;

    ModelProvider(String llmProviderPath, String embeddingProviderPath) {
        LLMProviderPath = llmProviderPath;
        EmbeddingProviderPath = embeddingProviderPath;
    }

    public String usedLLMPath(String path) {
        if (StringUtils.isBlank(path)) {
            return LLMProviderPath;
        }
        return path;
    }

    public String usedEmbeddingPath(String path) {
        if (StringUtils.isBlank(path)) {
            return EmbeddingProviderPath;
        }
        return path;
    }
}
