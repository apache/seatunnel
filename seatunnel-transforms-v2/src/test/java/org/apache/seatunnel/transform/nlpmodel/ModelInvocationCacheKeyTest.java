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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ModelInvocationCacheKeyTest {

    @Test
    void keyIsStableForEquivalentInputAndMetadataOrdering() {
        String first =
                ModelInvocationCacheKey.builder()
                        .provider(" OPENAI ")
                        .model("text-embedding-3-small")
                        .dimension(1536)
                        .modality(" Text ")
                        .format("TEXT")
                        .input("first line\r\nsecond line")
                        .metadata("chunk_id", "chunk-1")
                        .metadata("content_hash", "hash-1")
                        .build();
        String second =
                ModelInvocationCacheKey.builder()
                        .provider("OPENAI")
                        .model("text-embedding-3-small")
                        .dimension(1536)
                        .modality("text")
                        .format("text")
                        .input("first line\nsecond line")
                        .metadata("content_hash", "hash-1")
                        .metadata("chunk_id", "chunk-1")
                        .build();

        Assertions.assertEquals(first, second);
    }

    @Test
    void keySeparatesProviderModelDimensionModalityAndFormat() {
        String base =
                ModelInvocationCacheKey.builder()
                        .provider("OPENAI")
                        .model("text-embedding-3-small")
                        .dimension(1536)
                        .modality("text")
                        .format("text")
                        .input("same chunk")
                        .build();

        Assertions.assertNotEquals(
                base, keyWith("DOUBAO", "text-embedding-3-small", 1536, "text", "text"));
        Assertions.assertNotEquals(
                base, keyWith("OPENAI", "text-embedding-3-large", 1536, "text", "text"));
        Assertions.assertNotEquals(
                base, keyWith("OPENAI", "text-embedding-3-small", 1024, "text", "text"));
        Assertions.assertNotEquals(
                base, keyWith("OPENAI", "text-embedding-3-small", 1536, "jpeg", "text"));
        Assertions.assertNotEquals(
                base, keyWith("OPENAI", "text-embedding-3-small", 1536, "text", "url"));
    }

    @Test
    void keyDoesNotExposeRawLargeOrSensitiveInputContent() {
        String sensitiveInput =
                "api_key=secret-token\n"
                        + "customer text that should only participate through a digest ";

        String key =
                ModelInvocationCacheKey.builder()
                        .provider("OPENAI")
                        .model("text-embedding-3-small")
                        .dimension(1536)
                        .modality("text")
                        .format("text")
                        .input(sensitiveInput)
                        .build();

        Assertions.assertFalse(key.contains("api_key"));
        Assertions.assertFalse(key.contains("secret-token"));
        Assertions.assertFalse(key.contains("customer text"));
        Assertions.assertTrue(key.contains("input_sha256="));
    }

    @Test
    void binaryInputDigestUsesContentInsteadOfArrayIdentity() {
        String first =
                ModelInvocationCacheKey.builder()
                        .provider("BEDROCK")
                        .model("amazon.titan-embed-image-v1")
                        .dimension(1024)
                        .modality("jpeg")
                        .format("binary")
                        .input(new byte[] {1, 2, 3})
                        .build();
        String second =
                ModelInvocationCacheKey.builder()
                        .provider("BEDROCK")
                        .model("amazon.titan-embed-image-v1")
                        .dimension(1024)
                        .modality("jpeg")
                        .format("binary")
                        .input(new byte[] {1, 2, 3})
                        .build();
        String different =
                ModelInvocationCacheKey.builder()
                        .provider("BEDROCK")
                        .model("amazon.titan-embed-image-v1")
                        .dimension(1024)
                        .modality("jpeg")
                        .format("binary")
                        .input(new byte[] {1, 2, 4})
                        .build();

        Assertions.assertEquals(first, second);
        Assertions.assertNotEquals(first, different);
    }

    private static String keyWith(
            String provider, String model, int dimension, String modality, String format) {
        return ModelInvocationCacheKey.builder()
                .provider(provider)
                .model(model)
                .dimension(dimension)
                .modality(modality)
                .format(format)
                .input("same chunk")
                .build();
    }
}
