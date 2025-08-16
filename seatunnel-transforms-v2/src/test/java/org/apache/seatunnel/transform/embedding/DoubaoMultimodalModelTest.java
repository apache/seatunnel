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

package org.apache.seatunnel.transform.embedding;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.transform.nlpmodel.embedding.FieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalFieldValue;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DoubaoMultimodalModelTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testMultimodalBodyWithText() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map.Entry<String, Object> textFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("text_vector", "Hello world");
        FieldSpec fieldSpec = new FieldSpec(textFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(fieldSpec, "Hello world");

        ObjectNode result = model.multimodalBody(multimodalFieldValue);

        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("Hello world", inputNode.get("text").asText());
        Assertions.assertFalse(inputNode.has("image_url"));
        Assertions.assertFalse(inputNode.has("video_url"));

        model.close();
    }

    /**
     * { "model" : "doubao-embedding-vision", "encoding_format" : "float", "input" : [ { "type" :
     * "image_url", "image_url" : { "url" :
     * "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg" } }] }
     */
    @Test
    void testMultimodalBodyWithImage() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");

        Map.Entry<String, Object> imageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", imageFieldConfig);
        FieldSpec fieldSpec = new FieldSpec(imageFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        fieldSpec,
                        "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg");

        ObjectNode result = model.multimodalBody(multimodalFieldValue);

        // Verify the request structure
        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertTrue(result.get("input").isArray());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("image_url"));
        Assertions.assertEquals(
                "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg",
                inputNode.get("image_url").get("url").asText());
        Assertions.assertFalse(inputNode.has("text"));
        Assertions.assertFalse(inputNode.has("video_url"));

        model.close();
    }

    /**
     * { "model" : "doubao-embedding-vision", "encoding_format" : "float", "input" : [ { "type" :
     * "video_url", "video_url" : { "url" : "https://example.com/video.mp4" } } ] }
     */
    @Test
    void testMultimodalBodyWithVideo() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "mP4");
        videoFieldConfig.put("format", "url");

        Map.Entry<String, Object> videoFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("video_vector", videoFieldConfig);
        FieldSpec fieldSpec = new FieldSpec(videoFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(fieldSpec, "https://example.com/video.mp4");

        ObjectNode result = model.multimodalBody(multimodalFieldValue);

        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("video_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("video_url"));
        Assertions.assertEquals(
                "https://example.com/video.mp4", inputNode.get("video_url").get("url").asText());
        Assertions.assertFalse(inputNode.has("text"));
        Assertions.assertFalse(inputNode.has("image_url"));

        model.close();
    }

    /**
     * { "type": "image_url", "image_url": { "url":
     * f"data:image/<IMAGE_FORMAT>;base64,{base64_image}" } }
     */
    @Test
    void testMultimodalBodyWithBinaryImage() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision-250615",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map<String, Object> binaryImageFieldConfig = new HashMap<>();
        binaryImageFieldConfig.put("field", "binary_image_field");
        binaryImageFieldConfig.put("modality", "png");
        binaryImageFieldConfig.put("format", "binary");

        Map.Entry<String, Object> binaryImageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>(
                        "binary_image_vector", binaryImageFieldConfig);
        FieldSpec fieldSpec = new FieldSpec(binaryImageFieldEntry);

        byte[] mockImageData = "mock-image-data".getBytes();
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(fieldSpec, mockImageData);

        ObjectNode result = model.multimodalBody(multimodalFieldValue);

        Assertions.assertEquals("doubao-embedding-vision-250615", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("image_url"));

        model.close();
    }

    @Test
    void testParseMultimodalVectorResponseSuccess() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        String successResponse =
                "{\n"
                        + "  \"created\": 1743575029,\n"
                        + "  \"data\": {\n"
                        + "    \"embedding\": [\n"
                        + "      -0.123046875, -0.35546875, -0.318359375, 0.255859375, 1.5\n"
                        + "    ],\n"
                        + "    \"object\": \"embedding\"\n"
                        + "  },\n"
                        + "  \"id\": \"021743575029461acbe49a31755bec77b2f09448eb15fa9a88e47\",\n"
                        + "  \"model\": \"doubao-embedding-vision-250615\",\n"
                        + "  \"object\": \"list\",\n"
                        + "  \"usage\": {\n"
                        + "    \"prompt_tokens\": 13987,\n"
                        + "    \"prompt_tokens_details\": {\n"
                        + "      \"image_tokens\": 13800,\n"
                        + "      \"text_tokens\": 187\n"
                        + "    },\n"
                        + "    \"total_tokens\": 13987\n"
                        + "  }\n"
                        + "}";

        List<Float> result = model.parseMultimodalVectorResponse(successResponse);

        // Verify the parsed vector
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.size());
        Assertions.assertEquals(-0.123046875f, result.get(0), 0.0001f);
        Assertions.assertEquals(-0.35546875f, result.get(1), 0.0001f);
        Assertions.assertEquals(-0.318359375f, result.get(2), 0.0001f);
        Assertions.assertEquals(0.255859375f, result.get(3), 0.0001f);
        Assertions.assertEquals(1.5f, result.get(4), 0.0001f);

        model.close();
    }

    @Test
    void testUrlAutoDetectModality() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "image_field");
        fieldConfig.put("format", "url");
        fieldConfig.put("modality", "png");
        Map.Entry<String, Object> fieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig);
        FieldSpec fieldSpec = new FieldSpec(fieldEntry);

        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(fieldSpec, "https://example.com/photo.jpg");

        Assertions.assertEquals(
                ModalityType.JPEG, multimodalFieldValue.getFieldSpec().getModalityType());
        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());

        Map<String, Object> fieldConfig2 = new HashMap<>();
        fieldConfig2.put("field", "image_field");
        fieldConfig2.put("format", "url");
        fieldEntry = new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig2);
        fieldSpec = new FieldSpec(fieldEntry);

        multimodalFieldValue = new MultimodalFieldValue(fieldSpec, "https://example.com/photo.jpg");

        Assertions.assertEquals(
                ModalityType.JPEG, multimodalFieldValue.getFieldSpec().getModalityType());
        result = model.multimodalBody(multimodalFieldValue);
        inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());

        model.close();
    }

    @Test
    void testBinaryAutoDetectModality() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);

        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "image_field");
        fieldConfig.put("format", "binary");
        fieldConfig.put("modality", "png");
        Map.Entry<String, Object> fieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig);
        FieldSpec fieldSpec = new FieldSpec(fieldEntry);

        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(fieldSpec, "https://example.com/photo.jpg");

        Assertions.assertEquals(
                ModalityType.PNG, multimodalFieldValue.getFieldSpec().getModalityType());
        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());

        model.close();
    }
}
