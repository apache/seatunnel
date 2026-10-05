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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.transform.nlpmodel.embedding.SrcField;
import org.apache.seatunnel.transform.nlpmodel.embedding.VectorFieldSpec;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.ModalityType;
import org.apache.seatunnel.transform.nlpmodel.embedding.multimodal.MultimodalFieldValue;
import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class DoubaoMultimodalModelTest {

    private DoubaoModel model;

    @BeforeEach
    void setUp() {
        this.model =
                new DoubaoModel(
                        "test-api-key",
                        "doubao-embedding-vision",
                        "https://ark.cn-beijing.volces.com/api/v3/embeddings",
                        1);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (model != null) {
            model.close();
        }
    }

    @Test
    void testMultimodalBodyWithText() {
        Map.Entry<String, Object> textFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("text_vector", "text_field");
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(textFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0), "Hello world")));

        ObjectNode result = model.multimodalBody(multimodalFieldValue);

        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("Hello world", inputNode.get("text").asText());
        Assertions.assertFalse(inputNode.has("image_url"));
        Assertions.assertFalse(inputNode.has("video_url"));
    }

    /**
     * { "model": "doubao-embedding-vision", "encoding_format": "float", "input": [ { "type":
     * "image_url", "image_url": { "url":
     * "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg" } } ] }
     */
    @Test
    void testMultimodalBodyWithImage() {
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");
        Map.Entry<String, Object> imageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", imageFieldConfig);

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(imageFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0),
                                        "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg")));

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
    }

    /**
     * { "model": "doubao-embedding-vision", "encoding_format": "float", "input": [ { "type":
     * "video_url", "video_url": { "url": "https://example.com/video.mp4" } } ] }
     */
    @Test
    void testMultimodalBodyWithVideo() {
        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "mP4");
        videoFieldConfig.put("format", "url");
        Map.Entry<String, Object> videoFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("video_vector", videoFieldConfig);

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(videoFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0),
                                        "https://example.com/video.mp4")));

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
    }

    /**
     * { "type": "image_url", "image_url": { "url":
     * f"data:image/<IMAGE_FORMAT>;base64,{base64_image}" } }
     */
    @Test
    void testMultimodalBodyWithBinaryImage() {
        Map<String, Object> binaryImageFieldConfig = new HashMap<>();
        binaryImageFieldConfig.put("field", "binary_image_field");
        binaryImageFieldConfig.put("modality", "png");
        binaryImageFieldConfig.put("format", "binary");
        Map.Entry<String, Object> binaryImageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>(
                        "binary_image_vector", binaryImageFieldConfig);

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(binaryImageFieldEntry);
        byte[] mockImageData = "mock-image-data".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0), mockImageData)));

        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(1, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("image_url"));
        Assertions.assertTrue(
                inputNode
                        .get("image_url")
                        .get("url")
                        .asText()
                        .endsWith(Base64.getEncoder().encodeToString(mockImageData)));
    }

    /**
     * { "model": "doubao-embedding-vision", "encoding_format": "float", "input": [ { "type":
     * "text", "text": "Hello world 1" }, { "type": "text", "text": "Hello world 2" } ] }
     */
    @Test
    void testMultimodalBodyWithSameModalityList() {
        Map.Entry<String, Object> vectorFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>(
                        "same_multimodal_vector", Arrays.asList("text_field_1", "text_field_2"));
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(vectorFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Arrays.asList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0), "Hello world 1"),
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(1),
                                        "Hello world 2")));

        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(2, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("Hello world 1", inputNode.get("text").asText());
        Assertions.assertFalse(inputNode.has("image_url"));
        Assertions.assertFalse(inputNode.has("video_url"));

        inputNode = (ObjectNode) result.get("input").get(1);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("Hello world 2", inputNode.get("text").asText());
        Assertions.assertFalse(inputNode.has("image_url"));
        Assertions.assertFalse(inputNode.has("video_url"));
    }

    /**
     * { "model": "doubao-embedding-vision", "encoding_format": "float", "input": [ { "type":
     * "text", "text": "Hello world" }, { "type": "image_url", "image_url": { "url":
     * "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg" } }, { "type":
     * "video_url", "video_url": { "url": "https://example.com/video.mp4" } } ] }
     */
    @Test
    void testMultimodalBodyWithDifferentModalityList() {
        Object textFieldConfig = "text_field";
        if (ThreadLocalRandom.current().nextBoolean()) {
            Map<String, Object> textFieldConfigMap = new HashMap<>();
            textFieldConfigMap.put("field", "text_field");
            textFieldConfigMap.put("modality", "text");
            textFieldConfigMap.put("format", "text");
            textFieldConfig = textFieldConfigMap;
        }
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");
        Map<String, Object> videoFieldConfig = new HashMap<>();
        videoFieldConfig.put("field", "video_field");
        videoFieldConfig.put("modality", "mp4");
        videoFieldConfig.put("format", "url");
        Map.Entry<String, Object> vectorFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>(
                        "different_multimodal_vector",
                        Arrays.asList(textFieldConfig, imageFieldConfig, videoFieldConfig));

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(vectorFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Arrays.asList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0), "Hello world"),
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(1),
                                        "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg"),
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(2),
                                        "https://example.com/video.mp4")));

        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        Assertions.assertEquals("doubao-embedding-vision", result.get("model").asText());
        Assertions.assertEquals("float", result.get("encoding_format").asText());
        Assertions.assertEquals(3, result.get("input").size());

        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("Hello world", inputNode.get("text").asText());
        Assertions.assertFalse(inputNode.has("image_url"));
        Assertions.assertFalse(inputNode.has("video_url"));

        inputNode = (ObjectNode) result.get("input").get(1);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("image_url"));
        Assertions.assertEquals(
                "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg",
                inputNode.get("image_url").get("url").asText());
        Assertions.assertFalse(inputNode.has("text"));
        Assertions.assertFalse(inputNode.has("video_url"));

        inputNode = (ObjectNode) result.get("input").get(2);
        Assertions.assertEquals("video_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("video_url"));
        Assertions.assertEquals(
                "https://example.com/video.mp4", inputNode.get("video_url").get("url").asText());
        Assertions.assertFalse(inputNode.has("text"));
        Assertions.assertFalse(inputNode.has("image_url"));
    }

    @Test
    void testParseMultimodalVectorResponseSuccess() throws IOException {
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
    }

    @Test
    void testUrlAutoDetectModality() {
        // Explicitly configured modality (png) must be respected and NOT overridden by the runtime
        // value suffix (.jpg).
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "image_field");
        fieldConfig.put("format", "url");
        fieldConfig.put("modality", "png");
        Map.Entry<String, Object> imageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig);

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(imageFieldEntry);
        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0),
                                        "https://example.com/photo.jpg")));

        Assertions.assertEquals(
                ModalityType.PNG,
                multimodalFieldValue.getSrcFields().get(0).getFieldSpec().getModalityType());
        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());

        // No modality configured -> auto-detect from the value suffix (.jpg -> jpeg).
        Map<String, Object> fieldConfig2 = new HashMap<>();
        fieldConfig2.put("field", "image_field");
        fieldConfig2.put("format", "url");
        imageFieldEntry = new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig2);
        vectorFieldSpec = new VectorFieldSpec(imageFieldEntry);
        multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0),
                                        "https://example.com/photo.jpg")));

        Assertions.assertEquals(
                ModalityType.JPEG,
                multimodalFieldValue.getSrcFields().get(0).getFieldSpec().getModalityType());
        result = model.multimodalBody(multimodalFieldValue);
        inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
    }

    @Test
    void testExplicitModalityNotOverriddenBySuffix() {
        // Regression: modality = png + runtime value photo.jpg should stay png.
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "image_field");
        fieldConfig.put("format", "url");
        fieldConfig.put("modality", "png");
        Map.Entry<String, Object> imageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig);

        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(imageFieldEntry);
        SrcField srcField =
                new SrcField(
                        vectorFieldSpec.getSrcFieldSpecs().get(0), "https://example.com/photo.jpg");

        Assertions.assertEquals(ModalityType.PNG, srcField.getFieldSpec().getModalityType());
        Assertions.assertTrue(srcField.getFieldSpec().isModalityTypeExplicitlyConfigured());
    }

    @Test
    void testMixedConfigTextFieldWithImageSuffixStaysText() {
        // Regression: in a mixed multimodal job, a plain text field whose value happens to end with
        // a known image suffix (foo.jpg) must NOT be misclassified as an image.
        Map<String, Object> imageFieldConfig = new HashMap<>();
        imageFieldConfig.put("field", "image_field");
        imageFieldConfig.put("modality", "jpeg");
        imageFieldConfig.put("format", "url");

        Map.Entry<String, Object> entry =
                new java.util.AbstractMap.SimpleEntry<>(
                        "mix_vector", Arrays.asList("text_field", imageFieldConfig));
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);

        // first src field is the plain text field, with a value that ends with .jpg
        SrcField textSrcField =
                new SrcField(vectorFieldSpec.getSrcFieldSpecs().get(0), "this is foo.jpg");
        Assertions.assertEquals(ModalityType.TEXT, textSrcField.getFieldSpec().getModalityType());
        Assertions.assertFalse(textSrcField.getFieldSpec().isModalityTypeExplicitlyConfigured());

        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(Collections.singletonList(textSrcField));
        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("text", inputNode.get("type").asText());
        Assertions.assertEquals("this is foo.jpg", inputNode.get("text").asText());
    }

    @Test
    void testNoModalityPlainTextValueStaysText() {
        // No modality configured and value has no recognizable suffix -> stays TEXT.
        Map.Entry<String, Object> entry =
                new java.util.AbstractMap.SimpleEntry<>("text_vector", "hello world");
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(entry);
        SrcField srcField = new SrcField(vectorFieldSpec.getSrcFieldSpecs().get(0), "hello world");

        Assertions.assertEquals(ModalityType.TEXT, srcField.getFieldSpec().getModalityType());
        Assertions.assertFalse(srcField.getFieldSpec().isModalityTypeExplicitlyConfigured());
    }

    @Test
    void testBinaryAutoDetectModality() {
        Map<String, Object> fieldConfig = new HashMap<>();
        fieldConfig.put("field", "image_field");
        fieldConfig.put("format", "binary");
        fieldConfig.put("modality", "png");
        Map.Entry<String, Object> imageFieldEntry =
                new java.util.AbstractMap.SimpleEntry<>("image_vector", fieldConfig);
        VectorFieldSpec vectorFieldSpec = new VectorFieldSpec(imageFieldEntry);

        MultimodalFieldValue multimodalFieldValue =
                new MultimodalFieldValue(
                        Collections.singletonList(
                                new SrcField(
                                        vectorFieldSpec.getSrcFieldSpecs().get(0),
                                        "https://example.com/photo.jpg")));

        Assertions.assertEquals(
                ModalityType.PNG,
                multimodalFieldValue.getSrcFields().get(0).getFieldSpec().getModalityType());
        ObjectNode result = model.multimodalBody(multimodalFieldValue);
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
    }
}
