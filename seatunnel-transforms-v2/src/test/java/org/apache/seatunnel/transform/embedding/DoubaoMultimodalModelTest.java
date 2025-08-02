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

import org.apache.seatunnel.transform.nlpmodel.embedding.remote.doubao.DoubaoModel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
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

        Map<String, Object> textField = new HashMap<>();
        textField.put("text", "Hello world");
        ObjectNode result = model.multimodalBody(textField);

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

        Map<String, Object> imageField = new HashMap<>();
        imageField.put(
                "image",
                "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg");

        ObjectNode result = model.multimodalBody(imageField);

        Assertions.assertTrue(result.get("input").isArray());
        Assertions.assertEquals(1, result.get("input").size());
        ObjectNode inputNode = (ObjectNode) result.get("input").get(0);
        Assertions.assertEquals("image_url", inputNode.get("type").asText());
        Assertions.assertTrue(inputNode.has("image_url"));
        Assertions.assertEquals(
                "https://ck-test.tos-cn-beijing.volces.com/vlm/pexels-photo-27163466.jpeg",
                inputNode.get("image_url").get("url").asText());
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

        Map<String, Object> videoField = new HashMap<>();
        videoField.put("video", "https://example.com/video.mp4");

        ObjectNode result = model.multimodalBody(videoField);

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
}
