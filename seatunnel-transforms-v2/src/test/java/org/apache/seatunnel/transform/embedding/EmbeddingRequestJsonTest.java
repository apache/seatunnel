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

import org.apache.seatunnel.transform.embadding.processor.doubao.DoubaoModel;
import org.apache.seatunnel.transform.embadding.processor.openai.OpenAIModel;
import org.apache.seatunnel.transform.embadding.processor.qianfan.QianfanModel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class EmbeddingRequestJsonTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testOpenAIRequestJson() throws IOException {
        OpenAIModel model =
                new OpenAIModel(
                        "apikey", "modelName", "https://api.openai.com/v1/chat/completions", 1);
        ObjectNode node =
                model.createJsonNodeFromData(
                        new Object[] {
                            "Determine whether someone is Chinese or American by their name"
                        });
        Assertions.assertEquals(
                "{\"model\":\"modelName\",\"input\":\"Determine whether someone is Chinese or American by their name\"}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testDoubaoRequestJson() throws IOException {
        DoubaoModel model =
                new DoubaoModel(
                        "apikey", "modelName", "https://api.doubao.io/v1/chat/completions", 1);
        ObjectNode node =
                model.createJsonNodeFromData(
                        new Object[] {
                            "Determine whether someone is Chinese or American by their name"
                        });
        Assertions.assertEquals(
                "{\"model\":\"modelName\",\"input\":[\"Determine whether someone is Chinese or American by their name\"]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testQianfanRequestJson() throws IOException {
        QianfanModel model =
                new QianfanModel(
                        "apikey",
                        "secretKey",
                        "modelName",
                        "https://api.qianfan.io/v1/chat/completions",
                        1,
                        "xxxx",
                        "xxxxxxx");
        ObjectNode node =
                model.createJsonNodeFromData(
                        new Object[] {
                            "Determine whether someone is Chinese or American by their name"
                        });
        Assertions.assertEquals(
                "{\"input\":[\"Determine whether someone is Chinese or American by their name\"]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }
}
