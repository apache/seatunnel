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

package org.apache.seatunnel.transform.llm;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.format.json.RowToJsonConverters;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.custom.CustomModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.kimiai.KimiAIModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.microsoft.MicrosoftModel;
import org.apache.seatunnel.transform.nlpmodel.llm.remote.openai.OpenAIModel;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LLMRequestJsonTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testOpenAIRequestJson() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        OpenAIModel model =
                new OpenAIModel(
                        rowType,
                        SqlType.STRING,
                        null,
                        "Determine whether someone is Chinese or American by their name",
                        "gpt-3.5-turbo",
                        "sk-xxx",
                        "https://api.openai.com/v1/chat/completions");
        ObjectNode node =
                model.createJsonNodeFromData(
                        "Determine whether someone is Chinese or American by their name",
                        "{\"id\":1, \"name\":\"John\"}");
        Assertions.assertEquals(
                "{\"model\":\"gpt-3.5-turbo\",\"messages\":[{\"role\":\"system\",\"content\":\"Determine whether someone is Chinese or American by their name\"},{\"role\":\"user\",\"content\":\"{\\\"id\\\":1, \\\"name\\\":\\\"John\\\"}\"}]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testOpenAIProjectionRequestJson() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "city"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });
        OpenAIModel model =
                new OpenAIModel(
                        rowType,
                        SqlType.STRING,
                        Lists.newArrayList("name", "city"),
                        "Determine whether someone is Chinese or American by their name",
                        "gpt-3.5-turbo",
                        "sk-xxx",
                        "https://api.openai.com/v1/chat/completions");

        SeaTunnelRow row = new SeaTunnelRow(rowType.getFieldTypes().length);
        row.setField(0, 1);
        row.setField(1, "John");
        row.setField(2, "New York");
        ObjectNode rowNode = OBJECT_MAPPER.createObjectNode();
        RowToJsonConverters.RowToJsonConverter rowToJsonConverter = model.getRowToJsonConverter();
        rowToJsonConverter.convert(OBJECT_MAPPER, rowNode, model.createProjectionSeaTunnelRow(row));
        ObjectNode node =
                model.createJsonNodeFromData(
                        "Determine whether someone is Chinese or American by their name",
                        OBJECT_MAPPER.writeValueAsString(rowNode));
        Assertions.assertEquals(
                "{\"model\":\"gpt-3.5-turbo\",\"messages\":[{\"role\":\"system\",\"content\":\"Determine whether someone is Chinese or American by their name\"},{\"role\":\"user\",\"content\":\"{\\\"name\\\":\\\"John\\\",\\\"city\\\":\\\"New York\\\"}\"}]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testKimiAIRequestJson() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        KimiAIModel model =
                new KimiAIModel(
                        rowType,
                        SqlType.STRING,
                        null,
                        "Determine whether someone is Chinese or American by their name",
                        "moonshot-v1-8k",
                        "sk-xxx",
                        "https://api.moonshot.cn/v1/chat/completions");
        ObjectNode node =
                model.createJsonNodeFromData(
                        "Determine whether someone is Chinese or American by their name",
                        "{\"id\":1, \"name\":\"John\"}");
        Assertions.assertEquals(
                "{\"model\":\"moonshot-v1-8k\",\"messages\":[{\"role\":\"system\",\"content\":\"Determine whether someone is Chinese or American by their name\"},{\"role\":\"user\",\"content\":\"{\\\"id\\\":1, \\\"name\\\":\\\"John\\\"}\"}]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testMicrosoftRequestJson() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        MicrosoftModel model =
                new MicrosoftModel(
                        rowType,
                        SqlType.STRING,
                        null,
                        "Determine whether someone is Chinese or American by their name",
                        "gpt-35-turbo",
                        "sk-xxx",
                        "https://api.moonshot.cn/openai/deployments/${model}/chat/completions?api-version=2024-02-01");
        Field apiPathField = model.getClass().getDeclaredField("apiPath");
        apiPathField.setAccessible(true);
        String apiPath = (String) apiPathField.get(model);
        Assertions.assertEquals(
                "https://api.moonshot.cn/openai/deployments/gpt-35-turbo/chat/completions?api-version=2024-02-01",
                apiPath);

        ObjectNode node =
                model.createJsonNodeFromData(
                        "Determine whether someone is Chinese or American by their name",
                        "{\"id\":1, \"name\":\"John\"}");
        Assertions.assertEquals(
                "{\"messages\":[{\"role\":\"system\",\"content\":\"Determine whether someone is Chinese or American by their name\"},{\"role\":\"user\",\"content\":\"{\\\"id\\\":1, \\\"name\\\":\\\"John\\\"}\"}]}",
                OBJECT_MAPPER.writeValueAsString(node));
        model.close();
    }

    @Test
    void testCustomRequestJson() throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        Map<String, String> header = new HashMap<>();
        header.put("Content-Type", "application/json");

        List<Map<String, String>> messagesList = new ArrayList<>();

        Map<String, String> systemMessage = new HashMap<>();
        systemMessage.put("role", "system");
        systemMessage.put("content", "${prompt}");
        messagesList.add(systemMessage);

        Map<String, String> userMessage = new HashMap<>();
        userMessage.put("role", "user");
        userMessage.put("content", "${input}");
        messagesList.add(userMessage);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("model", "${model}");
        resultMap.put("messages", messagesList);

        CustomModel model =
                new CustomModel(
                        rowType,
                        SqlType.STRING,
                        null,
                        "Determine whether someone is Chinese or American by their name",
                        "custom-model",
                        "https://api.custom.com/v1/chat/completions",
                        header,
                        resultMap,
                        "{\"model\":\"${model}\",\"messages\":[{\"role\":\"system\",\"content\":\"${prompt}\"},{\"role\":\"user\",\"content\":\"${data}\"}]}");
        ObjectNode node =
                model.createJsonNodeFromData(
                        "Determine whether someone is Chinese or American by their name",
                        "{\"id\":1, \"name\":\"John\"}");
        Assertions.assertEquals(
                "{\"messages\":[{\"role\":\"system\",\"content\":\"Determine whether someone is Chinese or American by their name\"},{\"role\":\"user\",\"content\":\"{\\\"id\\\":1, \\\"name\\\":\\\"John\\\"}\"}],\"model\":\"custom-model\"}",
                OBJECT_MAPPER.writeValueAsString(node));
    }

    @Test
    void testCustomOllamaRequestJson() throws IOException {

        MockWebServer mockWebServer = new MockWebServer();
        mockWebServer.start(11434);
        String jsonResponse =
                "{\n"
                        + "    \"model\": \"qwen:7b\",\n"
                        + "    \"created_at\": \"2025-02-07T01:22:46.589856Z\",\n"
                        + "    \"message\": {\n"
                        + "        \"role\": \"assistant\",\n"
                        + "        \"content\": \"Based on the information provided in the JSON object, \\\"John\\\" does not inherently indicate if the person is Chinese or American. The name \\\"John\\\" is commonly used across many cultures. To determine a person's nationality based solely on their name, more context would be needed.\"\n"
                        + "    },\n"
                        + "    \"done_reason\": \"stop\",\n"
                        + "    \"done\": true,\n"
                        + "    \"total_duration\": 14435322300,\n"
                        + "    \"load_duration\": 28998200,\n"
                        + "    \"prompt_eval_count\": 34,\n"
                        + "    \"prompt_eval_duration\": 302000000,\n"
                        + "    \"eval_count\": 56,\n"
                        + "    \"eval_duration\": 14102000000\n"
                        + "}";

        mockWebServer.enqueue(
                new MockResponse()
                        .setBody(jsonResponse)
                        .addHeader("Content-Type", "application/json"));

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

        Map<String, String> header = new HashMap<>();
        header.put("Content-Type", "application/json");

        List<Map<String, String>> messagesList = new ArrayList<>();

        Map<String, String> systemMessage = new HashMap<>();
        systemMessage.put("role", "system");
        systemMessage.put("content", "${prompt}");
        messagesList.add(systemMessage);

        Map<String, String> userMessage = new HashMap<>();
        userMessage.put("role", "user");
        userMessage.put("content", "${input}");
        messagesList.add(userMessage);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("model", "${model}");
        resultMap.put("stream", false);
        resultMap.put("messages", messagesList);

        CustomModel model =
                new CustomModel(
                        rowType,
                        SqlType.STRING,
                        null,
                        "Determine whether someone is Chinese or American by their name",
                        "qwen:7b",
                        "http://localhost:11434/api/chat",
                        header,
                        resultMap,
                        "$.message.content");

        SeaTunnelRow row = new SeaTunnelRow(rowType.getFieldTypes().length);
        row.setField(0, 1);
        row.setField(1, "John");
        List<String> successResult = model.inference(Collections.singletonList(row));
        Assertions.assertFalse(successResult.isEmpty());
    }
}
