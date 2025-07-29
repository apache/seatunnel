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

package org.apache.seatunnel.connectors.seatunnel.redis.util;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonKeyValueMerger implements KeyValueMerger {
    private final RedisParameters redisParameters;

    public JsonKeyValueMerger(RedisParameters redisParameters) {
        this.redisParameters = redisParameters;
    }

    @Override
    public String parseWithKey(String key, String value) {
        ObjectNode objectNode = getObjectNode(key, value);
        return objectNode.toString();
    }

    private ObjectNode getObjectNode(String key, String value) {
        JsonNode node = JsonUtils.toJsonNode(value);
        if (node.isTextual()) {
            String text = node.textValue();
            if (looksLikeJson(text)) {
                try {
                    node = JsonUtils.parseObject(text);
                } catch (Exception e) {
                    log.debug(
                            "Looks like JSON, but failed to parse JSON object from text value: {}",
                            node.textValue());
                }
            }
        }

        ObjectNode objectNode;
        if (node instanceof ObjectNode) {
            objectNode = (ObjectNode) node;
        } else {
            objectNode = JsonUtils.createObjectNode();
            setValueInNode(objectNode, node);
        }
        objectNode.put(redisParameters.getKeyFieldName(), key);
        return objectNode;
    }

    public static boolean looksLikeJson(String text) {
        return text != null
                && ((text.startsWith("{") && text.endsWith("}"))
                        || (text.startsWith("[") && text.endsWith("]")));
    }

    private void setValueInNode(ObjectNode objectNode, JsonNode node) {
        String singleFieldName = redisParameters.getSingleFieldName();
        if (singleFieldName != null) {
            objectNode.set(singleFieldName, node);
        } else {
            throw CommonError.illegalArgument(
                    "singleFieldName is null",
                    "You must specify 'single_field_name' when using a single value with key-enabled schema.");
        }
    }
}
