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

package org.apache.seatunnel.edge.agent.starter.parse;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.connector.config.EdgeInputOptions;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlConfig;
import org.apache.seatunnel.edge.agent.starter.yaml.AgentYamlConfig.FileInputDefinition;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class AgentConfigBridge {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE =
            new TypeReference<Map<String, Object>>() {};

    public static ReadonlyConfig agent(AgentYamlConfig.AgentSection agent) {
        if (agent == null) {
            return ReadonlyConfig.fromMap(new LinkedHashMap<>());
        }
        Map<String, Object> map = toMap(agent);
        stripNulls(map);
        return ReadonlyConfig.fromMap(map);
    }

    public static ReadonlyConfig input(AgentYamlConfig.ReaderDefinition def) {
        Objects.requireNonNull(def, "def");
        FileInputDefinition fin = def.toFileInputDefinition();
        Map<String, Object> map = toMap(fin);
        map.put(FileCollectOptions.ID.key(), def.getId());
        if (def.getType() != null && !def.getType().trim().isEmpty()) {
            map.put(EdgeInputOptions.TYPE.key(), def.getType().trim());
        }
        stripNulls(map);
        return ReadonlyConfig.fromMap(map);
    }

    public static ReadonlyConfig queue(AgentYamlConfig.QueueDefinition queue) {
        if (queue == null) {
            return ReadonlyConfig.fromMap(new LinkedHashMap<>());
        }
        Map<String, Object> map = toMap(queue);
        stripNulls(map);
        return ReadonlyConfig.fromMap(map);
    }

    public static ReadonlyConfig output(AgentYamlConfig.OutputDefinition output) {
        Objects.requireNonNull(output, "output");
        Map<String, Object> map = toMap(output);
        stripNulls(map);
        return ReadonlyConfig.fromMap(map);
    }

    public static ReadonlyConfig retry(AgentYamlConfig.RetryDefinition retry) {
        if (retry == null) {
            return ReadonlyConfig.fromMap(new LinkedHashMap<>());
        }
        Map<String, Object> map = toMap(retry);
        stripNulls(map);
        return ReadonlyConfig.fromMap(map);
    }

    private static Map<String, Object> toMap(Object value) {
        return MAPPER.convertValue(value, MAP_TYPE);
    }

    private static void stripNulls(Map<String, Object> map) {
        Iterator<Map.Entry<String, Object>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            Object value = entry.getValue();
            if (value == null) {
                iterator.remove();
            } else if (value instanceof Map) {
                Map<String, Object> nested = (Map<String, Object>) value;
                stripNulls(nested);
                if (nested.isEmpty()) {
                    iterator.remove();
                }
            }
        }
    }
}
