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

package org.apache.seatunnel.edge.agent.starter.wal.sqlite;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class MetadataSerde {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> METADATA_TYPE =
            new TypeReference<Map<String, String>>() {};

    public static byte[] serialize(Map<String, String> metadata) throws Exception {
        Map<String, String> safeMetadata =
                metadata == null ? new HashMap<>() : new HashMap<>(metadata);
        return OBJECT_MAPPER.writeValueAsBytes(safeMetadata);
    }

    public static Map<String, String> deserialize(byte[] metadataBytes) throws Exception {
        if (metadataBytes == null || metadataBytes.length == 0) {
            return new HashMap<>();
        }
        Map<String, String> metadata = OBJECT_MAPPER.readValue(metadataBytes, METADATA_TYPE);
        return metadata == null ? new HashMap<>() : new HashMap<>(metadata);
    }
}
