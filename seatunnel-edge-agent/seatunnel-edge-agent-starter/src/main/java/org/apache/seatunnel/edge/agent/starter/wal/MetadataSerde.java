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

package org.apache.seatunnel.edge.agent.starter.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class MetadataSerde {

    public static byte[] serialize(Map<String, String> metadata) throws Exception {
        Map<String, String> safeMetadata =
                metadata == null ? new HashMap<>() : new HashMap<>(metadata);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            objectOutputStream.writeObject(safeMetadata);
            objectOutputStream.flush();
            return outputStream.toByteArray();
        }
    }

    public static Map<String, String> deserialize(byte[] metadataBytes) throws Exception {
        if (metadataBytes == null || metadataBytes.length == 0) {
            return new HashMap<>();
        }
        try (ObjectInputStream objectInputStream =
                new ObjectInputStream(new ByteArrayInputStream(metadataBytes))) {
            Object metadata = objectInputStream.readObject();
            if (metadata instanceof Map) {
                return (Map<String, String>) metadata;
            }
        }
        return new HashMap<>();
    }
}
