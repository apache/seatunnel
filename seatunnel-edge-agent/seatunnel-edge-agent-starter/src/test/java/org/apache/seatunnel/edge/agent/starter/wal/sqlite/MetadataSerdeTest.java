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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MetadataSerdeTest {

    @Test
    void serializeUsesJsonAndRoundTripsStringMap() throws Exception {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("path", "/var/log/app.log");
        metadata.put("line", "42");

        byte[] serialized = MetadataSerde.serialize(metadata);

        String json = new String(serialized, StandardCharsets.UTF_8);
        Assertions.assertTrue(json.startsWith("{"));
        Assertions.assertEquals(metadata, MetadataSerde.deserialize(serialized));
    }

    @Test
    void emptyMetadataBytesReturnEmptyMap() throws Exception {
        Assertions.assertTrue(MetadataSerde.deserialize(null).isEmpty());
        Assertions.assertTrue(MetadataSerde.deserialize(new byte[0]).isEmpty());
    }
}
