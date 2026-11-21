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

package org.apache.seatunnel.connectors.doris.sink.committer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class DorisCommitInfoSerializerTest {

    private final DorisCommitInfoSerializer serializer = new DorisCommitInfoSerializer();

    @Test
    void testSerializeAndDeserializeLabel() throws IOException {
        DorisCommitInfo commitInfo = new DorisCommitInfo("fe1:8030", "test_db", 12L, "job_label");

        DorisCommitInfo restored = serializer.deserialize(serializer.serialize(commitInfo));

        Assertions.assertEquals(commitInfo, restored);
    }

    @Test
    void testDeserializeLegacyCommitInfoWithoutLabel() throws IOException {
        DorisCommitInfo restored = serializer.deserialize(legacySerializedCommitInfo());

        Assertions.assertEquals("fe1:8030", restored.getHostPort());
        Assertions.assertEquals("test_db", restored.getDb());
        Assertions.assertEquals(12L, restored.getTxbID());
        Assertions.assertNull(restored.getLabel());
    }

    private byte[] legacySerializedCommitInfo() throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeUTF("fe1:8030");
            output.writeUTF("test_db");
            output.writeLong(12L);
            output.flush();
            return bytes.toByteArray();
        }
    }
}
