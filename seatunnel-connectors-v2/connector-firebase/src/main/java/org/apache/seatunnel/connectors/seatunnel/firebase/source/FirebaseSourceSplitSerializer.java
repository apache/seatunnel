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

package org.apache.seatunnel.connectors.seatunnel.firebase.source;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FirebaseSourceSplitSerializer implements Serializer<FirebaseSourceSplit> {
    @Override
    public byte[] serialize(FirebaseSourceSplit split) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getPath());

            List<String> keys = split.getKeys();
            out.writeInt(keys.size());
            for (String key : keys) {
                out.writeUTF(key);
            }
            out.flush();
        }
        return baos.toByteArray();
    }

    @Override
    public FirebaseSourceSplit deserialize(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String splitId = in.readUTF();
            String path = in.readUTF();

            int size = in.readInt();
            List<String> keys = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                keys.add(in.readUTF());
            }
            return new FirebaseSourceSplit(splitId, path, keys);
        }
    }
}
