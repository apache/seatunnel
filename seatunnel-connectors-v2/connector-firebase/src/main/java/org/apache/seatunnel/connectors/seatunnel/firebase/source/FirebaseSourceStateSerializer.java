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
import java.util.HashSet;
import java.util.Set;

public class FirebaseSourceStateSerializer implements Serializer<FirebaseSourceState> {

    private final FirebaseSourceSplitSerializer splitSerializer =
            new FirebaseSourceSplitSerializer();

    @Override
    public byte[] serialize(FirebaseSourceState state) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            // 1. Serialize Pending Splits
            Set<FirebaseSourceSplit> pendingSplits = state.getPendingSplits();
            out.writeInt(pendingSplits.size());
            for (FirebaseSourceSplit split : pendingSplits) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }

            // 2. Serialize Assigned Split IDs
            Set<String> assignedSplitIds = state.getAssignedSplitIds();
            out.writeInt(assignedSplitIds.size());
            for (String splitId : assignedSplitIds) {
                out.writeUTF(splitId);
            }

            out.flush();
        }
        return baos.toByteArray();
    }

    @Override
    public FirebaseSourceState deserialize(byte[] serialized) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            // 1. Deserialize Pending Splits
            int pendingSize = in.readInt();
            Set<FirebaseSourceSplit> pendingSplits = new HashSet<>(pendingSize);
            for (int i = 0; i < pendingSize; i++) {
                int splitBytesLen = in.readInt();
                byte[] splitBytes = new byte[splitBytesLen];
                in.readFully(splitBytes);
                pendingSplits.add(splitSerializer.deserialize(splitBytes));
            }

            // 2. Deserialize Assigned Split IDs
            int assignedSize = in.readInt();
            Set<String> assignedSplitIds = new HashSet<>(assignedSize);
            for (int i = 0; i < assignedSize; i++) {
                assignedSplitIds.add(in.readUTF());
            }

            return new FirebaseSourceState(pendingSplits, assignedSplitIds);
        }
    }
}
