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

package org.apache.seatunnel.transform.pivot;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Serializer for PivotGroupState.
 *
 * <p>This serializer uses Java serialization for simplicity. In production, you might want to use a
 * more efficient serialization framework like Kryo or Protobuf.
 */
public class PivotStateSerializer implements Serializer<PivotGroupState> {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(PivotGroupState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(state);
            oos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PivotGroupState deserialize(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (PivotGroupState) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize PivotGroupState", e);
        }
    }
}
