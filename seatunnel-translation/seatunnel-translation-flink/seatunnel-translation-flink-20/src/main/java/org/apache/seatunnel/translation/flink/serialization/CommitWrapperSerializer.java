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

package org.apache.seatunnel.translation.flink.serialization;

import org.apache.seatunnel.translation.flink.sink.CommitWrapper;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * The serializer of {@link CommitWrapper}, which is used to serialize and deserialize the commit
 * message wrapper.
 *
 * @param <CommT> The generic type of commit message
 */
public class CommitWrapperSerializer<CommT>
        implements SimpleVersionedSerializer<CommitWrapper<CommT>> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CommitWrapper<CommT> obj) throws IOException {
        return InstantiationUtil.serializeObject(obj.getCommit());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CommitWrapper<CommT> deserialize(int version, byte[] serialized) throws IOException {
        try {
            CommT commit =
                    (CommT)
                            InstantiationUtil.deserializeObject(
                                    serialized, getClass().getClassLoader());
            return new CommitWrapper<>(commit);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize commit wrapper", e);
        }
    }
}
