// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.seatunnel.connector.selectdb.sink.committer;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** define how to serialize SelectDBCommitInfo. */
public class SelectDBCommitInfoSerializer implements Serializer<SelectDBCommitInfo> {

    @Override
    public byte[] serialize(SelectDBCommitInfo obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(obj.getHostPort());
            out.writeUTF(obj.getClusterName());
            out.writeUTF(obj.getCopySQL());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public SelectDBCommitInfo deserialize(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final String hostPort = in.readUTF();
            final String clusterName = in.readUTF();
            final String copySQL = in.readUTF();
            return new SelectDBCommitInfo(hostPort, clusterName, copySQL);
        }
    }
}
