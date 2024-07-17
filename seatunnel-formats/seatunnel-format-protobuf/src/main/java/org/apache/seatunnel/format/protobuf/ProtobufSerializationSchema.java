/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.protobuf;

import com.google.protobuf.Descriptors;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ProtobufSerializationSchema implements SerializationSchema {

    private static final long serialVersionUID = 4438784443025715370L;

    private final RowToProtobufConverter converter;

    public ProtobufSerializationSchema(SeaTunnelRowType rowType,String protobufMessageName,String protobufSchema) {
        try {
            Descriptors.FileDescriptor[] fileDescriptors = CompileDescriptor.compileDescriptorTempFile(protobufSchema);
            this.converter = new RowToProtobufConverter(rowType,fileDescriptors,protobufMessageName);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        // TODO 返回序列化数据
        return  converter.convertRowToGenericRecord(element);
    }
}
