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

package org.apache.seatunnel.engine.core.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.DataCodec;
import com.hazelcast.internal.serialization.Data;

import static com.hazelcast.client.impl.protocol.ClientMessage.PARTITION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.RESPONSE_BACKUP_ACKS_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

/*
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * to seatunnel-engine/seatunnel-engine-core/src/main/resources/client-protocol-definition/SeaTunnelEngine.yaml
 */

/** */
@Generated("b7fc02107a714918a542f42f1c602b7f")
public final class SeaTunnelUploadConnectorJarCodec {
    // hex: 0xDE0D00
    public static final int REQUEST_MESSAGE_TYPE = 14552320;
    // hex: 0xDE0D01
    public static final int RESPONSE_MESSAGE_TYPE = 14552321;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET =
            PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE =
            REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE =
            RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private SeaTunnelUploadConnectorJarCodec() {}

    public static class RequestParameters {

        public long jobId;

        public com.hazelcast.internal.serialization.Data connectorJar;
    }

    public static ClientMessage encodeRequest(
            long jobId, com.hazelcast.internal.serialization.Data connectorJar) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("SeaTunnel.UploadConnectorJar");
        ClientMessage.Frame initialFrame =
                new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        clientMessage.add(initialFrame);
        DataCodec.encode(clientMessage, connectorJar);
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        request.connectorJar = DataCodec.decode(iterator);
        return request;
    }

    public static ClientMessage encodeResponse(Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame =
                new ClientMessage.Frame(
                        new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, response);
        return clientMessage;
    }

    public static Data decodeResponse(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        // empty initial frame
        iterator.next();
        return DataCodec.decode(iterator);
    }
}
