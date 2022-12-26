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

import static com.hazelcast.client.impl.protocol.ClientMessage.ForwardFrameIterator;
import static com.hazelcast.client.impl.protocol.ClientMessage.Frame;
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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;

/*
 * definitions on the https://github.com/hazelcast/hazelcast-client-protocol
 * to seatunnel-engine/seatunnel-engine-core/src/main/resources/client-protocol-definition/SeaTunnelEngine.yaml
 */

@Generated("b8660c8a07cf0fd33e4191f33a26b46e")
public final class SeaTunnelCancelJobCodec {
    //hex: 0xDE0400
    public static final int REQUEST_MESSAGE_TYPE = 14550016;
    //hex: 0xDE0401
    public static final int RESPONSE_MESSAGE_TYPE = 14550017;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private SeaTunnelCancelJobCodec() {
    }

    public static ClientMessage encodeRequest(long jobId) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("SeaTunnel.CancelJob");
        Frame initialFrame = new Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static long decodeRequest(ClientMessage clientMessage) {
        ForwardFrameIterator iterator = clientMessage.frameIterator();
        Frame initialFrame = iterator.next();
        return decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
    }

    public static ClientMessage encodeResponse() {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        Frame initialFrame = new Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        return clientMessage;
    }
}
