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

import static com.hazelcast.client.impl.protocol.ClientMessage.PARTITION_ID_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.RESPONSE_BACKUP_ACKS_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.TYPE_FIELD_OFFSET;
import static com.hazelcast.client.impl.protocol.ClientMessage.UNFRAGMENTED_MESSAGE;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.BYTE_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.INT_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.LONG_SIZE_IN_BYTES;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.decodeLong;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeBoolean;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeInt;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.encodeLong;

/** */
@Generated("fff1cf66eb87ca2e79cdb8ba0946517c")
public final class SeaTunnelGetCheckpointHistoryCodec {
    // hex: 0xDE2002
    public static final int REQUEST_MESSAGE_TYPE = 14593538;
    // hex: 0xDE2003
    public static final int RESPONSE_MESSAGE_TYPE = 14593539;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET =
            PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_PIPELINE_ID_FIELD_OFFSET =
            REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_HAS_PIPELINE_ID_FIELD_OFFSET =
            REQUEST_PIPELINE_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_LIMIT_FIELD_OFFSET =
            REQUEST_HAS_PIPELINE_ID_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int REQUEST_STATUS_FIELD_OFFSET =
            REQUEST_LIMIT_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE =
            REQUEST_STATUS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE =
            RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private SeaTunnelGetCheckpointHistoryCodec() {}

    public static class RequestParameters {
        public long jobId;
        public int pipelineId;
        public boolean hasPipelineId;
        public int limit;
        public int statusOrdinal;
    }

    public static ClientMessage encodeRequest(
            long jobId, Integer pipelineId, int limit, int statusOrdinal) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setRetryable(true);
        clientMessage.setOperationName("SeaTunnel.GetCheckpointHistory");
        ClientMessage.Frame initialFrame =
                new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        encodeInt(
                initialFrame.content,
                REQUEST_PIPELINE_ID_FIELD_OFFSET,
                pipelineId == null ? 0 : pipelineId);
        encodeBoolean(
                initialFrame.content, REQUEST_HAS_PIPELINE_ID_FIELD_OFFSET, pipelineId != null);
        encodeInt(initialFrame.content, REQUEST_LIMIT_FIELD_OFFSET, limit);
        encodeInt(initialFrame.content, REQUEST_STATUS_FIELD_OFFSET, statusOrdinal);
        clientMessage.add(initialFrame);
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        ClientMessage.Frame initialFrame = iterator.next();
        RequestParameters parameters = new RequestParameters();
        parameters.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        parameters.pipelineId = decodeInt(initialFrame.content, REQUEST_PIPELINE_ID_FIELD_OFFSET);
        parameters.hasPipelineId =
                decodeBoolean(initialFrame.content, REQUEST_HAS_PIPELINE_ID_FIELD_OFFSET);
        parameters.limit = decodeInt(initialFrame.content, REQUEST_LIMIT_FIELD_OFFSET);
        parameters.statusOrdinal = decodeInt(initialFrame.content, REQUEST_STATUS_FIELD_OFFSET);
        return parameters;
    }

    public static ClientMessage encodeResponse(com.hazelcast.internal.serialization.Data response) {
        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame =
                new ClientMessage.Frame(
                        new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);

        DataCodec.encode(clientMessage, response);
        return clientMessage;
    }

    public static com.hazelcast.internal.serialization.Data decodeResponse(
            ClientMessage clientMessage) {
        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        iterator.next();
        return DataCodec.decode(iterator);
    }
}
