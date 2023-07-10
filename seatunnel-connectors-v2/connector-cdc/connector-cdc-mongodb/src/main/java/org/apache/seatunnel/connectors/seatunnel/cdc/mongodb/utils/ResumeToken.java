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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import javax.annotation.Nonnull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

public class ResumeToken {

    private static final int K_TIMESTAMP = 130;

    public static @Nonnull BsonTimestamp decodeTimestamp(BsonDocument resumeToken) {
        Objects.requireNonNull(resumeToken, "Missing ResumeToken.");
        BsonValue bsonValue = resumeToken.get("_data");
        byte[] keyStringBytes = extractKeyStringBytes(bsonValue);
        validateKeyType(keyStringBytes);

        ByteBuffer buffer = ByteBuffer.wrap(keyStringBytes).order(ByteOrder.BIG_ENDIAN);
        int t = buffer.getInt();
        int i = buffer.getInt();
        return new BsonTimestamp(t, i);
    }

    private static byte[] extractKeyStringBytes(@Nonnull BsonValue bsonValue) {
        if (bsonValue.isBinary()) {
            return bsonValue.asBinary().getData();
        } else if (bsonValue.isString()) {
            return hexToUint8Array(bsonValue.asString().getValue());
        } else {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Unknown resume token format: " + bsonValue);
        }
    }

    private static void validateKeyType(byte[] keyStringBytes) {
        int kType = keyStringBytes[0] & 0xff;
        if (kType != K_TIMESTAMP) {
            throw new MongodbConnectorException(
                    ILLEGAL_ARGUMENT, "Unknown keyType of timestamp: " + kType);
        }
    }

    private static byte[] hexToUint8Array(@Nonnull String str) {
        int len = str.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] =
                    (byte)
                            ((Character.digit(str.charAt(i), 16) << 4)
                                    + Character.digit(str.charAt(i + 1), 16));
        }
        return data;
    }
}
