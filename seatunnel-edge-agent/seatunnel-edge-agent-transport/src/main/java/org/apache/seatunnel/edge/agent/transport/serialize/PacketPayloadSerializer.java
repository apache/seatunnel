/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.edge.agent.transport.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.edge.agent.transport.packet.EdgeIngressPacket;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketCompressionType;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePacketEncryptionType;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePayloadCompressor;
import org.apache.seatunnel.edge.agent.transport.packet.EdgePayloadEncryptor;

import java.util.Base64;
import java.util.Objects;

public class PacketPayloadSerializer implements PayloadSerializer {

    private final EdgePacketCompressionType compressionType;
    private final EdgePacketEncryptionType encryptionType;
    private final byte[] aesKey;
    private final ObjectMapper objectMapper;

    public PacketPayloadSerializer(
            EdgePacketCompressionType compressionType,
            EdgePacketEncryptionType encryptionType,
            byte[] aesKey) {
        this.compressionType = Objects.requireNonNull(compressionType, "compressionType");
        this.encryptionType = Objects.requireNonNull(encryptionType, "encryptionType");
        if (encryptionType == EdgePacketEncryptionType.AES_GCM) {
            this.aesKey =
                    Objects.requireNonNull(aesKey, "aesKey is required when encryption is AES_GCM");
        } else {
            this.aesKey = aesKey;
        }
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String serialize(byte[] rawPayload) {
        try {
            byte[] compressed = EdgePayloadCompressor.compress(rawPayload, compressionType);

            byte[] body;
            String ivBase64 = null;
            if (encryptionType == EdgePacketEncryptionType.AES_GCM) {
                byte[] iv = EdgePayloadEncryptor.generateIv();
                body = EdgePayloadEncryptor.encrypt(compressed, aesKey, iv);
                ivBase64 = Base64.getEncoder().encodeToString(iv);
            } else {
                body = compressed;
            }

            String payloadBase64 = Base64.getEncoder().encodeToString(body);

            EdgeIngressPacket packet =
                    EdgeIngressPacket.builder()
                            .version(1)
                            .payload(payloadBase64)
                            .compression(compressionType.getValue())
                            .encryption(encryptionType.getValue())
                            .iv(ivBase64)
                            .build();

            return objectMapper.writeValueAsString(packet);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize EdgeIngressPacket to JSON", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize WAL payload to packet wire format", e);
        }
    }
}
