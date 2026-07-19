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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketCompressionType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketEncryptionType;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.EdgeSocketRecordDeserializer;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.GeneralSecurityException;
import java.util.Base64;

public class EdgeSocketPacketRecordDeserializer implements EdgeSocketRecordDeserializer {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int AES_GCM_TAG_LENGTH_BITS = 128;

    private final EdgeSocketConfig config;

    public EdgeSocketPacketRecordDeserializer(EdgeSocketConfig config) {
        this.config = config;
    }

    /**
     * Parse packet envelope and decode encrypted payload into queue record.
     *
     * @param rawMessage ingress packet JSON string
     * @return queued payload bytes with declared compression type
     */
    @Override
    public EdgeSocketQueuedRecord deserializeRecord(String rawMessage) {
        try {
            EdgeSocketIngressPacket packet =
                    OBJECT_MAPPER.readValue(rawMessage, EdgeSocketIngressPacket.class);
            byte[] payloadBytes = Base64.getDecoder().decode(packet.getPayload());

            EdgeSocketEncryptionType encryptionType =
                    EdgeSocketEncryptionType.from(defaultIfBlank(packet.getEncryption()));
            byte[] decryptedPayload = decodeEncryption(payloadBytes, packet, encryptionType);

            EdgeSocketCompressionType compressionType =
                    EdgeSocketCompressionType.from(defaultIfBlank(packet.getCompression()));
            return new EdgeSocketQueuedRecord(decryptedPayload, compressionType);
        } catch (EdgeSocketConnectorException known) {
            throw known;
        } catch (Exception exception) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Deserialize edge ingress packet failed",
                    exception);
        }
    }

    /**
     * Decode payload encryption according to packet metadata.
     *
     * @param payloadBytes base64-decoded payload bytes from packet
     * @param packet ingress packet metadata
     * @param encryptionType resolved encryption type
     * @return decrypted payload bytes (or original bytes when encryption is NONE)
     */
    private byte[] decodeEncryption(
            byte[] payloadBytes,
            EdgeSocketIngressPacket packet,
            EdgeSocketEncryptionType encryptionType) {
        if (encryptionType == EdgeSocketEncryptionType.NONE) {
            return payloadBytes;
        }
        if (encryptionType != EdgeSocketEncryptionType.AES_GCM) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_UNSUPPORTED_ENCRYPTION,
                    "Unsupported packet encryption type: " + encryptionType);
        }
        if (config.getSecretKeyBytes() == null || config.getSecretKeyBytes().length == 0) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_AES_KEY_MISSING,
                    "Missing secret_key when packet encryption is AES_GCM");
        }
        if (packet.getIv() == null || packet.getIv().isEmpty()) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "Missing iv in AES_GCM packet");
        }
        try {
            byte[] iv = Base64.getDecoder().decode(packet.getIv());
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            SecretKeySpec key = new SecretKeySpec(config.getSecretKeyBytes(), "AES");
            cipher.init(
                    Cipher.DECRYPT_MODE, key, new GCMParameterSpec(AES_GCM_TAG_LENGTH_BITS, iv));
            return cipher.doFinal(payloadBytes);
        } catch (GeneralSecurityException securityException) {
            throw new EdgeSocketConnectorException(
                    EdgeSocketConnectorErrorCode.PACKET_DECODE_ERROR,
                    "AES_GCM decrypt failed",
                    securityException);
        }
    }

    private String defaultIfBlank(String value) {
        if (value == null || value.trim().isEmpty()) {
            return EdgeSocketCompressionType.NONE.getValue();
        }
        return value.trim();
    }
}
