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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.queue.EdgeSocketQueuedRecord;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.payload.EdgeSocketCompressionPayloadDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record.EdgeSocketPacketRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.serialize.record.EdgeSocketRawRecordDeserializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

class EdgeSocketPacketDeserializerTest {

    private final EdgeSocketCompressionPayloadDeserializer payloadDeserializer =
            new EdgeSocketCompressionPayloadDeserializer();
    private final EdgeSocketRawRecordDeserializer rawDeserializer =
            new EdgeSocketRawRecordDeserializer();

    /** RAW mode: plain text passes through unchanged. */
    @Test
    void rawModeShouldPassThroughPayload() {
        String message = "hello-raw-payload";
        EdgeSocketQueuedRecord record = rawDeserializer.deserializeRecord(message);

        Assertions.assertEquals(EdgeSocketCompressionType.NONE, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(message, decoded);
    }

    /** RAW mode: empty string is accepted without exception. */
    @Test
    void rawModeShouldAcceptEmptyString() {
        EdgeSocketQueuedRecord record = rawDeserializer.deserializeRecord("");
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals("", decoded);
    }

    /** NONE compression: payload round-trips correctly via payload deserializer. */
    @Test
    void noneCompressionRoundTrip() {
        String original = "round-trip-none";
        byte[] payloadBytes = original.getBytes(StandardCharsets.UTF_8);
        EdgeSocketQueuedRecord record =
                new EdgeSocketQueuedRecord(payloadBytes, EdgeSocketCompressionType.NONE);

        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** GZIP compression: compressed payload decompresses back to original text. */
    @Test
    void gzipCompressionRoundTrip() throws IOException {
        String original = "gzip-compressed-payload-content";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));
        EdgeSocketQueuedRecord record =
                new EdgeSocketQueuedRecord(compressed, EdgeSocketCompressionType.GZIP);

        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** ZLIB compression: compressed payload decompresses back to original text. */
    @Test
    void zlibCompressionRoundTrip() throws IOException {
        String original = "zlib-compressed-payload-content";
        byte[] compressed = deflate(original.getBytes(StandardCharsets.UTF_8), false);
        EdgeSocketQueuedRecord record =
                new EdgeSocketQueuedRecord(compressed, EdgeSocketCompressionType.ZLIB);

        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** DEFLATE (raw, no zlib header) compression: decompresses back to original text. */
    @Test
    void deflateCompressionRoundTrip() throws IOException {
        String original = "deflate-compressed-payload-content";
        byte[] compressed = deflate(original.getBytes(StandardCharsets.UTF_8), true);
        EdgeSocketQueuedRecord record =
                new EdgeSocketQueuedRecord(compressed, EdgeSocketCompressionType.DEFLATE);

        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /**
     * PACKET mode with NONE compression: a JSON envelope with base64 payload produces a queued
     * record that payload deserializer can decode correctly.
     */
    @Test
    void packetModeNoneCompressionRoundTrip() {
        String original = "packet-none-payload";
        String base64Payload =
                Base64.getEncoder().encodeToString(original.getBytes(StandardCharsets.UTF_8));
        String json =
                "{\"version\":1,\"payload\":\""
                        + base64Payload
                        + "\",\"compression\":\"NONE\",\"encryption\":\"NONE\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer = buildPacketDeserializer();

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        Assertions.assertEquals(EdgeSocketCompressionType.NONE, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /**
     * PACKET mode with GZIP compression: the payload field contains base64(gzip(original)). After
     * PACKET deserialization the queued record has compressionType=GZIP, and the payload
     * deserializer decompresses it back to original text.
     */
    @Test
    void packetModeGzipCompressionRoundTrip() throws IOException {
        String original = "packet-gzip-payload";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));
        String base64Payload = Base64.getEncoder().encodeToString(compressed);
        String json =
                "{\"version\":1,\"payload\":\""
                        + base64Payload
                        + "\",\"compression\":\"GZIP\",\"encryption\":\"NONE\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer = buildPacketDeserializer();

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        Assertions.assertEquals(EdgeSocketCompressionType.GZIP, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** PACKET mode with ZLIB compression round-trips correctly. */
    @Test
    void packetModeZlibCompressionRoundTrip() throws IOException {
        String original = "packet-zlib-payload";
        byte[] compressed = deflate(original.getBytes(StandardCharsets.UTF_8), false);
        String base64Payload = Base64.getEncoder().encodeToString(compressed);
        String json =
                "{\"version\":1,\"payload\":\""
                        + base64Payload
                        + "\",\"compression\":\"ZLIB\",\"encryption\":\"NONE\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer = buildPacketDeserializer();

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        Assertions.assertEquals(EdgeSocketCompressionType.ZLIB, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** PACKET mode with DEFLATE (raw) compression round-trips correctly. */
    @Test
    void packetModeDeflateCompressionRoundTrip() throws IOException {
        String original = "packet-deflate-payload";
        byte[] compressed = deflate(original.getBytes(StandardCharsets.UTF_8), true);
        String base64Payload = Base64.getEncoder().encodeToString(compressed);
        String json =
                "{\"version\":1,\"payload\":\""
                        + base64Payload
                        + "\",\"compression\":\"DEFLATE\",\"encryption\":\"NONE\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer = buildPacketDeserializer();

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        Assertions.assertEquals(EdgeSocketCompressionType.DEFLATE, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** AES_GCM encryption: encrypted payload decrypts back to original text. */
    @Test
    void packetModeAesGcmEncryptionRoundTrip() throws Exception {
        String original = "aes-gcm-encrypted-payload";
        byte[] plaintext = original.getBytes(StandardCharsets.UTF_8);

        SecretKey aesKey = generateAesKey();
        String secretKeyBase64 = Base64.getEncoder().encodeToString(aesKey.getEncoded());

        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        byte[] ciphertext = aesGcmEncrypt(plaintext, aesKey, iv);

        String json =
                "{\"version\":1,\"payload\":\""
                        + Base64.getEncoder().encodeToString(ciphertext)
                        + "\",\"compression\":\"NONE\",\"encryption\":\"AES_GCM\",\"iv\":\""
                        + Base64.getEncoder().encodeToString(iv)
                        + "\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer =
                buildPacketDeserializerWithKey(secretKeyBase64);

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** AES_GCM + GZIP: encrypted compressed payload round-trips correctly. */
    @Test
    void packetModeAesGcmWithGzipRoundTrip() throws Exception {
        String original = "aes-gcm-gzip-combined-payload";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));

        SecretKey aesKey = generateAesKey();
        String secretKeyBase64 = Base64.getEncoder().encodeToString(aesKey.getEncoded());

        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        byte[] ciphertext = aesGcmEncrypt(compressed, aesKey, iv);

        String json =
                "{\"version\":1,\"payload\":\""
                        + Base64.getEncoder().encodeToString(ciphertext)
                        + "\",\"compression\":\"GZIP\",\"encryption\":\"AES_GCM\",\"iv\":\""
                        + Base64.getEncoder().encodeToString(iv)
                        + "\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer =
                buildPacketDeserializerWithKey(secretKeyBase64);

        EdgeSocketQueuedRecord record = packetDeserializer.deserializeRecord(json);
        Assertions.assertEquals(EdgeSocketCompressionType.GZIP, record.getCompressionType());
        String decoded = payloadDeserializer.deserializeRecord(record);
        Assertions.assertEquals(original, decoded);
    }

    /** AES_GCM without secret_key configured should throw exception. */
    @Test
    void packetModeAesGcmMissingKeyThrows() throws Exception {
        String payload =
                Base64.getEncoder().encodeToString("data".getBytes(StandardCharsets.UTF_8));
        String iv = Base64.getEncoder().encodeToString(new byte[12]);
        String json =
                "{\"version\":1,\"payload\":\""
                        + payload
                        + "\",\"compression\":\"NONE\",\"encryption\":\"AES_GCM\",\"iv\":\""
                        + iv
                        + "\"}";

        EdgeSocketPacketRecordDeserializer packetDeserializer = buildPacketDeserializer();

        Assertions.assertThrows(
                EdgeSocketConnectorException.class,
                () -> packetDeserializer.deserializeRecord(json));
    }

    private EdgeSocketPacketRecordDeserializer buildPacketDeserializer() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 9999);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "test-token");
        configMap.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EdgeSocketConfig edgeConfig = new EdgeSocketConfig(config);
        return new EdgeSocketPacketRecordDeserializer(edgeConfig);
    }

    private EdgeSocketPacketRecordDeserializer buildPacketDeserializerWithKey(
            String secretKeyBase64) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), 9999);
        configMap.put(EdgeSocketSourceOptions.TOKEN.key(), "test-token");
        configMap.put(EdgeSocketSourceOptions.PACKET_MODE.key(), "PACKET");
        configMap.put(EdgeSocketSourceOptions.SECRET_KEY.key(), secretKeyBase64);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        EdgeSocketConfig edgeConfig = new EdgeSocketConfig(config);
        return new EdgeSocketPacketRecordDeserializer(edgeConfig);
    }

    private static SecretKey generateAesKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256);
        return keyGen.generateKey();
    }

    private static byte[] aesGcmEncrypt(byte[] plaintext, SecretKey key, byte[] iv)
            throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(128, iv));
        return cipher.doFinal(plaintext);
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(input);
        }
        return baos.toByteArray();
    }

    private static byte[] deflate(byte[] input, boolean nowrap) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream dos =
                new DeflaterOutputStream(
                        baos, new Deflater(Deflater.DEFAULT_COMPRESSION, nowrap))) {
            dos.write(input);
        }
        return baos.toByteArray();
    }
}
