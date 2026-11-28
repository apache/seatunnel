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

package org.apache.seatunnel.e2e.connector.coap;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Independent producers of telemetry representations for the UDP peer. */
final class CoapWireFixtures {
    private CoapWireFixtures() {}

    static byte[] flow5() {
        ByteBuffer wire = ByteBuffer.allocate(72);
        wire.putShort((short) 5).putShort((short) 1);
        wire.putInt(90000).putInt(1770000000).putInt(7).putInt(88);
        wire.put((byte) 1).put((byte) 3).putShort((short) 100);
        wire.put(new byte[] {(byte) 192, 0, 2, 1, (byte) 198, 51, 100, 2, 0, 0, 0, 0});
        wire.putShort((short) 15).putShort((short) 16);
        wire.putInt(-1).putInt(2024).putInt(1010).putInt(1020);
        wire.putShort((short) 12345).putShort((short) 443);
        wire.put((byte) 0).put((byte) 18).put((byte) 6).put((byte) 0);
        wire.putShort((short) 64500).putShort((short) 64501);
        wire.put((byte) 24).put((byte) 25).putShort((short) 0);
        return wire.array();
    }

    static byte[] flow9Templates() {
        ByteBuffer wire = flow9(56, 2, 123);
        // FlowSet 0, template 256 for an IPV4_SRC_ADDR and eight-octet IN_BYTES.
        wire.putShort((short) 0)
                .putShort((short) 16)
                .putShort((short) 256)
                .putShort((short) 2)
                .putShort((short) 8)
                .putShort((short) 4)
                .putShort((short) 1)
                .putShort((short) 8);
        // FlowSet 1, template 257 with a four-octet system scope and sampling interval.
        wire.putShort((short) 1)
                .putShort((short) 20)
                .putShort((short) 257)
                .putShort((short) 4)
                .putShort((short) 4)
                .putShort((short) 1)
                .putShort((short) 4)
                .putShort((short) 34)
                .putShort((short) 4)
                .putShort((short) 0);
        return wire.array();
    }

    static byte[] flow9Data(int observationDomain) {
        ByteBuffer wire = flow9(48, 2, observationDomain);
        wire.putShort((short) 256)
                .putShort((short) 16)
                .put(new byte[] {(byte) 192, 0, 2, 40})
                .putLong(-1L);
        wire.putShort((short) 257).putShort((short) 12).putInt(19).putInt(10000);
        return wire.array();
    }

    static byte[] flow9ShortTemplate() {
        ByteBuffer wire = flow9(32, 1, 123);
        wire.putShort((short) 0)
                .putShort((short) 12)
                .putShort((short) 258)
                .putShort((short) 1)
                .putShort((short) 4)
                .putShort((short) 1);
        return wire.array();
    }

    static byte[] flow9ShortData() {
        ByteBuffer wire = flow9(28, 1, 123);
        wire.putShort((short) 258).putShort((short) 8).put(new byte[] {6, 0, 0, 0});
        return wire.array();
    }

    private static ByteBuffer flow9(int storage, int count, int domain) {
        ByteBuffer wire = ByteBuffer.allocate(storage);
        wire.putShort((short) 9)
                .putShort((short) count)
                .putInt(90123)
                .putInt(1770001000)
                .putInt(314159)
                .putInt(domain);
        return wire;
    }

    static byte[] collectd() throws Exception {
        ByteArrayOutputStream packet = new ByteArrayOutputStream();
        part(packet, 0, "node-01\0".getBytes(StandardCharsets.UTF_8));
        part(packet, 2, "cpu\0".getBytes(StandardCharsets.UTF_8));
        part(packet, 4, "cpu\0".getBytes(StandardCharsets.UTF_8));
        part(packet, 8, ByteBuffer.allocate(8).putLong(1024L << 30).array());
        part(packet, 9, ByteBuffer.allocate(8).putLong(3L << 29).array());
        ByteBuffer values =
                ByteBuffer.allocate(38).putShort((short) 4).put(new byte[] {0, 1, 2, 3});
        values.putLong(0xffffffffL);
        values.put(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(1.25).array());
        values.putLong(-5).putLong(-1);
        part(packet, 6, values.array());
        return packet.toByteArray();
    }

    static byte[] signedCollectd(byte[] payload) throws Exception {
        byte[] username = "fixture".getBytes(StandardCharsets.UTF_8);
        byte[] secret = "fixture-only-collectd-key".getBytes(StandardCharsets.UTF_8);
        Mac hash = Mac.getInstance("HmacSHA256");
        hash.init(new SecretKeySpec(secret, "HmacSHA256"));
        hash.update(username);
        byte[] signature = hash.doFinal(payload);
        return ByteBuffer.allocate(36 + username.length + payload.length)
                .putShort((short) 0x0200)
                .putShort((short) (36 + username.length))
                .put(signature)
                .put(username)
                .put(payload)
                .array();
    }

    static byte[] encryptedCollectd(byte[] payload) throws Exception {
        byte[] username = "fixture".getBytes(StandardCharsets.UTF_8);
        byte[] secret = "fixture-only-collectd-key".getBytes(StandardCharsets.UTF_8);
        byte[] iv = new byte[] {17, 9, 23, 5, 7, 3, 11, 19, 21, 13, 15, 27, 31, 33, 35, 37};
        byte[] key = MessageDigest.getInstance("SHA-256").digest(secret);
        Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
        byte[] binding = MessageDigest.getInstance("SHA-1").digest(payload);
        byte[] encrypted =
                cipher.doFinal(
                        ByteBuffer.allocate(binding.length + payload.length)
                                .put(binding)
                                .put(payload)
                                .array());
        int length = 6 + username.length + iv.length + encrypted.length;
        return ByteBuffer.allocate(length)
                .putShort((short) 0x0210)
                .putShort((short) length)
                .putShort((short) username.length)
                .put(username)
                .put(iv)
                .put(encrypted)
                .array();
    }

    private static void part(ByteArrayOutputStream output, int tag, byte[] value) throws Exception {
        output.write(
                ByteBuffer.allocate(4)
                        .putShort((short) tag)
                        .putShort((short) (4 + value.length))
                        .array());
        output.write(value);
    }
}
