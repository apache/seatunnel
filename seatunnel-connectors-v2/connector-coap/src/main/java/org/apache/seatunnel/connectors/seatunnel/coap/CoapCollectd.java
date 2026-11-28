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

package org.apache.seatunnel.connectors.seatunnel.coap;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Collectd network protocol values, metadata, notifications and authenticated packets. */
final class CoapCollectd {
    private static final int SIGN = 0x0200;
    private static final int ENCRYPT = 0x0210;
    private static final String[] GAUGE_TYPES = {"counter", "gauge", "derive", "absolute"};
    private static final BigInteger THOUSAND = BigInteger.valueOf(1000);
    private final byte[] username;
    private final byte[] password;
    private final Map<String, String> types;
    private final boolean convertTime;
    private final boolean omitInterval;

    CoapCollectd(CoapSettings.Listener settings) {
        String user = settings.options.get(CoapOptions.COLLECTD_USER);
        String secret = settings.options.get(CoapOptions.COLLECTD_PASSWORD);
        if ((user == null) != (secret == null)
                || (user != null
                        && (user.isEmpty()
                                || secret.isEmpty()
                                || user.length() > 255
                                || secret.length() > 4096))) {
            throw CoapSettings.invalid(
                    "collectd_username and collectd_password must be supplied together");
        }
        this.username = user == null ? null : user.getBytes(StandardCharsets.UTF_8);
        this.password = secret == null ? null : secret.getBytes(StandardCharsets.UTF_8);
        this.types = settings.options.get(CoapOptions.COLLECTD_TYPES);
        this.convertTime = settings.options.get(CoapOptions.COLLECTD_CONVERT_TIME);
        this.omitInterval = settings.options.get(CoapOptions.COLLECTD_OMIT_INTERVAL);
        if (types.size() > 256
                || types.values().stream().anyMatch(names -> names.length() > 4096)) {
            throw CoapSettings.invalid("collectd_types exceeds its configured mapping budget");
        }
    }

    List<ObjectNode> decode(byte[] packet) throws IOException {
        if (packet.length < 4) {
            throw invalid();
        }
        byte[] body = unwrap(packet);
        ByteBuffer data = ByteBuffer.wrap(body);
        ObjectNode base = CoapPayload.JSON.createObjectNode();
        List<ObjectNode> records = new ArrayList<>();
        while (data.hasRemaining()) {
            if (data.remaining() < 4 || records.size() == CoapSettings.MAX_ROWS) {
                throw invalid();
            }
            int tag = unsignedShort(data);
            int length = unsignedShort(data) - 4;
            if (length < 0 || length > data.remaining()) {
                throw invalid();
            }
            ByteBuffer value = data.slice();
            value.limit(length);
            data.position(data.position() + length);
            switch (tag) {
                case 0:
                    base.put("host", string(value));
                    break;
                case 1:
                case 8:
                    base.put("time", time(value, tag == 8));
                    break;
                case 2:
                    base.put("plugin", string(value));
                    break;
                case 3:
                    base.put("plugin_instance", string(value));
                    break;
                case 4:
                    base.put("type", string(value));
                    break;
                case 5:
                    base.put("type_instance", string(value));
                    break;
                case 6:
                    records.add(values(value, base));
                    break;
                case 7:
                case 9:
                    BigInteger interval = time(value, tag == 9);
                    if (!omitInterval) {
                        base.put("interval", interval);
                    }
                    break;
                case 0x0100:
                    records.add(base.deepCopy().put("message", string(value)));
                    break;
                case 0x0101:
                    base.put("severity", unsigned(value));
                    break;
                case SIGN:
                case ENCRYPT:
                    throw invalid();
                default:
                    // Length-delimited extension parts may be skipped.
            }
        }
        if (records.isEmpty()) {
            throw invalid();
        }
        return records;
    }

    private byte[] unwrap(byte[] packet) throws IOException {
        ByteBuffer wire = ByteBuffer.wrap(packet);
        int type = unsignedShort(wire);
        int length = unsignedShort(wire);
        if (type != SIGN && type != ENCRYPT) {
            if (username != null) {
                throw invalid();
            }
            return packet;
        }
        if (username == null || length > packet.length) {
            throw invalid();
        }
        try {
            if (type == SIGN) {
                int userLength = length - 36;
                if (userLength != username.length) {
                    throw invalid();
                }
                byte[] expected = bytes(wire, 32);
                if (!MessageDigest.isEqual(username, bytes(wire, userLength))) {
                    throw invalid();
                }
                Mac auth = Mac.getInstance("HmacSHA256");
                auth.init(new SecretKeySpec(password, "HmacSHA256"));
                auth.update(packet, 36, packet.length - 36);
                if (!MessageDigest.isEqual(expected, auth.doFinal())) {
                    throw invalid();
                }
                return Arrays.copyOfRange(packet, length, packet.length);
            }
            if (length != packet.length || wire.remaining() < 2 + 16 + 20) {
                throw invalid();
            }
            int userLength = unsignedShort(wire);
            if (userLength != username.length
                    || wire.remaining() <= userLength + 16 + 20
                    || !MessageDigest.isEqual(username, bytes(wire, userLength))) {
                throw invalid();
            }
            byte[] iv = bytes(wire, 16);
            byte[] key = MessageDigest.getInstance("SHA-256").digest(password);
            Cipher cipher = Cipher.getInstance("AES/OFB/NoPadding");
            cipher.init(
                    Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new IvParameterSpec(iv));
            byte[] clear = cipher.doFinal(bytes(wire, wire.remaining()));
            byte[] body = Arrays.copyOfRange(clear, 20, clear.length);
            if (!MessageDigest.isEqual(
                    Arrays.copyOf(clear, 20), MessageDigest.getInstance("SHA-1").digest(body))) {
                throw invalid();
            }
            return body;
        } catch (GeneralSecurityException e) {
            throw invalid();
        }
    }

    private ObjectNode values(ByteBuffer value, ObjectNode metadata) throws IOException {
        if (value.remaining() < 2
                || !metadata.has("host")
                || !metadata.has("type")
                || !metadata.has("plugin")) {
            throw invalid();
        }
        int count = unsignedShort(value);
        if (count == 0 || count > 128 || value.remaining() != 9 * count) {
            throw invalid();
        }
        byte[] codes = bytes(value, count);
        String configured = types.get(metadata.path("type").asText());
        String[] fields = configured == null ? null : configured.split(",", -1);
        if (fields != null && fields.length != count) {
            throw invalid();
        }
        ObjectNode record = metadata.deepCopy();
        ArrayNode metrics = record.putArray("values");
        for (int i = 0; i < count; i++) {
            int code = Byte.toUnsignedInt(codes[i]);
            if (code >= GAUGE_TYPES.length) {
                throw invalid();
            }
            ObjectNode metric = metrics.addObject().put("kind", GAUGE_TYPES[code]);
            if (fields != null) {
                metric.put("name", fields[i].trim());
            }
            if (code == 1) {
                byte[] gauge = bytes(value, 8);
                double amount = ByteBuffer.wrap(gauge).order(ByteOrder.LITTLE_ENDIAN).getDouble();
                if (Double.isFinite(amount)) {
                    metric.put("value", amount);
                } else {
                    metric.putNull("value");
                }
            } else if (code == 2) {
                metric.put("value", value.getLong());
            } else {
                metric.put("value", new BigInteger(1, bytes(value, 8)));
            }
        }
        return record;
    }

    private BigInteger time(ByteBuffer input, boolean hires) throws IOException {
        BigInteger encoded = unsigned(input);
        if (!convertTime) {
            return encoded;
        }
        BigInteger millis = encoded.multiply(THOUSAND);
        return hires ? millis.shiftRight(30) : millis;
    }

    private static BigInteger unsigned(ByteBuffer input) throws IOException {
        if (input.remaining() != 8) {
            throw invalid();
        }
        return new BigInteger(1, bytes(input, 8));
    }

    private static String string(ByteBuffer input) throws IOException {
        if (input.remaining() == 0 || input.remaining() > 1024) {
            throw invalid();
        }
        byte[] value = bytes(input, input.remaining());
        if (value[value.length - 1] != 0) {
            throw invalid();
        }
        for (int i = 0; i < value.length - 1; i++) {
            if (value[i] == 0) {
                throw invalid();
            }
        }
        return CoapPayload.decodeText(Arrays.copyOf(value, value.length - 1));
    }

    private static int unsignedShort(ByteBuffer buffer) {
        return Short.toUnsignedInt(buffer.getShort());
    }

    private static byte[] bytes(ByteBuffer input, int count) {
        byte[] data = new byte[count];
        input.get(data);
        return data;
    }

    private static IOException invalid() {
        return new IOException("invalid or unauthenticated collectd packet");
    }
}
