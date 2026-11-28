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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Fixed HTTP/1.1 upgrade and RFC 6455 framing for the inbound listener. */
final class WebSocketServerProtocol {
    private static final int MAX_HEADERS_BYTES = 16384;
    private static final int MAX_HEADERS = 64;
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private WebSocketServerProtocol() {}

    static void upgrade(
            InputStream in, OutputStream out, WebSocketServerSourceOptions.Settings settings)
            throws IOException {
        int[] remaining = {MAX_HEADERS_BYTES};
        String[] request = line(in, remaining).split(" ", -1);
        if (request.length != 3 || !"GET".equals(request[0]) || !"HTTP/1.1".equals(request[2])) {
            throw new UpgradeFailure(400);
        }
        if (!settings.path.equals(request[1])) {
            throw new UpgradeFailure(404);
        }
        Map<String, String> headers = new HashMap<>();
        int count = 0;
        String field;
        while (!(field = line(in, remaining)).isEmpty()) {
            int delimiter = field.indexOf(':');
            if (++count > MAX_HEADERS || delimiter <= 0) {
                throw new UpgradeFailure(400);
            }
            String name = field.substring(0, delimiter).toLowerCase(Locale.ROOT);
            if (!name.matches("[!#$%&'*+.^_\\x60|~0-9a-z-]+")) {
                throw new UpgradeFailure(400);
            }
            String value = field.substring(delimiter + 1).trim();
            if (headers.containsKey(name)
                    && ("connection".equals(name) || "upgrade".equals(name))) {
                headers.merge(name, value, (old, next) -> old + "," + next);
            } else if (headers.putIfAbsent(name, value) != null && isUnique(name)) {
                throw new UpgradeFailure(400);
            }
        }
        String host = headers.get("host");
        if (host == null
                || !WebSocketServerSourceOptions.Settings.validHost(host)
                || settings.expectedHost != null && !settings.expectedHost.equalsIgnoreCase(host)
                || headers.containsKey("transfer-encoding")
                || (headers.containsKey("content-length")
                        && !"0".equals(headers.get("content-length")))
                || !containsToken(headers.get("connection"), "upgrade")
                || !containsToken(headers.get("upgrade"), "websocket")) {
            throw new UpgradeFailure(400);
        }
        if (!"13".equals(headers.get("sec-websocket-version"))) {
            throw new UpgradeFailure(426);
        }
        String key = headers.get("sec-websocket-key");
        if (!validKey(key)) {
            throw new UpgradeFailure(400);
        }
        String origin = headers.get("origin");
        if (origin != null) {
            try {
                if (!settings.allowedOrigins.contains(
                        WebSocketServerSourceOptions.Settings.origin(origin))) {
                    throw new UpgradeFailure(403);
                }
            } catch (IllegalArgumentException ex) {
                throw new UpgradeFailure(403);
            }
        }
        if (settings.token != null) {
            String auth = headers.get("authorization");
            if (auth == null
                    || !auth.regionMatches(true, 0, "Bearer ", 0, 7)
                    || !MessageDigest.isEqual(
                            settings.token.getBytes(StandardCharsets.US_ASCII),
                            auth.substring(7).getBytes(StandardCharsets.US_ASCII))) {
                throw new UpgradeFailure(401);
            }
        }
        try {
            String accept =
                    Base64.getEncoder()
                            .encodeToString(
                                    MessageDigest.getInstance("SHA-1")
                                            .digest(
                                                    (key + GUID)
                                                            .getBytes(StandardCharsets.US_ASCII)));
            writeAscii(
                    out,
                    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                            + "Connection: Upgrade\r\nSec-WebSocket-Accept: "
                            + accept
                            + "\r\n\r\n");
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException("WebSocket upgrade digest unavailable");
        }
    }

    private static boolean isUnique(String name) {
        switch (name) {
            case "host":
            case "origin":
            case "authorization":
            case "content-length":
            case "transfer-encoding":
            case "sec-websocket-key":
            case "sec-websocket-version":
                return true;
            default:
                return false;
        }
    }

    private static boolean containsToken(String value, String expected) {
        if (value != null) {
            for (String item : value.split(",", -1)) {
                if (expected.equalsIgnoreCase(item.trim())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean validKey(String key) {
        if (key == null || key.length() != 24) {
            return false;
        }
        try {
            byte[] value = Base64.getDecoder().decode(key);
            return value.length == 16 && key.equals(Base64.getEncoder().encodeToString(value));
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    private static String line(InputStream in, int[] remaining) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (; ; ) {
            if (--remaining[0] < 0) {
                throw new UpgradeFailure(400);
            }
            int value = readByte(in);
            if (value == '\r') {
                if (--remaining[0] < 0 || readByte(in) != '\n') {
                    throw new UpgradeFailure(400);
                }
                return new String(bytes.toByteArray(), StandardCharsets.US_ASCII);
            }
            if ((value < 32 && value != '\t') || value > 126) {
                throw new UpgradeFailure(400);
            }
            bytes.write(value);
        }
    }

    static void reject(OutputStream out, int status) throws IOException {
        String detail;
        switch (status) {
            case 401:
                detail = "401 Unauthorized\r\nWWW-Authenticate: Bearer";
                break;
            case 403:
                detail = "403 Forbidden";
                break;
            case 404:
                detail = "404 Not Found";
                break;
            case 426:
                detail = "426 Upgrade Required\r\nSec-WebSocket-Version: 13";
                break;
            default:
                detail = "400 Bad Request";
        }
        writeAscii(
                out, "HTTP/1.1 " + detail + "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    }

    private static void writeAscii(OutputStream out, String value) throws IOException {
        out.write(value.getBytes(StandardCharsets.US_ASCII));
        out.flush();
    }

    /** Validate the header and remaining message budget before allocating any payload. */
    static Frame readFrame(InputStream in, int first, int remainingPayload, int maximumPayload)
            throws IOException {
        boolean fin = (first & 128) != 0;
        int opcode = first & 15;
        int second = readByte(in);
        boolean control = opcode >= 8;
        int code = second & 127;
        if ((first & 112) != 0
                || (second & 128) == 0
                || (opcode > 2 && opcode < 8)
                || opcode > 10
                || (control && (!fin || code > 125))) {
            throw new FrameFailure(1002);
        }
        long size = code;
        if (code == 126) {
            size = (readByte(in) << 8) | readByte(in);
            if (size < 126) {
                throw new FrameFailure(1002);
            }
        } else if (code == 127) {
            size = readByte(in);
            if ((size & 128) != 0) {
                throw new FrameFailure(1002);
            }
            for (int offset = 0; offset < 7; offset++) {
                size = (size << 8) | readByte(in);
            }
            if (size < 65536) {
                throw new FrameFailure(1002);
            }
        }
        if (!control && size > (opcode == 0 ? remainingPayload : maximumPayload)) {
            throw new FrameFailure(1009);
        }
        byte[] mask = new byte[4];
        consume(in, mask);
        byte[] payload = new byte[(int) size];
        consume(in, payload);
        for (int i = 0; i < payload.length; i++) {
            payload[i] ^= mask[i & 3];
        }
        return new Frame(opcode, fin, payload);
    }

    static String text(byte[] data) throws FrameFailure {
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(data)).toString();
        } catch (CharacterCodingException ex) {
            throw new FrameFailure(1007);
        }
    }

    static void checkClosePayload(byte[] payload) throws FrameFailure {
        if (payload.length == 0) {
            return;
        }
        if (payload.length == 1) {
            throw new FrameFailure(1002);
        }
        int code = ((payload[0] & 255) << 8) | (payload[1] & 255);
        if (code < 1000
                || code > 4999
                || code == 1004
                || code == 1005
                || code == 1006
                || (code >= 1015 && code < 3000)) {
            throw new FrameFailure(1002);
        }
        if (payload.length > 2) {
            text(Arrays.copyOfRange(payload, 2, payload.length));
        }
    }

    static void control(OutputStream out, int opcode, byte[] payload) throws IOException {
        out.write(128 | opcode);
        out.write(payload.length);
        out.write(payload);
        out.flush();
    }

    static void close(OutputStream out, int code) throws IOException {
        control(out, 8, new byte[] {(byte) (code >> 8), (byte) code});
    }

    private static int readByte(InputStream in) throws IOException {
        int value = in.read();
        if (value < 0) {
            throw new EOFException();
        }
        return value;
    }

    private static void consume(InputStream in, byte[] value) throws IOException {
        int offset = 0;
        while (offset < value.length) {
            int count = in.read(value, offset, value.length - offset);
            if (count < 0) {
                throw new EOFException();
            }
            offset += count;
        }
    }

    static final class Frame {
        final int opcode;
        final boolean fin;
        final byte[] payload;

        Frame(int opcode, boolean fin, byte[] payload) {
            this.opcode = opcode;
            this.fin = fin;
            this.payload = payload;
        }
    }

    static final class UpgradeFailure extends IOException {
        final int status;

        UpgradeFailure(int status) {
            this.status = status;
        }
    }

    static final class FrameFailure extends IOException {
        final int code;

        FrameFailure(int code) {
            this.code = code;
        }
    }
}
