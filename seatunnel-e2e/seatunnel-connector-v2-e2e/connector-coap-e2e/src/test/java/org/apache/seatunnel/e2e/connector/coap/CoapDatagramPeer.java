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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Independent CoAP UDP peer, launched in a separate JVM from the connector. */
public final class CoapDatagramPeer {
    private static final InetAddress LOOPBACK = new InetSocketAddress("127.0.0.1", 0).getAddress();
    private static final int URI_PATH = 11;
    private static final int FORMAT = 12;
    private static final int BLOCK1 = 27;
    private static final int SIZE1 = 60;
    private static final int CONTINUE = 95;

    private CoapDatagramPeer() {}

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[1]);
        switch (args[0]) {
            case "send":
                send(port, 0, Arrays.copyOfRange(args, 2, args.length));
                break;
            case "send-from":
                send(port, Integer.parseInt(args[2]), Arrays.copyOfRange(args, 3, args.length));
                break;
            case "burst":
                burst(port, Integer.parseInt(args[2]));
                break;
            case "listen":
                listen(port, args[2], Paths.get(args[3]));
                break;
            default:
                throw new IllegalArgumentException(args[0]);
        }
    }

    private static void send(int port, int from, String[] messages) throws Exception {
        try (DatagramSocket socket = client(port, from)) {
            for (String description : messages) {
                Frame request = Frame.from(description);
                send(socket, request, new InetSocketAddress(LOOPBACK, port));
                try {
                    Frame response = receive(socket);
                    if (!Arrays.equals(request.token, response.token)) {
                        throw new IOException("response token differs");
                    }
                    System.out.println(response.code);
                } catch (SocketTimeoutException e) {
                    System.out.println("timeout");
                }
            }
        }
    }

    private static DatagramSocket client(int port, int from) throws IOException {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(LOOPBACK, from));
        socket.connect(LOOPBACK, port);
        socket.setReceiveBufferSize(262144);
        socket.setSoTimeout(1200);
        return socket;
    }

    private static void burst(int port, int count) throws Exception {
        try (DatagramSocket socket = client(port, 0)) {
            for (int i = 0; i < count; i++) {
                Frame frame = new Frame(0, 2, 1000 + i, new byte[] {(byte) (i >> 8), (byte) i});
                frame.add(URI_PATH, text("sensor"));
                frame.add(URI_PATH, text("data"));
                frame.option(FORMAT, 0);
                frame.payload = text("burst-" + i);
                send(socket, frame, new InetSocketAddress(LOOPBACK, port));
            }
            while (true) {
                try {
                    Frame response = receive(socket);
                    int id = (response.token[0] & 255) * 256 + (response.token[1] & 255);
                    System.out.println(id + ":" + response.code);
                } catch (SocketTimeoutException done) {
                    return;
                }
            }
        }
    }

    private static void listen(int port, String mode, Path journal) throws Exception {
        try (DatagramSocket socket = new DatagramSocket(new InetSocketAddress(LOOPBACK, port))) {
            socket.setSoTimeout(100);
            Files.createFile(Paths.get(journal + ".ready"));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            Path stop = Paths.get(journal + ".stop");
            Path release = Paths.get(journal + ".release");
            int attempts = 0;
            ByteArrayOutputStream transfer = new ByteArrayOutputStream();
            while (System.nanoTime() - deadline < 0 && !Files.exists(stop)) {
                byte[] data = new byte[2048];
                DatagramPacket datagram = new DatagramPacket(data, data.length);
                try {
                    socket.receive(datagram);
                } catch (SocketTimeoutException e) {
                    continue;
                }
                Frame request = Frame.read(data, datagram.getLength());
                if (request.code == 0) {
                    continue;
                }
                attempts++;
                record(journal, "request", request, request.payload);
                if ("silent".equals(mode) || ("drop-first".equals(mode) && attempts == 1)) {
                    continue;
                }
                if ("hold".equals(mode)) {
                    while (System.nanoTime() - deadline < 0
                            && !Files.exists(release)
                            && !Files.exists(stop)) {
                        Thread.sleep(25);
                    }
                    if (!Files.exists(release)) {
                        continue;
                    }
                }
                InetSocketAddress remote = (InetSocketAddress) datagram.getSocketAddress();
                if ("reset".equals(mode)) {
                    send(socket, new Frame(3, 0, request.mid, new byte[0]), remote);
                    continue;
                }
                int code = "4xx".equals(mode) ? 128 : "5xx".equals(mode) ? 163 : 68;
                int block = request.value(BLOCK1);
                if (block >= 0) {
                    transfer.write(request.payload);
                    if ((block & 8) != 0) {
                        code = CONTINUE;
                    } else {
                        record(journal, "assembled", request, transfer.toByteArray());
                        transfer.reset();
                    }
                }
                Frame response =
                        new Frame(
                                request.type == 0 ? 2 : 1,
                                code,
                                request.type == 0 ? request.mid : (request.mid + 32768) & 65535,
                                request.token);
                if (block >= 0) {
                    response.option(BLOCK1, block);
                }
                if ("4xx".equals(mode) || "5xx".equals(mode)) {
                    response.option(FORMAT, 0);
                    response.payload = text("peer-response-redaction-marker");
                }
                send(socket, response, remote);
            }
        }
    }

    private static void record(Path journal, String kind, Frame frame, byte[] payload)
            throws IOException {
        String entry =
                String.join(
                                "\t",
                                kind,
                                Integer.toString(frame.code),
                                Integer.toString(frame.type),
                                Integer.toString(frame.mid),
                                b64(frame.token),
                                frame.path(),
                                Integer.toString(frame.value(FORMAT)),
                                Integer.toString(frame.value(BLOCK1)),
                                b64(payload))
                        + "\n";
        Files.write(journal, text(entry), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static byte[] text(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String b64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static void send(DatagramSocket socket, Frame frame, InetSocketAddress dest)
            throws IOException {
        byte[] datagram = frame.encode();
        socket.send(new DatagramPacket(datagram, datagram.length, dest));
    }

    private static Frame receive(DatagramSocket socket) throws IOException {
        byte[] data = new byte[2048];
        DatagramPacket datagram = new DatagramPacket(data, data.length);
        socket.receive(datagram);
        return Frame.read(data, datagram.getLength());
    }

    /**
     * Independent packet representation used to verify IDs, tokens, options and payloads on the
     * wire.
     */
    private static final class Frame {
        private final int type;
        private final int code;
        private final int mid;
        private final byte[] token;
        private final Map<Integer, List<byte[]>> options = new TreeMap<>();
        private byte[] payload = new byte[0];

        private Frame(int type, int code, int mid, byte[] token) {
            this.type = type;
            this.code = code;
            this.mid = mid;
            this.token = token;
        }

        static Frame from(String description) {
            String[] fields = description.split(",", -1);
            Frame frame =
                    new Frame(
                            Integer.parseInt(fields[0]),
                            Integer.parseInt(fields[1]),
                            Integer.parseInt(fields[2]),
                            new byte[] {(byte) Integer.parseInt(fields[3])});
            for (String path : fields[4].substring(1).split("/")) {
                frame.add(URI_PATH, text(path));
            }
            for (int i = 5; i <= 7; i++) {
                int value = Integer.parseInt(fields[i]);
                if (value >= 0) {
                    frame.option(i == 5 ? FORMAT : i == 6 ? BLOCK1 : SIZE1, value);
                }
            }
            frame.payload = Base64.getDecoder().decode(fields[8]);
            return frame;
        }

        private void add(int number, byte[] value) {
            options.computeIfAbsent(number, ignored -> new ArrayList<>()).add(value);
        }

        private void option(int number, int value) {
            byte[] encoded;
            if (value == 0) {
                encoded = new byte[0];
            } else if (value <= 255) {
                encoded = new byte[] {(byte) value};
            } else {
                encoded = new byte[] {(byte) (value >>> 8), (byte) value};
            }
            add(number, encoded);
        }

        private String path() {
            StringBuilder uri = new StringBuilder();
            for (byte[] segment : options.getOrDefault(URI_PATH, Collections.emptyList())) {
                uri.append('/').append(new String(segment, StandardCharsets.UTF_8));
            }
            return uri.toString();
        }

        private int value(int number) {
            List<byte[]> values = options.get(number);
            if (values == null) {
                return -1;
            }
            int value = 0;
            for (byte part : values.get(0)) {
                value = (value << 8) | (part & 255);
            }
            return value;
        }

        private byte[] encode() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            bytes.write(64 | (type << 4) | token.length);
            bytes.write(code);
            bytes.write(mid >>> 8);
            bytes.write(mid);
            bytes.write(token);
            int previous = 0;
            for (Map.Entry<Integer, List<byte[]>> option : options.entrySet()) {
                for (byte[] value : option.getValue()) {
                    int delta = option.getKey() - previous;
                    bytes.write((nibble(delta) << 4) | nibble(value.length));
                    extension(bytes, delta);
                    extension(bytes, value.length);
                    bytes.write(value);
                    previous = option.getKey();
                }
            }
            if (payload.length > 0) {
                bytes.write(255);
                bytes.write(payload);
            }
            return bytes.toByteArray();
        }

        private static Frame read(byte[] bytes, int length) throws IOException {
            if (length < 4 || ((bytes[0] & 255) >>> 6) != 1) {
                throw new IOException("invalid CoAP header");
            }
            int tokenLength = bytes[0] & 15;
            int[] pos = {4 + tokenLength};
            if (tokenLength > 8 || pos[0] > length) {
                throw new IOException("invalid CoAP token");
            }
            Frame frame =
                    new Frame(
                            (bytes[0] >>> 4) & 3,
                            bytes[1] & 255,
                            ((bytes[2] & 255) << 8) | (bytes[3] & 255),
                            Arrays.copyOfRange(bytes, 4, pos[0]));
            int optionNumber = 0;
            while (pos[0] < length) {
                int option = bytes[pos[0]++] & 255;
                if (option == 255) {
                    frame.payload = Arrays.copyOfRange(bytes, pos[0], length);
                    break;
                }
                optionNumber += extension(bytes, pos, length, option >>> 4);
                int size = extension(bytes, pos, length, option & 15);
                if (size > length - pos[0]) {
                    throw new IOException("invalid CoAP option length");
                }
                frame.add(optionNumber, Arrays.copyOfRange(bytes, pos[0], pos[0] + size));
                pos[0] += size;
            }
            return frame;
        }

        private static int nibble(int value) {
            return value < 13 ? value : value < 269 ? 13 : 14;
        }

        private static void extension(ByteArrayOutputStream target, int value) {
            if (value >= 269) {
                target.write((value - 269) >>> 8);
                target.write(value - 269);
            } else if (value >= 13) {
                target.write(value - 13);
            }
        }

        private static int extension(byte[] source, int[] pos, int length, int value)
                throws IOException {
            if (value < 13) {
                return value;
            }
            if (value == 13 && pos[0] < length) {
                return 13 + (source[pos[0]++] & 255);
            }
            if (value == 14 && pos[0] + 1 < length) {
                return 269 + ((source[pos[0]++] & 255) << 8) + (source[pos[0]++] & 255);
            }
            throw new IOException("invalid CoAP option extension");
        }
    }
}
