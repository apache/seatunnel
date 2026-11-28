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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/** Validates async transport and codec settings before either direction starts I/O. */
class CoapSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    static final int EXCHANGE_LIFETIME_SECONDS = 247;
    static final int ABSOLUTE_BODY_LIMIT = 1048576;
    static final int DATAGRAM_BYTES = 1473;
    static final int DEDUP_PER_PEER = 64;
    static final int PROTOCOL_LIMIT_PER_WORKER = 64;
    static final int OUTBOUND_DATAGRAMS = 256;
    static final int MAX_ROWS = 1024;
    private static final long BUFFER_BUDGET = 64L * 1024 * 1024;

    final ReadonlyConfig options;
    final CoapOptions.Format format;
    final int maxBytes;
    final int blockTimeout;
    final String csvDelimiter;
    final String avroSchema;
    final String protoSchema;
    final String protoMessage;

    CoapSettings(ReadonlyConfig options, boolean bodyEnabled) {
        this.options = options;
        this.format = options.get(CoapOptions.FORMAT);
        this.maxBytes = bounded(options, CoapOptions.BODY_SIZE, 16, ABSOLUTE_BODY_LIMIT);
        this.blockTimeout = bounded(options, CoapOptions.BLOCK_TIMEOUT, 500, 120000);
        this.csvDelimiter = options.get(CoapOptions.CSV_DELIMITER);
        this.avroSchema = options.get(CoapOptions.AVRO_SCHEMA);
        this.protoSchema = options.get(CoapOptions.PROTOBUF_SCHEMA);
        this.protoMessage = options.get(CoapOptions.PROTOBUF_MESSAGE);
        if (format == CoapOptions.Format.CSV
                && bodyEnabled
                && (csvDelimiter.length() != 1
                        || csvDelimiter.charAt(0) < 32
                        || csvDelimiter.equals("\""))) {
            throw invalid("csv_delimiter must be one printable non-quote character");
        }
        if (format == CoapOptions.Format.PROTOBUF
                && bodyEnabled
                && (protoSchema == null
                        || protoSchema.trim().isEmpty()
                        || protoMessage == null
                        || protoMessage.trim().isEmpty())) {
            throw invalid("protobuf_schema and protobuf_message_name are required for protobuf");
        }
    }

    /** Validates one listener's routing, parser settings and total input buffer capacity. */
    static final class Listener extends CoapSettings {
        private static final long serialVersionUID = 1L;
        final InetAddress address;
        final int port;
        final List<String> path;
        final int queue;
        final int peers;
        final int concurrency;
        final long limit;
        final boolean typed;
        final CoapOptions.DatagramMode datagramMode;
        final int netflowTemplates;
        final int netflowTemplateTimeout;

        Listener(ReadonlyConfig options) {
            super(options, true);
            address = bindAddress(options.get(CoapOptions.BIND_HOST));
            port = bounded(options, CoapOptions.PORT, 1, 65535);
            path = segments(options.get(CoapOptions.RESOURCE));
            queue = bounded(options, CoapOptions.QUEUE, 1, 8192);
            peers = bounded(options, CoapOptions.PEERS, 1, 1024);
            concurrency = bounded(options, CoapOptions.CONCURRENCY, 1, 32);
            limit = options.get(CoapOptions.LIMIT);
            if (limit < 0) {
                throw invalid("stop_after_messages must be nonnegative");
            }
            long inFlightBodies = 2L * peers + 2L * concurrency;
            long decodedRows = (queue + (long) concurrency) * decodedBudget(maxBytes);
            long datagrams =
                    (long) peers * DEDUP_PER_PEER
                            + (long) concurrency * PROTOCOL_LIMIT_PER_WORKER
                            + OUTBOUND_DATAGRAMS;
            datagramMode = options.get(CoapOptions.DATAGRAM_MODE);
            netflowTemplates = bounded(options, CoapOptions.NETFLOW_TEMPLATES, 1, 1024);
            netflowTemplateTimeout =
                    bounded(options, CoapOptions.NETFLOW_TEMPLATE_TIMEOUT, 1000, 3600000);
            long parserBytes =
                    format == CoapOptions.Format.DATAGRAM
                                    && datagramMode == CoapOptions.DatagramMode.NETFLOW
                            ? (long) netflowTemplates * 2 * CoapNetflow.TEMPLATE_BUDGET_BYTES
                            : 0;
            if (inFlightBodies * maxBytes + decodedRows + datagrams * DATAGRAM_BYTES + parserBytes
                    > BUFFER_BUDGET) {
                throw invalid(
                        "queue, peer, protocol and payload settings exceed the 64 MiB buffer budget");
            }
            typed = options.getOptional(ConnectorCommonOptions.SCHEMA).isPresent();
            if ((format == CoapOptions.Format.CSV
                            || format == CoapOptions.Format.AVRO
                            || format == CoapOptions.Format.PROTOBUF)
                    && !typed) {
                throw invalid("schema is required for csv, avro and protobuf sources");
            }
            if (typed
                    && (format == CoapOptions.Format.TEXT
                            || format == CoapOptions.Format.BINARY
                            || (format == CoapOptions.Format.DATAGRAM
                                    && datagramMode == CoapOptions.DatagramMode.RAW_DATA))) {
                throw invalid(
                        "text, binary and raw datagram sources always provide the payload column");
            }
        }
    }

    /** Validates the destination and request representation before writing any records. */
    static final class Sender extends CoapSettings {
        private static final long serialVersionUID = 1L;
        final URI uri;
        final CoapOptions.Method method;
        final CoapOptions.RequestType requestType;
        final int timeout;
        final String payloadField;

        Sender(ReadonlyConfig options) {
            super(options, hasBody(options.get(CoapOptions.METHOD)));
            uri = uri(options.get(CoapOptions.URI));
            method = options.get(CoapOptions.METHOD);
            requestType = options.get(CoapOptions.REQUEST_TYPE);
            timeout = bounded(options, CoapOptions.TIMEOUT, 1, 120000);
            payloadField = options.get(CoapOptions.PAYLOAD_FIELD);
            if (hasBody()
                    && (format == CoapOptions.Format.XML
                            || format == CoapOptions.Format.LOG
                            || format == CoapOptions.Format.DATAGRAM)) {
                throw invalid("xml, log and datagram are source formats");
            }
        }

        boolean hasBody() {
            return hasBody(method);
        }

        private static boolean hasBody(CoapOptions.Method method) {
            return method == CoapOptions.Method.POST || method == CoapOptions.Method.PUT;
        }
    }

    private static URI uri(String value) {
        try {
            URI uri = new URI(value);
            if (!"coap".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || uri.getRawUserInfo() != null
                    || uri.getRawFragment() != null
                    || uri.toASCIIString().length() > 2048
                    || uri.getHost().indexOf('%') >= 0) {
                throw invalid(
                        "url must be an absolute coap:// UDP URI with no credentials or fragment");
            }
            // Californium matches lowercase schemes.
            return new URI(uri.toASCIIString().replaceFirst("^[^:]+", "coap"));
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid(
                    "url must be an absolute coap:// UDP URI with no credentials or fragment");
        }
    }

    private static InetAddress bindAddress(String input) {
        if (!input.matches("[0-9]+(\\.[0-9]+){3}|(?=.*:)[0-9a-fA-F:]+")) {
            throw invalid("bind_host must be a numeric IPv4 or IPv6 address");
        }
        try {
            InetAddress bind = InetAddress.getByName(input);
            if (bind.isMulticastAddress()) {
                throw invalid("bind_host must not be multicast");
            }
            return bind;
        } catch (UnknownHostException e) {
            throw invalid("bind_host must be a numeric IPv4 or IPv6 address");
        }
    }

    private static List<String> segments(String value) {
        if (!value.startsWith("/") || value.length() > 1024) {
            throw invalid("resource_path must be an absolute path with one or more segments");
        }
        List<String> segments = Arrays.asList(value.substring(1).split("/", -1));
        if (segments.size() > 16
                || segments.stream()
                        .anyMatch(
                                segment ->
                                        !segment.matches("[A-Za-z0-9_.~-]{1,64}")
                                                || segment.equals(".")
                                                || segment.equals(".."))) {
            throw invalid("resource_path segments must contain 1 to 64 unreserved URI characters");
        }
        return segments;
    }

    private static int bounded(ReadonlyConfig config, Option<Integer> key, int lower, int upper) {
        int value = config.get(key);
        if (value < lower || value > upper) {
            throw invalid(key.key() + " must be between " + lower + " and " + upper);
        }
        return value;
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("CoAP: " + message);
    }

    static int decodedBudget(int maxBytes) {
        return Math.max(256, maxBytes * 2);
    }
}
