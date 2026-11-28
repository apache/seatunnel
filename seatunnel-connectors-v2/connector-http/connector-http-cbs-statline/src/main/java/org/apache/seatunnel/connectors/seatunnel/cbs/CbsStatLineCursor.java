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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns one streamed response between demanded observations. Parsing never runs on the engine's
 * collector thread, and every response has finite allocation and active-work budgets.
 */
final class CbsStatLineCursor implements AutoCloseable {
    private static final ObjectMapper TEMPLATE =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .enable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .maxStringLength(1048576)
                                                    .maxNumberLength(1000)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final int MAX_PAGES = 10000;
    private static final int MAX_LINK = 8192;
    private final CloseableHttpClient client;
    private final String dataset;
    private final ScheduledExecutorService timer;
    private final AtomicBoolean cancelled;
    private final AtomicReference<HttpGet> inFlight;
    private final Limits limits;
    private final Set<URI> visited = new HashSet<>();
    // This lifecycle monitor never overlaps the collector monitor or remote work.
    private final Object lifecycle = new Object();
    private boolean active;
    private boolean closed;
    private URI position;
    private URI next;
    private HttpGet request;
    private CloseableHttpResponse response;
    private ObjectMapper mapper;
    private JsonParser parser;
    private boolean started;
    private boolean inValues;
    private boolean sawValue;
    private boolean sawNext;
    private long activeNanos;

    CbsStatLineCursor(
            CloseableHttpClient client,
            URI start,
            String dataset,
            ScheduledExecutorService timer,
            AtomicBoolean cancelled,
            AtomicReference<HttpGet> inFlight,
            Limits limits) {
        this.client = client;
        this.position = start;
        this.dataset = dataset;
        this.timer = timer;
        this.cancelled = cancelled;
        this.inFlight = inFlight;
        this.limits = limits;
    }

    /**
     * Produces only the next requested observation. Leaving a prepared row parked never reads the
     * next observation, so a row cap retains its existing unread-suffix semantics.
     */
    SeaTunnelRow next() throws IOException {
        synchronized (lifecycle) {
            if (closed || cancelled.get()) {
                return null;
            }
            active = true;
        }
        try {
            while (!cancelled.get() && position != null) {
                if (request == null) {
                    beginPage();
                }
                long remaining = limits.activeNanos - activeNanos;
                if (remaining <= 0) {
                    throw deadline();
                }
                HttpGet owned = request;
                // Each operation owns its marker; an old canceled timer cannot abort a later one.
                AtomicBoolean running = new AtomicBoolean(true);
                long began = System.nanoTime();
                ScheduledFuture<?> expiry =
                        timer.schedule(
                                () -> {
                                    if (running.compareAndSet(true, false)) {
                                        owned.abort();
                                    }
                                },
                                remaining,
                                TimeUnit.NANOSECONDS);
                SeaTunnelRow row;
                try {
                    if (parser == null) {
                        openPage();
                    }
                    row = observation();
                } catch (IOException error) {
                    if (System.nanoTime() - began >= remaining) {
                        throw deadline();
                    }
                    throw error;
                } finally {
                    running.set(false);
                    expiry.cancel(false);
                    activeNanos += System.nanoTime() - began;
                }
                if (activeNanos >= limits.activeNanos) {
                    throw deadline();
                }
                if (cancelled.get()) {
                    return null;
                }
                if (row != null) {
                    return row;
                }
                closePage();
                position = next;
            }
            return null;
        } catch (IOException error) {
            IOException failure =
                    error instanceof JsonProcessingException
                            ? new IOException("Invalid CBS OData JSON response (body withheld)")
                            : error;
            try {
                closePage();
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        } finally {
            boolean release;
            synchronized (lifecycle) {
                active = false;
                release = closed || cancelled.get();
            }
            if (release) {
                closePage();
            }
        }
    }

    /**
     * Aborts before closing an unread entity. Active parsing cleans up on its own worker; a paused
     * cursor can be released immediately without concurrent Jackson parser access.
     */
    @Override
    public void close() throws IOException {
        boolean release;
        synchronized (lifecycle) {
            closed = true;
            release = !active;
        }
        if (release) {
            closePage();
        } else {
            HttpGet owned = inFlight.get();
            if (owned != null) {
                owned.abort();
            }
        }
    }

    private void beginPage() throws IOException {
        if (visited.size() == MAX_PAGES || !visited.add(position)) {
            throw new IOException("CBS OData continuation repeated or exceeded the page limit");
        }
        request = new HttpGet(position);
        request.setHeader("Accept", "application/json");
        inFlight.set(request);
        if (cancelled.get()) {
            request.abort();
            throw new IOException("CBS OData read canceled");
        }
        activeNanos = 0;
        next = null;
        started = false;
        inValues = false;
        sawValue = false;
        sawNext = false;
    }

    private void openPage() throws IOException {
        response = client.execute(request);
        int status = response.getStatusLine().getStatusCode();
        if (status != 200 || response.getEntity() == null) {
            throw new IOException("CBS OData Observations request returned HTTP " + status);
        }
        mapper = TEMPLATE.copy();
        parser =
                mapper.getFactory()
                        .createParser(
                                new BoundedInput(
                                        response.getEntity().getContent(),
                                        limits.pageBytes,
                                        limits.tokenBytes));
    }

    private SeaTunnelRow observation() throws IOException {
        if (!started) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("CBS OData response must be an object");
            }
            started = true;
        }
        while (true) {
            JsonToken token = parser.nextToken();
            if (inValues) {
                if (token == JsonToken.END_ARRAY) {
                    inValues = false;
                    continue;
                }
                if (token != JsonToken.START_OBJECT) {
                    throw new IOException("CBS OData observations must be objects");
                }
                return row(stageObservation());
            }
            if (token == JsonToken.END_OBJECT) {
                if (!sawValue || parser.nextToken() != null) {
                    throw new IOException("Invalid CBS OData response envelope");
                }
                return null;
            }
            if (token != JsonToken.FIELD_NAME) {
                throw new IOException("Invalid CBS OData response envelope");
            }
            String field = parser.currentName();
            token = parser.nextToken();
            if ("value".equals(field)) {
                if (sawValue || token != JsonToken.START_ARRAY) {
                    throw new IOException("CBS OData response must contain one value array");
                }
                sawValue = true;
                inValues = true;
            } else if ("@odata.nextLink".equals(field)) {
                if (sawNext || (token != JsonToken.VALUE_NULL && token != JsonToken.VALUE_STRING)) {
                    throw new IOException("CBS OData nextLink must be a URL string or null");
                }
                sawNext = true;
                if (token == JsonToken.VALUE_STRING) {
                    String link = parser.getText();
                    if (link.length() > MAX_LINK) {
                        throw new IOException("CBS OData continuation exceeds the URL limit");
                    }
                    next = CbsStatLineReader.continuation(position, link);
                }
            } else {
                parser.skipChildren();
            }
        }
    }

    /**
     * Stages one bounded observation before tree allocation. Native numeric text avoids a Double
     * conversion, while field order, duplicate last-value behavior and optional fields stay native.
     */
    private JsonNode stageObservation() throws IOException {
        LimitedOutput bytes = new LimitedOutput(limits.observationBytes);
        int depth = 0;
        int count = 0;
        try (JsonGenerator output = mapper.getFactory().createGenerator(bytes)) {
            do {
                if (++count > limits.observationTokens) {
                    throw new IOException("CBS OData observation exceeds the token limit");
                }
                JsonToken token = parser.currentToken();
                if (token == null) {
                    throw new IOException("Invalid CBS OData observation");
                }
                switch (token) {
                    case START_OBJECT:
                        output.writeStartObject();
                        depth++;
                        break;
                    case END_OBJECT:
                        output.writeEndObject();
                        depth--;
                        break;
                    case START_ARRAY:
                        output.writeStartArray();
                        depth++;
                        break;
                    case END_ARRAY:
                        output.writeEndArray();
                        depth--;
                        break;
                    case FIELD_NAME:
                        output.writeFieldName(parser.currentName());
                        break;
                    case VALUE_STRING:
                        output.writeString(parser.getText());
                        break;
                    case VALUE_NUMBER_INT:
                    case VALUE_NUMBER_FLOAT:
                        output.writeNumber(parser.getText());
                        break;
                    case VALUE_TRUE:
                        output.writeBoolean(true);
                        break;
                    case VALUE_FALSE:
                        output.writeBoolean(false);
                        break;
                    case VALUE_NULL:
                        output.writeNull();
                        break;
                    default:
                        throw new IOException("Invalid CBS OData observation token");
                }
                // Bound each occurrence before a large token list/tree can be constructed.
                output.flush();
            } while (depth > 0 && parser.nextToken() != null);
        }
        if (depth != 0) {
            throw new IOException("Incomplete CBS OData observation");
        }
        return mapper.readTree(bytes.bytes.toByteArray());
    }

    private SeaTunnelRow row(JsonNode observation) throws IOException {
        JsonNode id = observation.path("Id");
        JsonNode measure = observation.path("Measure");
        JsonNode value = observation.path("Value");
        if (!id.isIntegralNumber()
                || !id.canConvertToLong()
                || !measure.isTextual()
                || (!value.isNull() && !value.isMissingNode() && !value.isNumber())) {
            throw new IOException("Invalid CBS OData observation Id, Measure, or Value");
        }
        return new SeaTunnelRow(
                new Object[] {
                    dataset,
                    id.longValue(),
                    measure.textValue(),
                    value.isNumber() ? value.asText() : null,
                    optionalText(observation, "StringValue"),
                    optionalText(observation, "ValueAttribute"),
                    observation.toString()
                });
    }

    private static String optionalText(JsonNode observation, String field) throws IOException {
        JsonNode value = observation.path(field);
        if (value.isNull() || value.isMissingNode()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new IOException("CBS OData observation field " + field + " must be text or null");
        }
        return value.textValue();
    }

    private IOException deadline() {
        return new IOException("CBS OData active fetch/decode deadline exceeded");
    }

    private void closePage() throws IOException {
        HttpGet owned;
        CloseableHttpResponse oldResponse;
        JsonParser oldParser;
        synchronized (lifecycle) {
            // Only one closer detaches paused or failed resources; native close happens off-lock.
            owned = request;
            request = null;
            oldResponse = response;
            oldParser = parser;
            parser = null;
            response = null;
            mapper = null;
        }
        try (CloseableHttpResponse closeResponse = oldResponse;
                JsonParser closeParser = oldParser) {
            if (owned != null) {
                try {
                    owned.abort();
                } finally {
                    inFlight.compareAndSet(owned, null);
                }
            }
        }
    }

    /**
     * Contains fixed production envelopes; small injected limits let real HTTP component cases
     * reject modest payloads without creating production-scale allocation or timing probes.
     */
    static final class Limits {
        final long pageBytes;
        final int tokenBytes;
        final int observationBytes;
        final int observationTokens;
        final long activeNanos;

        Limits(
                long pageBytes,
                int tokenBytes,
                int observationBytes,
                int observationTokens,
                long activeMillis) {
            this.pageBytes = pageBytes;
            this.tokenBytes = tokenBytes;
            this.observationBytes = observationBytes;
            this.observationTokens = observationTokens;
            this.activeNanos = TimeUnit.MILLISECONDS.toNanos(activeMillis);
        }

        static Limits defaults() {
            return new Limits(64L * 1024 * 1024, 1024 * 1024, 4 * 1024 * 1024, 200000, 120000);
        }
    }

    /**
     * Bounds one encoded observation before materializing its tree. Flushes during staging make the
     * byte gate effective before traversal of later fields, not only after serialization.
     */
    private static final class LimitedOutput extends OutputStream {
        private final int limit;
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        private LimitedOutput(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            check(1);
            bytes.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            check(length);
            bytes.write(value, offset, length);
        }

        private void check(int length) throws IOException {
            if (length > limit - bytes.size()) {
                throw new IOException("CBS OData observation exceeds the byte limit");
            }
        }
    }

    /**
     * Caps consumed decompressed bytes and quoted token bytes before native parser allocation.
     * Alignment detection leaves UTF-8/16/32 bytes and BOMs unchanged for Jackson's own decoder.
     */
    private static final class BoundedInput extends InputStream {
        private final InputStream input;
        private final long byteLimit;
        private final int tokenLimit;
        private final byte[] prefix = new byte[4];
        private long consumed;
        private int prefixLength;
        private int width;
        private boolean littleEndian;
        private int unit;
        private int unitBytes;
        private boolean quoted;
        private boolean escaped;
        private long quotedBytes;

        private BoundedInput(InputStream input, long byteLimit, int tokenLimit) {
            this.input = input;
            this.byteLimit = byteLimit;
            this.tokenLimit = tokenLimit;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (consumed >= byteLimit) {
                if (input.read() < 0) {
                    return -1;
                }
                throw new IOException("CBS OData response exceeds the byte limit");
            }
            int size = (int) Math.min(Math.min(length, 8192), byteLimit - consumed);
            int count = input.read(target, offset, size);
            if (count < 0) {
                return -1;
            }
            consumed += count;
            for (int i = offset; i < offset + count; i++) {
                accept(target[i] & 255);
            }
            return count;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }

        private void accept(int value) throws IOException {
            if (width == 0) {
                prefix[prefixLength++] = (byte) value;
                if (prefixLength == 4) {
                    detect();
                    for (byte b : prefix) {
                        unit(b & 255);
                    }
                }
            } else {
                unit(value);
            }
        }

        private void detect() {
            int quad =
                    (prefix[0] & 255) << 24
                            | (prefix[1] & 255) << 16
                            | (prefix[2] & 255) << 8
                            | prefix[3] & 255;
            if (quad == 0x0000FEFF || (quad >>> 8) == 0) {
                width = 4;
            } else if (quad == 0xFFFE0000 || (quad & 0xFFFFFF) == 0) {
                width = 4;
                littleEndian = true;
            } else if ((quad >>> 16) == 0xFEFF || ((quad >>> 24) == 0)) {
                width = 2;
            } else if ((quad >>> 16) == 0xFFFE || (quad & 0x00FF0000) == 0) {
                width = 2;
                littleEndian = true;
            } else {
                width = 1;
            }
        }

        private void unit(int value) throws IOException {
            unit = littleEndian ? unit | value << (unitBytes * 8) : unit << 8 | value;
            if (++unitBytes != width) {
                return;
            }
            int ch = unit;
            unit = 0;
            unitBytes = 0;
            if (!quoted) {
                if (ch == '"') {
                    quoted = true;
                    quotedBytes = width;
                }
                return;
            }
            quotedBytes += width;
            if (quotedBytes > tokenLimit) {
                throw new IOException("CBS OData JSON string or field name exceeds the byte limit");
            }
            if (escaped) {
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == '"') {
                quoted = false;
            }
        }
    }
}
