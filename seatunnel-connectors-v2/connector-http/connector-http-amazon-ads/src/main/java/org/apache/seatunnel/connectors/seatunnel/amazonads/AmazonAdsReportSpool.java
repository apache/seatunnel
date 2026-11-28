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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonToken;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.zip.GZIPInputStream;

/**
 * Verifies an entire bounded gzip report before exposing rows through a private temporary file. A
 * saved byte offset can only be reused after the same row digest has been reconstructed.
 */
final class AmazonAdsReportSpool implements AutoCloseable {
    final int rows;
    final long bytes;
    final String digest;
    private final Path file;
    private final AmazonAdsResource resource;
    private final int rowLimit;
    private RandomAccessFile reader;

    private AmazonAdsReportSpool(
            AmazonAdsResource resource,
            int rowLimit,
            Path file,
            int rows,
            long bytes,
            String digest) {
        this.resource = resource;
        this.rowLimit = rowLimit;
        this.file = file;
        this.rows = rows;
        this.bytes = bytes;
        this.digest = digest;
    }

    static AmazonAdsReportSpool read(
            AmazonAdsConfig config,
            AmazonAdsResource resource,
            AmazonAdsPartition.Window window,
            String profile,
            ObjectMapper json,
            InputStream entity,
            String encoding,
            Guard guard)
            throws IOException, InterruptedException {
        Path staging = null;
        try {
            staging = Files.createTempFile("seatunnel-amazonads-", ".rows");
            InputStream decoded = payload(config, resource, entity, encoding);
            MessageDigest checksum = AmazonAdsRecord.newDigest();
            int count = 0;
            long bytes = 0;
            try (JsonParser parser = json.getFactory().createParser(decoded);
                    DataOutputStream output =
                            new DataOutputStream(
                                    new BufferedOutputStream(Files.newOutputStream(staging)))) {
                if (parser.nextToken() != JsonToken.START_ARRAY) {
                    throw resource.fail("report payload must be a JSON array");
                }
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    guard.check();
                    if (++count > config.maxRows) {
                        throw resource.fail("report exceeds max_report_rows");
                    }
                    byte[] row = record(config, resource, window, profile, json, parser, guard);
                    bytes += Integer.BYTES + (long) row.length;
                    if (bytes > config.maxReport) {
                        throw resource.fail("decoded report spool exceeds max_report_bytes");
                    }
                    output.writeInt(row.length);
                    output.write(row);
                    AmazonAdsRecord.add(checksum, row);
                }
                if (parser.nextToken() != null) {
                    throw resource.fail("report contains unexpected trailing JSON");
                }
                guard.check();
            }
            return new AmazonAdsReportSpool(
                    resource,
                    config.maxResponse,
                    staging,
                    count,
                    bytes,
                    AmazonAdsRecord.finish(checksum));
        } catch (IOException | InterruptedException | RuntimeException ex) {
            remove(staging);
            throw ex;
        }
    }

    private static InputStream payload(
            AmazonAdsConfig config, AmazonAdsResource resource, InputStream raw, String encoding)
            throws IOException {
        boolean transferGzip = "gzip".equalsIgnoreCase(encoding);
        if (encoding != null && !transferGzip && !"identity".equalsIgnoreCase(encoding)) {
            throw resource.fail("unsupported report Content-Encoding");
        }
        BufferedInputStream wire =
                new BufferedInputStream(
                        new BoundedInput(
                                raw,
                                config.maxDownload,
                                resource,
                                "compressed report exceeds max_download_bytes"));
        if (!startsGzip(wire)) {
            throw resource.fail("report download is not gzip");
        }
        BufferedInputStream first =
                new BufferedInputStream(
                        new BoundedInput(
                                new GZIPInputStream(wire),
                                config.maxReport,
                                resource,
                                "decoded report exceeds max_report_bytes"));
        if (transferGzip && startsGzip(first)) {
            return new BoundedInput(
                    new GZIPInputStream(first),
                    config.maxReport,
                    resource,
                    "decoded report exceeds max_report_bytes");
        }
        return first;
    }

    private static boolean startsGzip(BufferedInputStream bytes) throws IOException {
        bytes.mark(2);
        int first = bytes.read();
        int second = bytes.read();
        bytes.reset();
        return first == 0x1f && second == 0x8b;
    }

    private static byte[] record(
            AmazonAdsConfig config,
            AmazonAdsResource resource,
            AmazonAdsPartition.Window window,
            String profile,
            ObjectMapper json,
            JsonParser parser,
            Guard guard)
            throws IOException, InterruptedException {
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw resource.fail("report contains a non-object record");
        }
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (JsonGenerator output =
                json.getFactory()
                        .createGenerator(new BoundedOutput(raw, config.maxResponse, resource))) {
            int depth = 0;
            int tokens = 0;
            do {
                guard.check();
                JsonToken current = parser.currentToken();
                if (current == null || ++tokens > 100000) {
                    throw resource.fail("report row is truncated or exceeds the JSON token limit");
                }
                if (current == JsonToken.START_OBJECT || current == JsonToken.START_ARRAY) {
                    depth++;
                } else if (current == JsonToken.END_OBJECT || current == JsonToken.END_ARRAY) {
                    depth--;
                }
                if (current == JsonToken.VALUE_NUMBER_FLOAT) {
                    output.writeNumber(parser.getDecimalValue());
                } else if (current == JsonToken.VALUE_NUMBER_INT) {
                    output.writeNumber(parser.getBigIntegerValue());
                } else {
                    output.copyCurrentEvent(parser);
                }
                if (depth > 0) {
                    parser.nextToken();
                }
            } while (depth > 0);
        }
        ObjectNode value = (ObjectNode) json.readTree(raw.toByteArray());
        if ("DAILY".equals(AmazonAdsReportCatalog.forResource(resource).timeUnit)) {
            JsonNode date = value.path("date");
            try {
                if (!date.isTextual() || !date.textValue().matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                    throw resource.fail("daily report did not return a valid date");
                }
                LocalDate day = LocalDate.parse(date.textValue());
                if (day.isBefore(window.from) || day.isAfter(window.to)) {
                    throw resource.fail("daily report date is outside the requested window");
                }
            } catch (DateTimeParseException e) {
                throw resource.fail("daily report did not return a valid date");
            }
        }
        value.put("profileId", profile);
        value.put("reportDate", window.to.toString());
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (JsonGenerator output =
                json.getFactory()
                        .createGenerator(new BoundedOutput(result, config.maxResponse, resource))) {
            json.writeTree(output, value);
        }
        return result.toByteArray();
    }

    synchronized void seek(long offset) {
        if (offset < 0 || offset > bytes || reader != null) {
            throw resource.fail("saved report offset is invalid");
        }
        try {
            reader = new RandomAccessFile(file.toFile(), "r");
            reader.seek(offset);
        } catch (IOException ex) {
            throw resource.fail("could not open the decoded report spool");
        }
    }

    synchronized byte[] nextRow() {
        try {
            int length = reader.readInt();
            if (length < 1 || length > rowLimit) {
                throw resource.fail("report spool row exceeds its bound");
            }
            byte[] row = new byte[length];
            reader.readFully(row);
            return row;
        } catch (IOException ex) {
            throw resource.fail("could not read the decoded report spool");
        }
    }

    synchronized long offset() {
        try {
            return reader.getFilePointer();
        } catch (IOException ex) {
            throw resource.fail("could not read the report spool position");
        }
    }

    @Override
    public synchronized void close() {
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException ignored) {
                // Release the private staging file regardless of the stream state.
            } finally {
                reader = null;
            }
        }
        remove(file);
    }

    private static void remove(Path file) {
        if (file != null) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ignored) {
                file.toFile().deleteOnExit();
            }
        }
    }

    /**
     * Allows the transport to enforce its elapsed deadline while decoding a large result.
     *
     * <p>The time check applies between parser tokens as well as while the socket remains active.
     */
    interface Guard {
        long check() throws InterruptedException;
    }

    /**
     * Bounds one stream layer before bytes can advance to a decompressor or parser.
     *
     * <p>A compressed transfer and the expanded file are counted independently.
     */
    private static final class BoundedInput extends FilterInputStream {
        private final AmazonAdsResource resource;
        private final String message;
        private long remaining;

        BoundedInput(InputStream source, int maximum, AmazonAdsResource resource, String message) {
            super(source);
            remaining = maximum;
            this.resource = resource;
            this.message = message;
        }

        @Override
        public int read() throws IOException {
            int result = in.read();
            if (result != -1) {
                account(1);
            }
            return result;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            int read = in.read(target, offset, (int) Math.min(length, remaining + 1));
            if (read > 0) {
                account(read);
            }
            return read;
        }

        private void account(int count) {
            if (count > remaining) {
                throw resource.fail(message);
            }
            remaining -= count;
        }
    }

    /**
     * Rejects an oversized individual JSON object before its buffer can grow beyond the row budget.
     *
     * <p>The file limit and the per-record memory limit are enforced separately.
     */
    private static final class BoundedOutput extends OutputStream {
        private final ByteArrayOutputStream target;
        private final int maximum;
        private final AmazonAdsResource resource;

        BoundedOutput(ByteArrayOutputStream target, int maximum, AmazonAdsResource resource) {
            this.target = target;
            this.maximum = maximum;
            this.resource = resource;
        }

        @Override
        public void write(int value) {
            ensure(1);
            target.write(value);
        }

        @Override
        public void write(byte[] part, int offset, int length) {
            ensure(length);
            target.write(part, offset, length);
        }

        private void ensure(int length) {
            if (length > maximum - target.size()) {
                throw resource.fail("report row exceeds max_response_bytes");
            }
        }
    }
}
