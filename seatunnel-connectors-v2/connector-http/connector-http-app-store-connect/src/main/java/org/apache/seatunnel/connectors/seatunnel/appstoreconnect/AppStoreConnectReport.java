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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PushbackReader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

/**
 * Validated report staging and logical TSV row iteration.
 *
 * <p>Wire files and decoded data have separate limits. The complete stream is verified before
 * exposing a row, and files are local to one live attempt rather than part of checkpoint state.
 */
final class AppStoreConnectReport implements AutoCloseable {
    final String digest;
    final long size;
    private final Path decoded;
    private final AppStoreConnectResource resource;
    private final int rowLimit;
    private Tsv input;

    private AppStoreConnectReport(
            Path decoded,
            String digest,
            long size,
            AppStoreConnectResource resource,
            int rowLimit) {
        this.decoded = decoded;
        this.digest = digest;
        this.size = size;
        this.resource = resource;
        this.rowLimit = rowLimit;
    }

    /**
     * Verifies the gzip file, the segment MD5 and length where available, and every TSV record.
     * This prevents a corrupt trailer or late malformed row from being written to the sink.
     */
    static AppStoreConnectReport prepare(
            AppStoreConnectHttp http,
            AppStoreConnectConfig config,
            AppStoreConnectResource resource,
            InputStream body,
            boolean contentGzip,
            AppStoreConnectApi.Segment segment,
            long deadline)
            throws IOException, InterruptedException {
        Path wire = null;
        Path representation = null;
        Path decoded = null;
        try {
            wire = Files.createTempFile("seatunnel-appstore-wire-", ".gz");
            String md5 =
                    copy(
                            http,
                            resource,
                            body,
                            wire,
                            config.wireLimit,
                            messageDigest("MD5"),
                            deadline);
            Path source = wire;
            if (contentGzip && (segment == null || !matches(segment, wire, md5))) {
                representation = Files.createTempFile("seatunnel-appstore-representation-", ".gz");
                try (InputStream file = Files.newInputStream(wire);
                        InputStream inflated = new GZIPInputStream(file)) {
                    md5 =
                            copy(
                                    http,
                                    resource,
                                    inflated,
                                    representation,
                                    config.decodedLimit,
                                    messageDigest("MD5"),
                                    deadline);
                }
                source = representation;
            }
            if (!matches(segment, source, md5)) {
                throw resource.failure("analytics segment size or MD5 does not match metadata");
            }

            decoded = Files.createTempFile("seatunnel-appstore-rows-", ".tsv");
            String fingerprint;
            try (BufferedInputStream file = new BufferedInputStream(Files.newInputStream(source))) {
                if (isGzip(file)) {
                    try (InputStream inflated = new GZIPInputStream(file)) {
                        fingerprint =
                                copy(
                                        http,
                                        resource,
                                        inflated,
                                        decoded,
                                        config.decodedLimit,
                                        AppStoreConnectConfig.sha256(),
                                        deadline);
                    }
                } else if (source == representation && segment == null) {
                    // HTTP Content-Encoding may describe the report's only gzip layer.
                    fingerprint =
                            copy(
                                    http,
                                    resource,
                                    file,
                                    decoded,
                                    config.decodedLimit,
                                    AppStoreConnectConfig.sha256(),
                                    deadline);
                } else {
                    throw resource.failure("report does not contain an expected gzip stream");
                }
            }
            long rows = 0;
            try (Tsv validation = new Tsv(decoded, resource, config.rowLimit)) {
                while (validation.next() != null) {
                    ++rows;
                    http.remaining(resource, deadline);
                }
            }
            AppStoreConnectReport prepared =
                    new AppStoreConnectReport(
                            decoded, fingerprint, rows, resource, config.rowLimit);
            decoded = null;
            return prepared;
        } catch (IOException ex) {
            http.remaining(resource, deadline);
            throw resource.failure(
                    "compressed report is truncated, malformed, or could not be staged");
        } finally {
            delete(wire);
            delete(representation);
            delete(decoded);
        }
    }

    private static boolean matches(AppStoreConnectApi.Segment segment, Path path, String hash)
            throws IOException {
        return segment == null || Files.size(path) == segment.size && hash.equals(segment.checksum);
    }

    private static boolean isGzip(BufferedInputStream file) throws IOException {
        file.mark(2);
        boolean gzip = file.read() == 0x1f && file.read() == 0x8b;
        file.reset();
        return gzip;
    }

    private static MessageDigest messageDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("AppStoreConnect cannot verify a report checksum");
        }
    }

    private static String copy(
            AppStoreConnectHttp http,
            AppStoreConnectResource resource,
            InputStream source,
            Path destination,
            long max,
            MessageDigest digest,
            long deadline)
            throws IOException, InterruptedException {
        byte[] block = new byte[8192];
        try (OutputStream file = Files.newOutputStream(destination)) {
            InputStream bounded = new LimitedStream(source, max, resource, "report byte limit");
            for (int count; (count = bounded.read(block)) != -1; ) {
                http.remaining(resource, deadline);
                file.write(block, 0, count);
                digest.update(block, 0, count);
            }
        }
        return AppStoreConnectConfig.hex(digest.digest());
    }

    /**
     * Reopens the verified spool and skips only rows already acknowledged in checkpoint state.
     *
     * <p>The staging worker has checked all rows and the digest before applying this offset.
     */
    void seek(AppStoreConnectHttp http, long offset, long deadline) throws InterruptedException {
        if (offset < 0 || offset > size) {
            throw resource.failure("saved report offset is outside the verified file");
        }
        try {
            input = new Tsv(decoded, resource, rowLimit);
            for (long i = 0; i < offset; i++) {
                if (input.next() == null) {
                    throw resource.failure("verified report changed before resume");
                }
                if ((i & 255) == 0) {
                    http.remaining(resource, deadline);
                }
            }
        } catch (IOException ex) {
            throw resource.failure("could not reopen the verified report");
        }
    }

    ObjectNode next(ObjectMapper json) {
        try {
            List<String> cells = input.next();
            if (cells == null) {
                throw resource.failure("verified report ended unexpectedly");
            }
            ObjectNode row = json.createObjectNode();
            for (int i = 0; i < cells.size(); i++) {
                row.put(input.columns.get(i), cells.get(i));
            }
            return row;
        } catch (IOException ex) {
            throw resource.failure("could not read the verified report");
        }
    }

    @Override
    public void close() {
        try {
            if (input != null) {
                input.close();
            }
        } catch (IOException ignored) {
            // The spool is being discarded.
        } finally {
            delete(decoded);
        }
    }

    private static void delete(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // Do not expose a host filesystem path.
            }
        }
    }

    /**
     * Enforces separate limits for transport bytes and decompressed bytes.
     *
     * <p>Bounds are checked before the caller can retain a block beyond the configured limit.
     */
    static final class LimitedStream extends FilterInputStream {
        private final long maximum;
        private final AppStoreConnectResource resource;
        private final String name;
        private long count;

        LimitedStream(
                InputStream source, long maximum, AppStoreConnectResource resource, String name) {
            super(source);
            this.maximum = maximum;
            this.resource = resource;
            this.name = name;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value != -1) {
                check(1);
            }
            return value;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            int read = in.read(target, offset, length);
            if (read > 0) {
                check(read);
            }
            return read;
        }

        private void check(int read) {
            count += read;
            if (count > maximum) {
                throw resource.failure(name + " exceeded");
            }
        }
    }

    /**
     * Prevents close from draining an untrusted HTTP entity after a quota is hit.
     *
     * <p>The wrapping HTTPS exchange owns and closes the network socket.
     */
    static final class NonClosingStream extends FilterInputStream {
        NonClosingStream(InputStream source) {
            super(source);
        }

        @Override
        public void close() {
            // The HTTPS exchange owns this stream.
        }
    }

    /**
     * Strict UTF-8 TSV reader that preserves quoted tabs, line breaks and numeric text.
     *
     * <p>Width and header uniqueness are verified against the report's own columns so newly added
     * columns are preserved without relying on physical column order.
     */
    private static final class Tsv implements AutoCloseable {
        private final PushbackReader input;
        private final AppStoreConnectResource resource;
        private final int maxRow;
        private final List<String> columns;

        Tsv(Path path, AppStoreConnectResource resource, int maxRow) throws IOException {
            this.resource = resource;
            this.maxRow = maxRow;
            input =
                    new PushbackReader(
                            new BufferedReader(
                                    new InputStreamReader(
                                            Files.newInputStream(path),
                                            StandardCharsets.UTF_8
                                                    .newDecoder()
                                                    .onMalformedInput(CodingErrorAction.REPORT)
                                                    .onUnmappableCharacter(
                                                            CodingErrorAction.REPORT))),
                            1);
            try {
                int first = input.read();
                if (first != -1 && first != '\uFEFF') {
                    input.unread(first);
                }
                List<String> headers = record();
                if (headers == null
                        || headers.size() < 2
                        || headers.stream().anyMatch(String::isBlank)
                        || headers.size() > 1024
                        || resource.terminal == AppStoreConnectResource.Stage.DOWNLOAD
                                && !headers.contains("Date")) {
                    throw resource.failure("report has an invalid TSV header");
                }
                HashSet<String> names = new HashSet<>();
                for (String header : headers) {
                    if (!names.add(header.toLowerCase(Locale.ROOT))) {
                        throw resource.failure("report has duplicate TSV column names");
                    }
                }
                columns = headers;
            } catch (IOException | RuntimeException ex) {
                input.close();
                throw ex;
            }
        }

        List<String> next() throws IOException {
            List<String> row;
            while ((row = record()) != null) {
                if (row.size() == columns.size()) {
                    return row;
                }
                if (row.size() != 1 || !row.get(0).isEmpty()) {
                    throw resource.failure("TSV row width differs from the header");
                }
            }
            return null;
        }

        /**
         * Separates record framing from field content, including quoted multiline cells.
         *
         * <p>The parser counts input characters before growing a buffer and tracks closing quotes
         * separately.
         */
        private List<String> record() throws IOException {
            List<String> cells = new ArrayList<>();
            StringBuilder value = new StringBuilder();
            boolean quoted = false;
            boolean afterQuote = false;
            boolean started = false;
            int length = 0;
            for (int c; (c = input.read()) != -1; ) {
                if (++length > maxRow) {
                    throw resource.failure("TSV row exceeds max_report_row_chars");
                }
                started = true;
                if (quoted) {
                    if (c == '"') {
                        quoted = false;
                        afterQuote = true;
                    } else {
                        value.append((char) c);
                    }
                } else if (afterQuote && c == '"') {
                    quoted = true;
                    afterQuote = false;
                    value.append('"');
                } else if (c == '\t' || c == '\r' || c == '\n') {
                    cells.add(value.toString());
                    if (c != '\t') {
                        if (c == '\r') {
                            int next = input.read();
                            if (next >= 0 && next != '\n') {
                                input.unread(next);
                            }
                        }
                        return cells;
                    }
                    if (cells.size() >= 1024) {
                        throw resource.failure("TSV column limit exceeded");
                    }
                    value.setLength(0);
                    afterQuote = false;
                } else if (afterQuote) {
                    throw resource.failure("TSV contains characters after a quoted field");
                } else if (c == '"' && value.length() == 0) {
                    quoted = true;
                } else {
                    value.append((char) c);
                }
            }
            if (quoted) {
                throw resource.failure("TSV contains an unterminated quoted field");
            }
            if (!started) {
                return null;
            }
            cells.add(value.toString());
            return cells;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
