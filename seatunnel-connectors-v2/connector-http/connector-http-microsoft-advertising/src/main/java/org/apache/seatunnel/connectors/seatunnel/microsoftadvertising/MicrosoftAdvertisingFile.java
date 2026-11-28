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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Kind;
import org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingCatalog.Resource;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

/**
 * Bounded local CSV spool for one completed provider job.
 *
 * <p>Archive validation and a full logical CSV pass precede row publication. On restore, the same
 * job is downloaded again, hashed and positioned before any remaining row is emitted.
 */
final class MicrosoftAdvertisingFile implements AutoCloseable {
    private static final DateTimeFormatter HOUR =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssxxx", Locale.ROOT);
    private static final DateTimeFormatter BULK_OUTPUT =
            new DateTimeFormatterBuilder()
                    .appendPattern("uuuu-MM-dd'T'HH:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
                    .appendOffset("+HH:MM", "+00:00")
                    .toFormatter(Locale.ROOT);
    private static final DateTimeFormatter BULK_DATE =
            new DateTimeFormatterBuilder()
                    .appendPattern("M/d/uuuu H:mm:ss")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter(Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter BUDGET_DATE =
            DateTimeFormatter.ofPattern("M/d/uuuu", Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
    private static final Set<String> BULK_METADATA =
            Set.of("Format Version", "Account", "Campaign", "Ad Group");

    final String digest;
    final int rowCount;
    private final Path path;
    private final Cursor cursor;

    private MicrosoftAdvertisingFile(String digest, int rowCount, Path path, Cursor cursor) {
        this.digest = digest;
        this.rowCount = rowCount;
        this.path = path;
        this.cursor = cursor;
    }

    /**
     * Consumes and validates the full body under independent compressed and expanded bounds.
     *
     * <p>No archive path is extracted. A failed attempt removes every temporary file it created.
     */
    static MicrosoftAdvertisingFile read(
            MicrosoftAdvertisingConfig config,
            Resource resource,
            MicrosoftAdvertisingSplit split,
            ObjectMapper json,
            InputStream wire,
            Deadline deadline)
            throws IOException, InterruptedException {
        Path expanded = null;
        Cursor cursor = null;
        boolean completed = false;
        try {
            expanded = Files.createTempFile("seatunnel-msads-", ".csv");
            BufferedInputStream input = new BufferedInputStream(wire);
            input.mark(4);
            int a = input.read();
            int b = input.read();
            int c = input.read();
            int d = input.read();
            input.reset();
            boolean zip = a == 'P' && b == 'K' && c == 3 && d == 4;
            boolean gzip = a == 0x1f && b == 0x8b;
            if (resource.kind == Kind.REPORT && !zip) {
                throw new MicrosoftAdvertisingFailure("report download is not a ZIP archive");
            }
            MessageDigest hash = MicrosoftAdvertisingConfig.digest();
            if (zip) {
                extractZip(config, input, expanded, hash, deadline);
            } else {
                try (InputStream data = gzip ? new GZIPInputStream(input) : input;
                        OutputStream target = Files.newOutputStream(expanded)) {
                    copy(
                            data,
                            target,
                            config.expandedBytes,
                            "max_expanded_bytes",
                            hash,
                            null,
                            deadline);
                }
            }
            String digest = MicrosoftAdvertisingConfig.hex(hash.digest());
            MicrosoftAdvertisingSplit.Account account = split.accounts.get(split.accountIndex);
            int records = 0;
            try (Cursor validating = new Cursor(expanded, config, resource, account, split, json)) {
                while (validating.read(deadline) != null) {
                    ++records;
                }
            }
            split.file.validate(digest, records);
            cursor = new Cursor(expanded, config, resource, account, split, json);
            for (int consumed = 0; consumed < split.file.row; ++consumed) {
                cursor.read(deadline);
            }
            MicrosoftAdvertisingFile file =
                    new MicrosoftAdvertisingFile(digest, records, expanded, cursor);
            completed = true;
            return file;
        } catch (CharacterCodingException ex) {
            throw new MicrosoftAdvertisingFailure("download CSV is not valid UTF-8");
        } catch (ZipException | EOFException ex) {
            throw new MicrosoftAdvertisingFailure("invalid or truncated download archive");
        } finally {
            if (!completed) {
                closeCursor(cursor);
                delete(expanded);
            }
        }
    }

    private static void extractZip(
            MicrosoftAdvertisingConfig config,
            InputStream input,
            Path expanded,
            MessageDigest digest,
            Deadline deadline)
            throws IOException, InterruptedException {
        Path archive = Files.createTempFile("seatunnel-msads-", ".zip");
        try {
            try (OutputStream wire = Files.newOutputStream(archive)) {
                copy(
                        input,
                        wire,
                        config.compressedBytes,
                        "max_compressed_bytes",
                        null,
                        null,
                        deadline);
            }
            try (ZipFile zip = new ZipFile(archive.toFile(), StandardCharsets.UTF_8)) {
                if (zip.size() != 1) {
                    throw new MicrosoftAdvertisingFailure(
                            "download ZIP must contain one CSV entry");
                }
                ZipEntry entry = zip.entries().nextElement();
                String name = entry.getName();
                if (entry.isDirectory()
                        || name.startsWith(".")
                        || name.indexOf('/') >= 0
                        || name.indexOf('\\') >= 0
                        || name.indexOf(':') >= 0
                        || !name.toLowerCase(Locale.ROOT).endsWith(".csv")
                        || !MicrosoftAdvertisingConfig.opaque(name, 512)
                        || entry.getSize() < 0
                        || entry.getCrc() < 0) {
                    throw new MicrosoftAdvertisingFailure("download ZIP entry is invalid");
                }
                if (entry.getSize() > config.expandedBytes) {
                    throw new MicrosoftAdvertisingFailure(
                            "download ZIP entry exceeds max_expanded_bytes");
                }
                CRC32 crc = new CRC32();
                long length;
                try (InputStream data = zip.getInputStream(entry);
                        OutputStream target = Files.newOutputStream(expanded)) {
                    length =
                            copy(
                                    data,
                                    target,
                                    config.expandedBytes,
                                    "max_expanded_bytes",
                                    digest,
                                    crc,
                                    deadline);
                }
                if (length != entry.getSize() || crc.getValue() != entry.getCrc()) {
                    throw new MicrosoftAdvertisingFailure(
                            "download ZIP has an invalid size or CRC");
                }
            }
        } finally {
            delete(archive);
        }
    }

    private static long copy(
            InputStream data,
            OutputStream out,
            long max,
            String option,
            MessageDigest digest,
            CRC32 crc,
            Deadline deadline)
            throws IOException, InterruptedException {
        byte[] block = new byte[16384];
        long count = 0;
        for (int length; (length = data.read(block)) >= 0; ) {
            count += length;
            if (count > max) {
                throw new MicrosoftAdvertisingFailure("download exceeds " + option);
            }
            out.write(block, 0, length);
            if (digest != null) {
                digest.update(block, 0, length);
            }
            if (crc != null) {
                crc.update(block, 0, length);
            }
            deadline.check();
        }
        return count;
    }

    Row next() {
        try {
            Row row =
                    cursor.read(
                            () -> {
                                if (Thread.currentThread().isInterrupted()) {
                                    throw new InterruptedException(
                                            "MicrosoftAdvertising source closed");
                                }
                            });
            if (row == null) {
                throw new MicrosoftAdvertisingFailure(
                        "validated CSV ended before its recorded row count");
            }
            return row;
        } catch (IOException | InterruptedException ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new MicrosoftAdvertisingFailure("could not read the validated local CSV");
        }
    }

    /**
     * Releases the input and private temporary CSV after completion or cancellation.
     *
     * <p>Only a fingerprint of the bytes survives a checkpoint.
     */
    @Override
    public void close() {
        closeCursor(cursor);
        delete(path);
    }

    private static void closeCursor(Cursor handle) {
        if (handle != null) {
            try {
                handle.close();
            } catch (IOException ignored) {
                // File ownership still ends even if a close fails.
            }
        }
    }

    private static void delete(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
                // A removed reader cannot retain an open handle for cleanup retries.
            }
        }
    }

    static String hourly(String value) {
        if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}\\|[0-9]{1,2}")) {
            throw new MicrosoftAdvertisingFailure("invalid hourly report TimePeriod");
        }
        try {
            LocalDateTime utc =
                    LocalDate.parse(value.substring(0, 10))
                            .atTime(Integer.parseInt(value.substring(11)), 0);
            return HOUR.format(utc.atOffset(ZoneOffset.UTC));
        } catch (DateTimeException ex) {
            throw new MicrosoftAdvertisingFailure("invalid hourly report TimePeriod");
        }
    }

    private static String budgetDate(String value) {
        try {
            LocalDate date =
                    value.indexOf('/') >= 0
                            ? LocalDate.parse(value, BUDGET_DATE)
                            : LocalDate.parse(value);
            return date.toString();
        } catch (DateTimeException ex) {
            throw new MicrosoftAdvertisingFailure("invalid budget summary Date");
        }
    }

    private static String modified(String value) {
        if (value.isEmpty()) {
            return value;
        }
        try {
            OffsetDateTime date =
                    value.indexOf('/') >= 0
                            ? LocalDateTime.parse(value, BULK_DATE).atOffset(ZoneOffset.UTC)
                            : OffsetDateTime.parse(value).withOffsetSameInstant(ZoneOffset.UTC);
            return BULK_OUTPUT.format(date);
        } catch (DateTimeException ex) {
            return value;
        }
    }

    /**
     * Checks that the network exchange or local parsing remains within its deadline.
     *
     * <p>The callback can interrupt both file expansion and checkpoint prefix skipping.
     */
    @FunctionalInterface
    interface Deadline {
        void check() throws InterruptedException;
    }

    /**
     * One validated output row from a bulk or reporting CSV.
     *
     * <p>The optional key is an ordered tuple encoded as JSON, independent of field separators.
     */
    static final class Row {
        final String key;
        final ObjectNode record;

        Row(String key, ObjectNode record) {
            this.key = key;
            this.record = record;
        }
    }

    /**
     * Projects validated CSV fields into the native report or bulk record.
     *
     * <p>Every path checks column counts before projecting the current row.
     */
    private static final class Cursor implements AutoCloseable {
        private final Csv csv;
        private final List<String> header;
        private final MicrosoftAdvertisingConfig config;
        private final Resource resource;
        private final MicrosoftAdvertisingSplit.Account account;
        private final MicrosoftAdvertisingSplit split;
        private final ObjectMapper json;
        private final int typeIndex;
        private int consumed;

        Cursor(
                Path path,
                MicrosoftAdvertisingConfig config,
                Resource resource,
                MicrosoftAdvertisingSplit.Account account,
                MicrosoftAdvertisingSplit split,
                ObjectMapper json)
                throws IOException {
            this.config = config;
            this.resource = resource;
            this.account = account;
            this.split = split;
            this.json = json;
            Csv open = new Csv(Files.newBufferedReader(path, StandardCharsets.UTF_8), config);
            boolean success = false;
            try {
                header = open.next();
                if (header == null
                        || new HashSet<>(header).size() != header.size()
                        || header.stream().anyMatch(String::isBlank)) {
                    throw new MicrosoftAdvertisingFailure(
                            "download CSV has invalid or duplicate column headers");
                }
                typeIndex = header.indexOf("Type");
                if (resource.kind == Kind.REPORT && !header.equals(resource.columns)
                        || resource.kind == Kind.BULK
                                && (typeIndex < 0 || !header.contains("Id"))) {
                    throw new MicrosoftAdvertisingFailure(
                            "download CSV columns do not match the request");
                }
                csv = open;
                success = true;
            } finally {
                if (!success) {
                    open.close();
                }
            }
        }

        Row read(Deadline deadline) throws IOException, InterruptedException {
            List<String> cells;
            while ((cells = csv.next()) != null) {
                deadline.check();
                if (++consumed > config.csvRows) {
                    throw new MicrosoftAdvertisingFailure("download exceeds max_csv_rows");
                }
                if (cells.size() != header.size()) {
                    throw new MicrosoftAdvertisingFailure(
                            "download CSV row has a different number of columns");
                }
                if (resource.kind == Kind.BULK && !resource.csvType.equals(cells.get(typeIndex))) {
                    String type = cells.get(typeIndex);
                    if (!BULK_METADATA.contains(type)) {
                        throw new MicrosoftAdvertisingFailure(
                                "bulk CSV contains an unexpected row type");
                    }
                    if ("Format Version".equals(type)
                            && header.contains("Name")
                            && !"6.0".equals(cells.get(header.indexOf("Name")))) {
                        throw new MicrosoftAdvertisingFailure("bulk CSV format version is not 6.0");
                    }
                    continue;
                }
                ObjectNode record = json.createObjectNode();
                for (int i = 0; i < header.size(); ++i) {
                    String cell = cells.get(i);
                    if (resource.kind == Kind.REPORT && (cell.isEmpty() || cell.equals("--"))) {
                        record.putNull(header.get(i));
                    } else {
                        record.put(header.get(i), cell);
                    }
                }
                if (resource.kind == Kind.BULK) {
                    bulk(record);
                } else {
                    report(record);
                }
                return new Row(key(record), record);
            }
            return null;
        }

        private void bulk(ObjectNode record) {
            String accountId = record.path("Account Id").asText("");
            if (!accountId.isEmpty() && !account.id.equals(accountId)) {
                throw new MicrosoftAdvertisingFailure(
                        "bulk CSV account does not match the scoped request");
            }
            record.put("Account Id", account.id);
            if (record.has("Modified Time")) {
                record.put("Modified Time", modified(record.path("Modified Time").asText()));
            }
            for (String key : resource.identity) {
                MicrosoftAdvertisingApi.id(record.path(key), "bulk row");
            }
        }

        private void report(ObjectNode record) {
            String accountId = record.path("AccountId").asText("");
            if (!accountId.isEmpty() && !account.id.equals(accountId)) {
                throw new MicrosoftAdvertisingFailure(
                        "report CSV account does not match the scoped request");
            }
            String value = record.path("TimePeriod").asText("");
            if (!value.isEmpty() && "Hourly".equals(resource.aggregation)) {
                record.put("TimePeriod", hourly(value));
            }
            if (resource.predefinedTime != null) {
                boolean day = "DayOfWeek".equals(resource.aggregation);
                if (!value.matches(day ? "[1-7]" : "(?:[01]?[0-9]|2[0-3])")) {
                    throw new MicrosoftAdvertisingFailure(
                            "invalid grouped report TimePeriod bucket");
                }
                LocalDate[] dates = resource.predefinedDates(split.asOf);
                record.put(day ? "DayOfWeek" : "HourOfDay", value);
                record.put("StartOfTimePeriod", dates[0].toString());
                record.put("EndOfTimePeriod", dates[1].toString());
                record.put("TimePeriod", dates[1].toString());
            }
            String budget = record.path("Date").asText("");
            if ("BudgetSummaryReportRequest".equals(resource.requestType) && !budget.isEmpty()) {
                record.put("Date", budgetDate(budget));
            }
        }

        private String key(ObjectNode record) {
            if (resource.identity.isEmpty()) {
                return null;
            }
            ArrayNode values = json.createArrayNode();
            for (String part : resource.identity) {
                boolean zero = part.endsWith(":0");
                String column = zero ? part.substring(0, part.length() - 2) : part;
                values.add(record.path(column).asText(zero ? "0" : ""));
            }
            return values.toString();
        }

        @Override
        public void close() throws IOException {
            csv.close();
        }
    }

    /**
     * Streaming comma-separated record parser with strict quoted field handling.
     *
     * <p>A limit applies to the complete logical record, including embedded newlines and escapes.
     */
    private static final class Csv implements AutoCloseable {
        private final PushbackReader input;
        private final int maxRow;
        private final int maxColumns;
        private boolean first = true;

        Csv(Reader input, MicrosoftAdvertisingConfig config) {
            this.input = new PushbackReader(input, 1);
            maxRow = config.csvRow;
            maxColumns = config.csvColumns;
        }

        List<String> next() throws IOException {
            List<String> fields = new ArrayList<>();
            StringBuilder cell = new StringBuilder();
            boolean quoted = false;
            boolean quoteClosed = false;
            boolean sawContent = false;
            int length = 0;
            int raw = input.read();
            if (first) {
                first = false;
                if (raw == '\ufeff') {
                    raw = input.read();
                }
            }
            for (; raw != -1; raw = input.read()) {
                char character = (char) raw;
                if (++length > maxRow) {
                    throw new MicrosoftAdvertisingFailure(
                            "download CSV record exceeds max_csv_row_chars");
                }
                if (quoted) {
                    if (character == '"') {
                        int next = input.read();
                        if (next == '"') {
                            if (++length > maxRow) {
                                throw new MicrosoftAdvertisingFailure(
                                        "download CSV record exceeds max_csv_row_chars");
                            }
                            cell.append('"');
                        } else {
                            quoted = false;
                            quoteClosed = true;
                            if (next != -1) {
                                input.unread(next);
                            }
                        }
                    } else {
                        cell.append(character);
                    }
                } else if (character == ',' || character == '\r' || character == '\n') {
                    boolean newLine = character != ',';
                    if (character == '\r') {
                        int next = input.read();
                        if (next != -1 && next != '\n') {
                            input.unread(next);
                        }
                    }
                    if (newLine && !sawContent) {
                        length = 0;
                        continue;
                    }
                    append(fields, cell);
                    if (newLine) {
                        return fields;
                    }
                    sawContent = true;
                    quoteClosed = false;
                } else {
                    sawContent = true;
                    if (quoteClosed || character == '"' && cell.length() > 0 || character == 0) {
                        throw new MicrosoftAdvertisingFailure("invalid quoted download CSV field");
                    }
                    if (character == '"') {
                        quoted = true;
                    } else {
                        cell.append(character);
                    }
                }
            }
            if (quoted) {
                throw new MicrosoftAdvertisingFailure("unterminated quoted download CSV field");
            }
            if (sawContent) {
                append(fields, cell);
                return fields;
            }
            return null;
        }

        private void append(List<String> fields, StringBuilder cell) {
            if (fields.size() >= maxColumns) {
                throw new MicrosoftAdvertisingFailure("download CSV exceeds max_csv_columns");
            }
            fields.add(cell.toString());
            cell.setLength(0);
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
