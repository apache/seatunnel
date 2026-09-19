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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.mppdb;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.relational.TableId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Decoder for serial and parallel JSON/text output from {@code mppdb_decoding}. */
final class MppdbTextDecoder {

    /** JSON parser used for mppdb row-change objects. */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Serial and parallel BEGIN/COMMIT records carry transaction details as plain text. */
    private static final Pattern TRANSACTION_PATTERN =
            Pattern.compile("^(BEGIN|COMMIT)\\b(.*)$", Pattern.CASE_INSENSITIVE);

    /** Serial decoding places the transaction id directly after BEGIN or COMMIT. */
    private static final Pattern LEADING_TRANSACTION_ID = Pattern.compile("^\\s*(\\d+)\\b");

    /** Parallel decoding labels the COMMIT transaction id as XID. */
    private static final Pattern NAMED_TRANSACTION_ID =
            Pattern.compile("\\bXID\\s*:?\\s*(\\d+)\\b", Pattern.CASE_INSENSITIVE);

    /** Parallel text rows use the documented `table schema table OPERATION:` prefix. */
    private static final Pattern TEXT_CHANGE_PATTERN =
            Pattern.compile(
                    "^table\\s+(\"(?:[^\"]|\"\")*\"|\\S+)\\s+"
                            + "(\"(?:[^\"]|\"\")*\"|\\S+)\\s+"
                            + "(INSERT|UPDATE|DELETE):\\s*(.*)$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /** Column values in text mode use `name[type]:literal` tokens. */
    private static final Pattern TEXT_COLUMN_PATTERN =
            Pattern.compile(
                    "(\"(?:[^\"]|\"\")*\"|[^\\s\\[]+)\\[([^]]+)]\\s*:\\s*"
                            + "(?:'((?:''|[^'])*)'|([^\\s]+))");

    /** Decodes one unframed serial mppdb message. */
    MppdbWalChange decodeRecord(long lsn, long transactionId, String data) {
        if (data == null || data.trim().isEmpty()) {
            throw malformed("empty text record");
        }
        String record = data.trim();
        Matcher transaction = TRANSACTION_PATTERN.matcher(record);
        if (transaction.matches()) {
            long parsedTransactionId = parseTransactionId(transaction.group(2), transactionId);
            MppdbWalChange.Type type =
                    "BEGIN".equalsIgnoreCase(transaction.group(1))
                            ? MppdbWalChange.Type.BEGIN
                            : MppdbWalChange.Type.COMMIT;
            return new MppdbWalChange(lsn, parsedTransactionId, type, null, null, null, null);
        }
        if (record.charAt(0) == '{') {
            return decodeJson(lsn, transactionId, record);
        }
        return decodeTextChange(lsn, transactionId, record);
    }

    /**
     * Decodes parallel JSON or text frames.
     *
     * <p>Each frame starts with a uint32 byte length, followed by uint64 LSN and the textual
     * record.
     */
    List<MppdbWalChange> decodeBatch(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Collections.emptyList();
        }
        if (looksUnframed(payload)) {
            return Collections.singletonList(
                    decodeRecord(0, 0, new String(payload, StandardCharsets.UTF_8)));
        }

        ByteBuffer batch = ByteBuffer.wrap(payload);
        List<MppdbWalChange> changes = new ArrayList<>();
        while (batch.hasRemaining()) {
            requireRemaining(batch, Integer.BYTES, "frame length");
            long unsignedSize = Integer.toUnsignedLong(batch.getInt());
            if (unsignedSize == 0) {
                if (batch.hasRemaining()) {
                    throw malformed("bytes found after the text batch terminator");
                }
                break;
            }
            if (unsignedSize < Long.BYTES || unsignedSize > Integer.MAX_VALUE) {
                throw malformed("invalid text frame length " + unsignedSize);
            }
            int frameSize = (int) unsignedSize;
            requireRemaining(batch, frameSize, "text frame");
            long lsn = batch.getLong();
            byte[] data = new byte[frameSize - Long.BYTES];
            batch.get(data);
            changes.add(decodeRecord(lsn, 0, new String(data, StandardCharsets.UTF_8)));
            if (batch.hasRemaining()
                    && (batch.get(batch.position()) == 'P' || batch.get(batch.position()) == 'F')) {
                byte separator = batch.get();
                if (separator == 'F') {
                    if (batch.remaining() == Integer.BYTES && batch.getInt() == 0) {
                        break;
                    }
                    if (batch.hasRemaining()) {
                        throw malformed("bytes found after the final text frame");
                    }
                    break;
                }
            }
        }
        return changes;
    }

    /** Returns whether a payload starts directly with serial text or JSON. */
    private boolean looksUnframed(byte[] payload) {
        int index = 0;
        while (index < payload.length && Character.isWhitespace(payload[index])) {
            index++;
        }
        if (index == payload.length) {
            return true;
        }
        byte first = payload[index];
        return first == '{'
                || first == 'B'
                || first == 'b'
                || first == 'C'
                || first == 'c'
                || first == 't'
                || first == 'T';
    }

    /** Parses the official mppdb JSON row-change object without ad hoc string splitting. */
    private MppdbWalChange decodeJson(long lsn, long transactionId, String data) {
        final JsonNode root;
        try {
            root = OBJECT_MAPPER.readTree(data);
        } catch (IOException e) {
            throw malformed("invalid JSON record", e);
        }
        String qualifiedTable = requiredText(root, "table_name");
        TableId tableId = TableId.parse(qualifiedTable, false);
        if (tableId == null || tableId.schema() == null || tableId.table() == null) {
            throw malformed("table_name must use schema.table format: " + qualifiedTable);
        }
        MppdbWalChange.Type type = parseDataChangeType(requiredText(root, "op_type"));
        List<MppdbWalChange.ColumnValue> newColumns = Collections.emptyList();
        List<MppdbWalChange.ColumnValue> oldColumns = Collections.emptyList();
        if (type == MppdbWalChange.Type.INSERT || type == MppdbWalChange.Type.UPDATE) {
            newColumns = decodeJsonColumns(root, "columns_name", "columns_type", "columns_val");
        }
        if (type == MppdbWalChange.Type.UPDATE || type == MppdbWalChange.Type.DELETE) {
            oldColumns = decodeJsonColumns(root, "old_keys_name", "old_keys_type", "old_keys_val");
        }
        return new MppdbWalChange(
                lsn,
                transactionId,
                type,
                tableId.schema(),
                tableId.table(),
                oldColumns,
                newColumns);
    }

    /** Parses a parallel text row-change record. */
    private MppdbWalChange decodeTextChange(long lsn, long transactionId, String data) {
        Matcher changeMatcher = TEXT_CHANGE_PATTERN.matcher(data);
        if (!changeMatcher.matches()) {
            throw malformed("unsupported text record: " + data);
        }
        MppdbWalChange.Type type = parseDataChangeType(changeMatcher.group(3));
        String tupleData = changeMatcher.group(4).trim();
        List<MppdbWalChange.ColumnValue> oldColumns = Collections.emptyList();
        List<MppdbWalChange.ColumnValue> newColumns = Collections.emptyList();
        if (type == MppdbWalChange.Type.INSERT) {
            newColumns = parseTextColumns(removeOptionalPrefix(tupleData, "new-tuple:"));
        } else if (type == MppdbWalChange.Type.DELETE) {
            oldColumns = parseTextColumns(removeOptionalPrefix(tupleData, "old-key:"));
        } else {
            String normalized = tupleData.toLowerCase(Locale.ROOT);
            int oldStart = normalized.indexOf("old-key:");
            int newStart = normalized.indexOf("new-tuple:");
            if (newStart < 0) {
                throw malformed("UPDATE text record does not contain new-tuple: " + tupleData);
            }
            if (oldStart >= 0) {
                if (oldStart > newStart || !tupleData.substring(0, oldStart).trim().isEmpty()) {
                    throw malformed("invalid UPDATE tuple order: " + tupleData);
                }
                oldColumns =
                        parseTextColumns(
                                tupleData.substring(oldStart + "old-key:".length(), newStart));
            } else if (!tupleData.substring(0, newStart).trim().isEmpty()) {
                throw malformed("invalid UPDATE tuple prefix: " + tupleData);
            }
            newColumns = parseTextColumns(tupleData.substring(newStart + "new-tuple:".length()));
            if (newColumns.isEmpty()) {
                throw malformed("UPDATE text record contains an empty new tuple");
            }
        }
        return new MppdbWalChange(
                lsn,
                transactionId,
                type,
                unquoteIdentifier(changeMatcher.group(1)),
                unquoteIdentifier(changeMatcher.group(2)),
                oldColumns,
                newColumns);
    }

    /**
     * Parses one whitespace-delimited list of text-format columns without ignoring malformed gaps.
     */
    private List<MppdbWalChange.ColumnValue> parseTextColumns(String data) {
        String columnsText = data.trim();
        if (columnsText.isEmpty()) {
            return Collections.emptyList();
        }
        List<MppdbWalChange.ColumnValue> columns = new ArrayList<>();
        Matcher columnMatcher = TEXT_COLUMN_PATTERN.matcher(columnsText);
        int consumed = 0;
        while (columnMatcher.find()) {
            if (!columnsText.substring(consumed, columnMatcher.start()).trim().isEmpty()) {
                throw malformed("could not parse text columns: " + columnsText);
            }
            String quotedValue = columnMatcher.group(3);
            String rawValue = quotedValue == null ? columnMatcher.group(4) : quotedValue;
            boolean nullValue = quotedValue == null && "null".equalsIgnoreCase(rawValue);
            columns.add(
                    new MppdbWalChange.ColumnValue(
                            unquoteIdentifier(columnMatcher.group(1)),
                            0,
                            columnMatcher.group(2),
                            nullValue ? null : rawValue.replace("''", "'"),
                            nullValue));
            consumed = columnMatcher.end();
        }
        if (columns.isEmpty() || !columnsText.substring(consumed).trim().isEmpty()) {
            throw malformed("could not parse text columns: " + columnsText);
        }
        return columns;
    }

    /** Removes a tuple label accepted by GaussDB examples when it is present. */
    private String removeOptionalPrefix(String data, String prefix) {
        return data.regionMatches(true, 0, prefix, 0, prefix.length())
                ? data.substring(prefix.length()).trim()
                : data;
    }

    /** Builds aligned column values from the three arrays in an mppdb JSON record. */
    private List<MppdbWalChange.ColumnValue> decodeJsonColumns(
            JsonNode root, String namesField, String typesField, String valuesField) {
        JsonNode names = requiredArray(root, namesField);
        JsonNode types = requiredArray(root, typesField);
        JsonNode values = requiredArray(root, valuesField);
        if (names.size() != types.size() || names.size() != values.size()) {
            throw malformed(
                    "JSON column arrays are not aligned for "
                            + namesField
                            + ": names="
                            + names.size()
                            + ", types="
                            + types.size()
                            + ", values="
                            + values.size());
        }
        List<MppdbWalChange.ColumnValue> columns = new ArrayList<>(names.size());
        for (int index = 0; index < names.size(); index++) {
            String literal = values.get(index).isNull() ? null : values.get(index).asText();
            boolean nullValue = literal == null || "null".equalsIgnoreCase(literal);
            columns.add(
                    new MppdbWalChange.ColumnValue(
                            unquoteIdentifier(names.get(index).asText()),
                            0,
                            types.get(index).asText(),
                            nullValue ? null : decodeSqlLiteral(literal),
                            nullValue));
        }
        return columns;
    }

    /** Converts a PostgreSQL-compatible SQL literal emitted as JSON text into raw text. */
    private String decodeSqlLiteral(String literal) {
        if (literal.length() >= 2 && literal.startsWith("'") && literal.endsWith("'")) {
            return literal.substring(1, literal.length() - 1).replace("''", "'");
        }
        return literal;
    }

    /** Reads a required textual JSON field. */
    private String requiredText(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isTextual() || value.asText().trim().isEmpty()) {
            throw malformed("missing or invalid JSON field " + field);
        }
        return value.asText();
    }

    /** Reads a required JSON array field. */
    private JsonNode requiredArray(JsonNode root, String field) {
        JsonNode value = root.get(field);
        if (value == null || !value.isArray()) {
            throw malformed("missing or invalid JSON array " + field);
        }
        return value;
    }

    /** Converts a supported DML operation name into the normalized record kind. */
    private MppdbWalChange.Type parseDataChangeType(String operation) {
        try {
            MppdbWalChange.Type type =
                    MppdbWalChange.Type.valueOf(operation.toUpperCase(Locale.ROOT));
            if (type == MppdbWalChange.Type.BEGIN || type == MppdbWalChange.Type.COMMIT) {
                throw malformed("transaction operation used as a data change: " + operation);
            }
            return type;
        } catch (IllegalArgumentException e) {
            throw malformed("unsupported operation " + operation, e);
        }
    }

    /** Removes PostgreSQL identifier quotes retained by mppdb JSON output. */
    private String unquoteIdentifier(String identifier) {
        String value = identifier.trim();
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1).replace("\"\"", "\"");
        }
        return value;
    }

    /** Parses an unsigned transaction id into the signed long representation used by Debezium. */
    private long parseUnsignedLong(String value, String field) {
        try {
            return Long.parseUnsignedLong(value);
        } catch (NumberFormatException e) {
            throw malformed("invalid " + field + " " + value, e);
        }
    }

    /** Extracts either the serial leading transaction id or the parallel XID label. */
    private long parseTransactionId(String details, long fallback) {
        Matcher leading = LEADING_TRANSACTION_ID.matcher(details);
        if (leading.find()) {
            return parseUnsignedLong(leading.group(1), "transaction id");
        }
        Matcher named = NAMED_TRANSACTION_ID.matcher(details);
        return named.find() ? parseUnsignedLong(named.group(1), "transaction id") : fallback;
    }

    /** Verifies frame bounds before reading from a server payload. */
    private void requireRemaining(ByteBuffer buffer, int required, String field) {
        if (required < 0 || buffer.remaining() < required) {
            throw malformed(
                    "truncated "
                            + field
                            + ": required "
                            + required
                            + " bytes but only "
                            + buffer.remaining()
                            + " remain");
        }
    }

    /** Creates a consistent exception for malformed text output. */
    private IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("Malformed mppdb_decoding text payload: " + message);
    }

    /** Creates a consistent exception that retains the JSON or numeric parser failure. */
    private IllegalArgumentException malformed(String message, Exception cause) {
        return new IllegalArgumentException(
                "Malformed mppdb_decoding text payload: " + message, cause);
    }
}
