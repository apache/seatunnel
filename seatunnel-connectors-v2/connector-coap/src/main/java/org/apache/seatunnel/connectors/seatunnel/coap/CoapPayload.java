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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.format.avro.AvroDeserializationSchema;
import org.apache.seatunnel.format.avro.AvroSerializationSchema;
import org.apache.seatunnel.format.avro.RowToAvroConverter;
import org.apache.seatunnel.format.csv.CsvDeserializationSchema;
import org.apache.seatunnel.format.csv.CsvSerializationSchema;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.json.JsonSerializationSchema;
import org.apache.seatunnel.format.protobuf.ProtobufDeserializationSchema;
import org.apache.seatunnel.format.protobuf.ProtobufSerializationSchema;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.eclipse.californium.core.coap.MediaTypeRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converts one complete, bounded CoAP representation without logging its content. */
final class CoapPayload {
    static final ObjectMapper JSON =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(
                            DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                            DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    static {
        JSON.getFactory()
                .setStreamReadConstraints(
                        StreamReadConstraints.builder()
                                .maxNestingDepth(100)
                                .maxNumberLength(1024)
                                .maxStringLength(CoapSettings.ABSOLUTE_BODY_LIMIT)
                                .build());
    }

    private CoapPayload() {}

    static int contentFormat(CoapOptions.Format format) {
        switch (format) {
            case JSON:
            case RECORD_JSON:
                return MediaTypeRegistry.APPLICATION_JSON;
            case TEXT:
            case CSV:
            case LOG:
                return MediaTypeRegistry.TEXT_PLAIN;
            case XML:
                return MediaTypeRegistry.APPLICATION_XML;
            default:
                return MediaTypeRegistry.APPLICATION_OCTET_STREAM;
        }
    }

    /** Converts one complete message to a batch that can be admitted atomically. */
    interface Decoder {
        List<SeaTunnelRow> decode(byte[] bytes, InetSocketAddress peer) throws IOException;
    }

    /** Encodes one source row using the configured destination representation. */
    interface Encoder {
        byte[] encode(SeaTunnelRow row) throws IOException;
    }

    static Decoder decoder(CoapSettings.Listener settings, CatalogTable table) {
        switch (settings.format) {
            case JSON:
                return jsonReader(settings, table);
            case TEXT:
                return (bytes, peer) ->
                        singleton(new SeaTunnelRow(new Object[] {decodeText(bytes)}));
            case BINARY:
                return (bytes, peer) -> singleton(new SeaTunnelRow(new Object[] {bytes.clone()}));
            case CSV:
                return csvReader(settings, table);
            case AVRO:
                AvroDeserializationSchema avro =
                        new AvroDeserializationSchema(table, settings.avroSchema);
                return (bytes, peer) -> singleton(avro.deserialize(bytes));
            case PROTOBUF:
                Map<String, String> metadata = new HashMap<>();
                metadata.put("protobuf_schema", settings.protoSchema);
                metadata.put("protobuf_message_name", settings.protoMessage);
                CatalogTable payload =
                        CatalogTable.of(
                                table.getTableId(),
                                table.getTableSchema(),
                                metadata,
                                table.getPartitionKeys(),
                                table.getComment());
                ProtobufDeserializationSchema protobuf = new ProtobufDeserializationSchema(payload);
                return (bytes, peer) -> singleton(protobuf.deserialize(bytes));
            case XML:
                return CoapXml.decoder(settings, table);
            case LOG:
                return CoapLog.decoder(settings, table);
            case DATAGRAM:
                return new CoapDatagram(settings, table);
            case RECORD_JSON:
                return CoapRecordJson.decoder(settings, table);
            default:
                throw new IllegalStateException("Unsupported CoAP format");
        }
    }

    static Encoder encoder(CoapSettings.Sender settings, SeaTunnelRowType rowType) {
        if (!settings.hasBody()) {
            return row -> new byte[0];
        }
        switch (settings.format) {
            case JSON:
                JsonSerializationSchema json = new JsonSerializationSchema(rowType);
                return row -> {
                    limitRow(row, settings.maxBytes);
                    LimitedOutput output = new LimitedOutput(settings.maxBytes);
                    JSON.writeValue(output, json.convert(row));
                    return output.bytes();
                };
            case TEXT:
                int text = payloadColumn(settings, rowType, SqlType.STRING);
                return row -> checkedText(row.getField(text), settings.maxBytes);
            case BINARY:
                int binary = payloadColumn(settings, rowType, SqlType.BYTES);
                return row -> checkedBinary(row.getField(binary), settings.maxBytes);
            case CSV:
                return bounded(
                        CsvSerializationSchema.builder()
                                .seaTunnelRowType(rowType)
                                .delimiter(settings.csvDelimiter)
                                .build(),
                        settings.maxBytes);
            case AVRO:
                return settings.options.get(CoapOptions.AVRO_CONTAINER)
                        ? avroContainer(rowType, settings.maxBytes)
                        : bounded(new AvroSerializationSchema(rowType), settings.maxBytes);
            case PROTOBUF:
                return bounded(
                        new ProtobufSerializationSchema(
                                rowType, settings.protoMessage, settings.protoSchema),
                        settings.maxBytes);
            case RECORD_JSON:
                return CoapRecordJson.encoder(rowType, settings.maxBytes);
            default:
                throw new IllegalStateException("Unsupported CoAP format");
        }
    }

    private static Decoder jsonReader(CoapSettings.Listener settings, CatalogTable table) {
        JsonDeserializationSchema rows =
                settings.typed ? new JsonDeserializationSchema(table, false, false) : null;
        return (bytes, peer) -> {
            String text = decodeText(bytes);
            JsonNode checked = JSON.readTree(text);
            if (checked == null || checked.isMissingNode()) {
                throw new IOException("invalid JSON representation");
            }
            if (rows == null) {
                return singleton(new SeaTunnelRow(new Object[] {text}));
            }
            if (checked.isObject()) {
                return singleton(rows.deserialize(text));
            }
            if (!checked.isArray() || checked.size() > CoapSettings.MAX_ROWS) {
                throw new IOException("expected a JSON object or bounded array of objects");
            }
            List<SeaTunnelRow> decoded = new ArrayList<>(checked.size());
            for (JsonNode node : checked) {
                if (!node.isObject()) {
                    throw new IOException("expected JSON record object");
                }
                decoded.add(rows.deserialize(node.toString()));
            }
            return decoded;
        };
    }

    private static Decoder csvReader(CoapSettings.Listener settings, CatalogTable table) {
        CsvDeserializationSchema csv =
                CsvDeserializationSchema.builder()
                        .seaTunnelRowType(table.getSeaTunnelRowType())
                        .setCatalogTable(table)
                        .delimiter(settings.csvDelimiter)
                        .build();
        CSVFormat format = CSVFormat.DEFAULT.withDelimiter(settings.csvDelimiter.charAt(0));
        return (bytes, peer) -> {
            try (CSVParser parser = CSVParser.parse(decodeText(bytes), format)) {
                List<SeaTunnelRow> records = new ArrayList<>();
                for (CSVRecord row : parser) {
                    if (records.size() == CoapSettings.MAX_ROWS
                            || row.size() != table.getSeaTunnelRowType().getTotalFields()) {
                        throw new IOException(
                                "CSV records exceed bounds or do not match the schema");
                    }
                    Map<Integer, String> fields = new HashMap<>();
                    for (int i = 0; i < row.size(); i++) {
                        fields.put(i, row.get(i));
                    }
                    records.add(csv.getSeaTunnelRow(fields));
                }
                return records;
            }
        };
    }

    private static Encoder bounded(SerializationSchema schema, int limit) {
        return row -> {
            limitRow(row, limit);
            return checkedBinary(schema.serialize(row), limit);
        };
    }

    private static Encoder avroContainer(SeaTunnelRowType rowType, int limit) {
        RowToAvroConverter converter = new RowToAvroConverter(rowType);
        return row -> {
            limitRow(row, limit);
            LimitedOutput output = new LimitedOutput(limit);
            try (DataFileWriter<GenericRecord> file = new DataFileWriter<>(converter.getWriter())) {
                file.create(converter.getSchema(), output);
                file.append(converter.convertRowToGenericRecord(row));
            }
            return output.bytes();
        };
    }

    static List<SeaTunnelRow> singleton(SeaTunnelRow value) throws IOException {
        if (value == null) {
            throw new IOException("decoder produced no record");
        }
        return Collections.singletonList(value);
    }

    static void limitRows(List<SeaTunnelRow> rows, int bytes) throws IOException {
        if (rows.size() > CoapSettings.MAX_ROWS) {
            throw new IOException("too many decoded records");
        }
        int budget = CoapSettings.decodedBudget(bytes);
        for (SeaTunnelRow row : rows) {
            budget = remaining(row, budget, 0);
        }
    }

    private static int payloadColumn(
            CoapSettings.Sender settings, SeaTunnelRowType rowType, SqlType type) {
        int index = rowType.indexOf(settings.payloadField, false);
        if (index < 0 || rowType.getFieldType(index).getSqlType() != type) {
            throw CoapSettings.invalid("payload_field must refer to an input " + type + " column");
        }
        return index;
    }

    private static byte[] checkedBinary(Object input, int limit) throws IOException {
        if (!(input instanceof byte[])) {
            throw new IOException("CoAP sink requires a non-null binary payload");
        }
        byte[] bytes = (byte[]) input;
        if (bytes.length > limit) {
            throw tooLarge();
        }
        return bytes;
    }

    private static byte[] checkedText(Object input, int limit) throws IOException {
        if (!(input instanceof String)) {
            throw new IOException("CoAP sink requires a non-null text payload");
        }
        String text = (String) input;
        if (text.length() > limit) {
            throw tooLarge();
        }
        ByteBuffer encoded =
                StandardCharsets.UTF_8
                        .newEncoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .encode(CharBuffer.wrap(text));
        if (encoded.remaining() > limit) {
            throw tooLarge();
        }
        byte[] result = new byte[encoded.remaining()];
        encoded.get(result);
        return result;
    }

    static String decodeText(byte[] bytes) throws IOException {
        return StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString();
    }

    // Applies a structural preflight before generated serializers can allocate a second large
    // buffer.
    static void limitRow(SeaTunnelRow row, int limit) throws IOException {
        remaining(row, limit, 0);
    }

    private static int remaining(Object field, int limit, int depth) throws IOException {
        if (limit < 0 || depth > 100) {
            throw tooLarge();
        }
        int left = limit - 1;
        if (field instanceof CharSequence) {
            left -= ((CharSequence) field).length();
        } else if (field instanceof byte[]) {
            left -= ((byte[]) field).length;
        } else if (field instanceof BigInteger) {
            left -= ((BigInteger) field).bitLength() / 8;
        } else if (field instanceof BigDecimal) {
            left -= ((BigDecimal) field).unscaledValue().bitLength() / 8;
        } else if (field instanceof SeaTunnelRow) {
            left = remaining(((SeaTunnelRow) field).getFields(), left, depth + 1);
        } else if (field instanceof Map) {
            for (Map.Entry<?, ?> value : ((Map<?, ?>) field).entrySet()) {
                left = remaining(value.getKey(), left, depth + 1);
                left = remaining(value.getValue(), left, depth + 1);
            }
        } else if (field instanceof Iterable) {
            for (Object value : (Iterable<?>) field) {
                left = remaining(value, left, depth + 1);
            }
        } else if (field != null && field.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(field); i++) {
                left = remaining(Array.get(field, i), left, depth + 1);
            }
        }
        if (left < 0) {
            throw tooLarge();
        }
        return left;
    }

    private static IOException tooLarge() {
        return new IOException("CoAP sink payload exceeds max_payload_bytes");
    }

    /** Rejects a write before increasing the output buffer beyond the message limit. */
    static final class LimitedOutput extends OutputStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();
        private final int limit;

        LimitedOutput(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            reserve(1);
            output.write(value);
        }

        @Override
        public void write(byte[] values, int offset, int count) throws IOException {
            reserve(count);
            output.write(values, offset, count);
        }

        private void reserve(int count) throws IOException {
            if (count > limit - output.size()) {
                throw tooLarge();
            }
        }

        byte[] bytes() {
            return output.toByteArray();
        }
    }
}
