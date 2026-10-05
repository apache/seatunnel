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

package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient.RowKeyMutation;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;
import org.apache.seatunnel.connectors.seatunnel.bigtable.state.BigtableCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.bigtable.state.BigtableSinkState;

import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Writes {@link SeaTunnelRow} records to Google Cloud Bigtable.
 *
 * <p>Each row is converted to a Bigtable mutation using the configured column-family mapping and
 * row-key strategy. Mutations are accumulated in a local buffer and flushed in bulk when the buffer
 * reaches {@code batchMutationSize}.
 */
@Slf4j
public class BigtableSinkWriter
        implements SinkWriter<SeaTunnelRow, BigtableCommitInfo, BigtableSinkState>,
                SupportMultiTableSinkWriter<Void> {

    private static final String ALL_COLUMNS_KEY = "all_columns";
    private static final String DEFAULT_FAMILY = "cf";

    private final BigtableClient bigtableClient;
    private final SeaTunnelRowType rowType;
    private final BigtableParameters parameters;
    private final List<Integer> rowkeyColumnIndexes;
    private final int versionColumnIndex;
    private final String defaultFamily;

    /** Buffer of pending mutations, flushed in batches. */
    private final List<RowKeyMutation> buffer;

    public BigtableSinkWriter(
            SeaTunnelRowType rowType,
            BigtableParameters parameters,
            List<Integer> rowkeyColumnIndexes,
            int versionColumnIndex) {
        this(rowType, parameters, rowkeyColumnIndexes, versionColumnIndex, null);
    }

    BigtableSinkWriter(
            SeaTunnelRowType rowType,
            BigtableParameters parameters,
            List<Integer> rowkeyColumnIndexes,
            int versionColumnIndex,
            BigtableClient bigtableClient) {
        this.rowType = rowType;
        this.parameters = parameters;
        this.rowkeyColumnIndexes = rowkeyColumnIndexes;
        this.versionColumnIndex = versionColumnIndex;
        this.buffer = new ArrayList<>(parameters.getBatchMutationSize());

        Map<String, String> familyMap = parameters.getColumnFamily();
        if (familyMap != null && familyMap.containsKey(ALL_COLUMNS_KEY)) {
            this.defaultFamily = familyMap.get(ALL_COLUMNS_KEY);
        } else {
            this.defaultFamily = DEFAULT_FAMILY;
        }

        this.bigtableClient =
                bigtableClient != null ? bigtableClient : BigtableClient.createInstance(parameters);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        RowKeyMutation entry = convertRowToMutation(element); // outside lock: no shared state
        synchronized (buffer) {
            buffer.add(entry);
            if (buffer.size() >= parameters.getBatchMutationSize()) {
                flush();
            }
        }
    }

    @Override
    public Optional<BigtableCommitInfo> prepareCommit() throws IOException {
        synchronized (buffer) {
            flush();
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        try {
            synchronized (buffer) {
                flush();
            }
        } finally {
            if (bigtableClient != null) {
                bigtableClient.close();
            }
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }
        List<RowKeyMutation> toFlush = new ArrayList<>(buffer);
        buffer.clear(); // clear first: prevents re-sending if bulkMutate throws
        bigtableClient.bulkMutate(toFlush);
    }

    private RowKeyMutation convertRowToMutation(SeaTunnelRow row) {
        ByteString rowKey = buildRowKey(row);
        if (rowKey.isEmpty()) {
            throw new BigtableConnectorException(
                    BigtableConnectorErrorCode.WRITE_FAILED,
                    "Row key cannot be empty. Check rowkey_column configuration.");
        }

        long timestamp = System.currentTimeMillis() * 1000L; // Bigtable uses microseconds
        if (versionColumnIndex != -1) {
            Object versionField = row.getField(versionColumnIndex);
            if (versionField instanceof Long) {
                timestamp = (Long) versionField;
            }
        }

        Mutation mutation = Mutation.create();

        List<Integer> writeColumnIndexes =
                IntStream.range(0, row.getArity())
                        .boxed()
                        .filter(idx -> !rowkeyColumnIndexes.contains(idx))
                        .filter(idx -> idx != versionColumnIndex)
                        .collect(Collectors.toList());

        for (Integer idx : writeColumnIndexes) {
            String fieldName = rowType.getFieldName(idx);
            String family = resolveFamily(fieldName);
            Object fieldValue = row.getField(idx);
            ByteString qualifier = ByteString.copyFromUtf8(fieldName);

            if (fieldValue == null) {
                if (parameters.getNullMode() == BigtableSinkOptions.NullMode.EMPTY) {
                    mutation.setCell(family, qualifier, timestamp, ByteString.EMPTY);
                }
                // SKIP: do nothing
            } else {
                ByteString valueBytes = convertToByteString(row, idx);
                mutation.setCell(family, qualifier, timestamp, valueBytes);
            }
        }

        return new RowKeyMutation(rowKey, mutation);
    }

    private ByteString buildRowKey(SeaTunnelRow row) {
        if (rowkeyColumnIndexes.size() == 1) {
            return fieldToByteString(row, rowkeyColumnIndexes.get(0));
        }
        String delimiter = parameters.getRowkeyDelimiter();
        List<String> parts = new ArrayList<>();
        for (Integer idx : rowkeyColumnIndexes) {
            Object field = row.getField(idx);
            parts.add(field == null ? "" : field.toString());
        }
        return ByteString.copyFromUtf8(String.join(delimiter, parts));
    }

    private ByteString fieldToByteString(SeaTunnelRow row, int index) {
        Object field = row.getField(index);
        if (field == null) {
            return ByteString.EMPTY;
        }
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(index);
        if (fieldType.getSqlType() == org.apache.seatunnel.api.table.type.SqlType.BYTES) {
            return ByteString.copyFrom((byte[]) field);
        }
        return ByteString.copyFromUtf8(field.toString());
    }

    private String resolveFamily(String fieldName) {
        Map<String, String> familyMap = parameters.getColumnFamily();
        if (familyMap == null) {
            return defaultFamily;
        }
        return familyMap.getOrDefault(fieldName, defaultFamily);
    }

    private ByteString convertToByteString(SeaTunnelRow row, int index) {
        Object field = row.getField(index);
        SeaTunnelDataType<?> fieldType = rowType.getFieldType(index);
        switch (fieldType.getSqlType()) {
            case TINYINT:
                return ByteString.copyFrom(new byte[] {(Byte) field});
            case SMALLINT:
                short sv = (Short) field;
                return ByteString.copyFrom(new byte[] {(byte) (sv >> 8), (byte) sv});
            case INT:
                int iv = (Integer) field;
                return ByteString.copyFrom(ByteBuffer.allocate(4).putInt(iv).array());
            case BIGINT:
                long lv = (Long) field;
                return ByteString.copyFrom(ByteBuffer.allocate(8).putLong(lv).array());
            case FLOAT:
                float fv = (Float) field;
                return ByteString.copyFrom(ByteBuffer.allocate(4).putFloat(fv).array());
            case DOUBLE:
                double dv = (Double) field;
                return ByteString.copyFrom(ByteBuffer.allocate(8).putDouble(dv).array());
            case BOOLEAN:
                return ByteString.copyFrom(new byte[] {(byte) ((Boolean) field ? 1 : 0)});
            case BYTES:
                return ByteString.copyFrom((byte[]) field);
            case DECIMAL:
                BigDecimal bd =
                        field instanceof BigDecimal
                                ? (BigDecimal) field
                                : new BigDecimal(field.toString());
                return ByteString.copyFrom(bd.toPlainString().getBytes(StandardCharsets.UTF_8));
            case DATE:
                LocalDate date =
                        field instanceof LocalDate
                                ? (LocalDate) field
                                : DateUtils.parse(field.toString());
                return ByteString.copyFrom(
                        DateUtils.toString(date, DateUtils.Formatter.YYYY_MM_DD)
                                .getBytes(StandardCharsets.UTF_8));
            case TIME:
                LocalTime time =
                        field instanceof LocalTime
                                ? (LocalTime) field
                                : TimeUtils.parse(field.toString());
                return ByteString.copyFrom(
                        TimeUtils.toString(time, TimeUtils.Formatter.HH_MM_SS)
                                .getBytes(StandardCharsets.UTF_8));
            case TIMESTAMP:
                LocalDateTime ts =
                        field instanceof LocalDateTime
                                ? (LocalDateTime) field
                                : DateTimeUtils.parse(field.toString());
                return ByteString.copyFrom(
                        DateTimeUtils.toString(ts, DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS)
                                .getBytes(StandardCharsets.UTF_8));
            case STRING:
                return ByteString.copyFromUtf8(field.toString());
            default:
                throw new BigtableConnectorException(
                        BigtableConnectorErrorCode.WRITE_FAILED,
                        String.format(
                                "Bigtable connector does not support column type [%s]",
                                fieldType.getSqlType()));
        }
    }
}
