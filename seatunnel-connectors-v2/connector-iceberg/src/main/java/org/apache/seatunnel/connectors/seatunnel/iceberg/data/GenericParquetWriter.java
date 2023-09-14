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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.iceberg.data.parquet.BaseParquetWriter;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class GenericParquetWriter extends BaseParquetWriter<SeaTunnelRow> {

    private SeaTunnelRowType seaTunnelRowType;

    private MessageType messageType;

    private GenericParquetWriter(SeaTunnelRowType seaTunnelRowType, MessageType messageType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.messageType = messageType;
    }

    public static ParquetValueWriter<SeaTunnelRow> buildWriter(
            MessageType type, SeaTunnelRowType rowType) {
        return new GenericParquetWriter(rowType, type).createWriter(type);
    }

    @Override
    protected StructWriter<SeaTunnelRow> createStructWriter(List<ParquetValueWriter<?>> writers) {
        Preconditions.checkArgument(
                writers.size() == seaTunnelRowType.getTotalFields(),
                "Invalid number of writers: %s (expected: %s)",
                writers.size(),
                seaTunnelRowType.getTotalFields());

        List<ParquetValueWriter<?>> newWriters = new ArrayList<>(writers.size());
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            ColumnDescriptor desc = messageType.getColumns().get(i);
            PrimitiveType primitiveType = desc.getPrimitiveType();
            switch (seaTunnelRowType.getFieldTypes()[i].getSqlType()) {
                case STRING:
                    newWriters.add(ParquetValueWriters.strings(desc));
                    break;
                case BOOLEAN:
                    newWriters.add(ParquetValueWriters.booleans(desc));
                    break;
                case TINYINT:
                    newWriters.add(ParquetValueWriters.tinyints(desc));
                    break;
                case SMALLINT:
                    newWriters.add(ParquetValueWriters.shorts(desc));
                    break;
                case INT:
                    newWriters.add(ParquetValueWriters.ints(desc));
                    break;
                case BIGINT:
                    newWriters.add(ParquetValueWriters.longs(desc));
                    break;
                case FLOAT:
                    newWriters.add(ParquetValueWriters.floats(desc));
                    break;
                case DOUBLE:
                    newWriters.add(ParquetValueWriters.doubles(desc));
                    break;
                case DECIMAL:
                    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                                    primitiveType.getLogicalTypeAnnotation();
                    switch (primitiveType.getPrimitiveTypeName()) {
                        case INT32:
                            newWriters.add(
                                    decimalAsInteger(
                                            desc, decimal.getPrecision(), decimal.getScale()));
                            break;
                        case INT64:
                            newWriters.add(
                                    decimalAsLong(
                                            desc, decimal.getPrecision(), decimal.getScale()));
                            break;
                        case BINARY:
                        case FIXED_LEN_BYTE_ARRAY:
                            newWriters.add(
                                    decimalAsFixed(
                                            desc, decimal.getPrecision(), decimal.getScale()));
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported base type for decimal: "
                                            + primitiveType.getPrimitiveTypeName());
                    }
                    break;
                case BYTES:
                    newWriters.add(new ByteArrayWriter(desc));
                    break;
                case DATE:
                    newWriters.add(new DateWriter(desc));
                    break;
                case TIME:
                    newWriters.add(new TimeWriter(desc));
                    break;
                case TIMESTAMP:
                    newWriters.add(new TimestampWriter(desc));
                    break;

                default:
                    throw new UnsupportedOperationException(
                            "Unsupported type: "
                                    + seaTunnelRowType.getFieldTypes()[i].getSqlType());
            }
        }

        // add option writer
        for (int i = 0; i < messageType.getColumns().size(); i++) {
            ColumnDescriptor columnDescriptor = messageType.getColumns().get(i);
            newWriters.set(
                    i,
                    ParquetValueWriters.option(
                            columnDescriptor.getPrimitiveType(),
                            columnDescriptor.getMaxDefinitionLevel(),
                            newWriters.get(i)));
        }

        return new SeaTunnelRowWriter(newWriters);
    }

    private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
    private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

    public static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsInteger(
            ColumnDescriptor desc, int precision, int scale) {
        return new IntegerDecimalWriter(desc, precision, scale);
    }

    public static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsLong(
            ColumnDescriptor desc, int precision, int scale) {
        return new LongDecimalWriter(desc, precision, scale);
    }

    public static ParquetValueWriters.PrimitiveWriter<BigDecimal> decimalAsFixed(
            ColumnDescriptor desc, int precision, int scale) {
        return new FixedDecimalWriter(desc, precision, scale);
    }

    private static class IntegerDecimalWriter
            extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
        private final int precision;
        private final int scale;

        private IntegerDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void write(int repetitionLevel, BigDecimal decimal) {
            decimal = decimal.setScale(scale);
            Preconditions.checkArgument(
                    decimal.scale() == scale,
                    "Cannot write value as decimal(%s,%s), wrong scale: %s",
                    precision,
                    scale,
                    decimal);
            Preconditions.checkArgument(
                    decimal.precision() <= precision,
                    "Cannot write value as decimal(%s,%s), too large: %s",
                    precision,
                    scale,
                    decimal);

            column.writeInteger(repetitionLevel, decimal.unscaledValue().intValue());
        }
    }

    private static class LongDecimalWriter extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
        private final int precision;
        private final int scale;

        private LongDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public void write(int repetitionLevel, BigDecimal decimal) {
            decimal = decimal.setScale(scale);
            Preconditions.checkArgument(
                    decimal.scale() == scale,
                    "Cannot write value as decimal(%s,%s), wrong scale: %s",
                    precision,
                    scale,
                    decimal);
            Preconditions.checkArgument(
                    decimal.precision() <= precision,
                    "Cannot write value as decimal(%s,%s), too large: %s",
                    precision,
                    scale,
                    decimal);

            column.writeLong(repetitionLevel, decimal.unscaledValue().longValue());
        }
    }

    private static class FixedDecimalWriter
            extends ParquetValueWriters.PrimitiveWriter<BigDecimal> {
        private final int precision;
        private final int scale;
        private final ThreadLocal<byte[]> bytes;

        private FixedDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
            super(desc);
            this.precision = precision;
            this.scale = scale;
            this.bytes =
                    ThreadLocal.withInitial(
                            () -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
        }

        @Override
        public void write(int repetitionLevel, BigDecimal decimal) {
            decimal = decimal.setScale(scale);
            byte[] binary =
                    DecimalUtil.toReusedFixLengthBytes(precision, scale, decimal, bytes.get());
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(binary));
        }
    }

    private static class DateWriter extends ParquetValueWriters.PrimitiveWriter<LocalDate> {
        private DateWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, LocalDate value) {
            column.writeInteger(repetitionLevel, (int) ChronoUnit.DAYS.between(EPOCH_DAY, value));
        }
    }

    private static class TimeWriter extends ParquetValueWriters.PrimitiveWriter<LocalTime> {
        private TimeWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, LocalTime value) {
            column.writeLong(repetitionLevel, value.toNanoOfDay() / 1000);
        }
    }

    private static class TimestampWriter
            extends ParquetValueWriters.PrimitiveWriter<LocalDateTime> {
        private TimestampWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, LocalDateTime value) {
            column.writeLong(
                    repetitionLevel,
                    ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
        }
    }

    private static class ByteArrayWriter extends ParquetValueWriters.PrimitiveWriter<byte[]> {
        private ByteArrayWriter(ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public void write(int repetitionLevel, byte[] bytes) {
            column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(bytes));
        }
    }

    private static class SeaTunnelRowWriter extends StructWriter<SeaTunnelRow> {
        private SeaTunnelRowWriter(List<ParquetValueWriter<?>> writers) {
            super(writers);
        }

        @Override
        protected Object get(SeaTunnelRow struct, int index) {
            return struct.getField(index);
        }
    }
}
