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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FilePluginException;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {

    private SeaTunnelRowType seaTunnelRowType;

    private static final byte[] PARQUET_MAGIC = new byte[]{(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};
    private static final long NANOS_PER_MILLISECOND = 1000000;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws Exception {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            throw new Exception("please check file type");
        }
        Path filePath = new Path(path);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(filePath, getConfiguration());
        int fieldsCount = seaTunnelRowType.getTotalFields();
        GenericRecord record;
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(hadoopInputFile).build()) {
            while ((record = reader.read()) != null) {
                Object[] fields = new Object[fieldsCount];
                for (int i = 0; i < fieldsCount; i++) {
                    Object data = record.get(i);
                    fields[i] = resolveObject(data, seaTunnelRowType.getFieldType(i));
                }
                output.collect(new SeaTunnelRow(fields));
            }
        }
    }

    private Object resolveObject(Object field, SeaTunnelDataType<?> fieldType) {
        switch (fieldType.getSqlType()) {
            case ARRAY:
                List<Object> origArray = ((ArrayList<Object>) field).stream().map(item -> {
                    try {
                        return ((GenericData.Record) item).get("array_element");
                    } catch (Exception e) {
                        return item;
                    }
                }).collect(Collectors.toList());
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return origArray.toArray(new String[0]);
                    case BOOLEAN:
                        return origArray.toArray(new Boolean[0]);
                    case TINYINT:
                        return origArray.toArray(new Byte[0]);
                    case SMALLINT:
                        return origArray.toArray(new Short[0]);
                    case INT:
                        return origArray.toArray(new Integer[0]);
                    case BIGINT:
                        return origArray.toArray(new Long[0]);
                    case FLOAT:
                        return origArray.toArray(new Float[0]);
                    case DOUBLE:
                        return origArray.toArray(new Double[0]);
                    default:
                        String errorMsg = String.format("SeaTunnel array type not support this type [%s] now", fieldType.getSqlType());
                        throw new UnsupportedOperationException(errorMsg);
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach((key, value) -> dataMap.put(resolveObject(key, keyType), resolveObject(value, valueType)));
                return dataMap;
            case BOOLEAN:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return field;
            case STRING:
                return field.toString();
            case TINYINT:
                return Byte.parseByte(field.toString());
            case SMALLINT:
                return Short.parseShort(field.toString());
            case DECIMAL:
                int precision = ((DecimalType) fieldType).getPrecision();
                int scale = ((DecimalType) fieldType).getScale();
                return bytes2Decimal(((GenericData.Fixed) field).bytes(), precision, scale);
            case NULL:
                return null;
            case BYTES:
                ByteBuffer buffer = (ByteBuffer) field;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes, 0, bytes.length);
                return bytes;
            case DATE:
                return LocalDate.ofEpochDay(Long.parseLong(field.toString()));
            case TIMESTAMP:
                Binary binary = Binary.fromConstantByteArray(((GenericData.Fixed) field).bytes());
                NanoTime nanoTime = NanoTime.fromBinary(binary);
                int julianDay = nanoTime.getJulianDay();
                long nanosOfDay = nanoTime.getTimeOfDayNanos();
                long timestamp = (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLISECOND;
                return new Timestamp(timestamp).toLocalDateTime();
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
                Object[] objects = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    SeaTunnelDataType<?> dataType = rowType.getFieldType(i);
                    objects[i] = resolveObject(((GenericRecord) field).get(i), dataType);
                }
                return new SeaTunnelRow(objects);
            default:
                // do nothing
                // never got in there
                throw new UnsupportedOperationException("SeaTunnel not support this data type now");
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) throws FilePluginException {
        if (seaTunnelRowType != null) {
            return seaTunnelRowType;
        }
        Path filePath = new Path(path);
        ParquetMetadata metadata;
        try {
            HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(filePath, getConfiguration());
            ParquetFileReader reader = ParquetFileReader.open(hadoopInputFile);
            metadata = reader.getFooter();
            reader.close();
        } catch (IOException e) {
            throw new FilePluginException("Create parquet reader failed", e);
        }
        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType schema = fileMetaData.getSchema();
        int fieldCount = schema.getFieldCount();
        String[] fields = new String[fieldCount];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fields[i] = schema.getFieldName(i);
            Type type = schema.getType(i);
            SeaTunnelDataType<?> fieldType = parquetType2SeaTunnelType(type);
            types[i] = fieldType;
        }
        seaTunnelRowType = new SeaTunnelRowType(fields, types);
        return seaTunnelRowType;
    }

    private SeaTunnelDataType<?> parquetType2SeaTunnelType(Type type) {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    OriginalType originalType = type.asPrimitiveType().getOriginalType();
                    if (originalType == null) {
                        return BasicType.INT_TYPE;
                    }
                    switch (type.asPrimitiveType().getOriginalType()) {
                        case INT_8:
                            return BasicType.BYTE_TYPE;
                        case INT_16:
                            return BasicType.SHORT_TYPE;
                        case DATE:
                            return LocalTimeType.LOCAL_DATE_TYPE;
                        default:
                            String errorMsg = String.format("Not support this type [%s]", type);
                            throw new UnsupportedOperationException(errorMsg);
                    }
                case INT64:
                    return BasicType.LONG_TYPE;
                case INT96:
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                case BINARY:
                    if (type.asPrimitiveType().getOriginalType() == null) {
                        return PrimitiveByteArrayType.INSTANCE;
                    }
                    return BasicType.STRING_TYPE;
                case FLOAT:
                    return BasicType.FLOAT_TYPE;
                case DOUBLE:
                    return BasicType.DOUBLE_TYPE;
                case BOOLEAN:
                    return BasicType.BOOLEAN_TYPE;
                case FIXED_LEN_BYTE_ARRAY:
                    String typeInfo = type.getLogicalTypeAnnotation().toString()
                            .replaceAll(SqlType.DECIMAL.toString(), "")
                            .replaceAll("\\(", "")
                            .replaceAll("\\)", "");
                    String[] splits = typeInfo.split(",");
                    int precision = Integer.parseInt(splits[0]);
                    int scale = Integer.parseInt(splits[1]);
                    return new DecimalType(precision, scale);
                default:
                    String errorMsg = String.format("Not support this type [%s]", type);
                    throw new UnsupportedOperationException(errorMsg);
            }
        } else {
            LogicalTypeAnnotation logicalTypeAnnotation = type.asGroupType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                // struct type
                List<Type> fields = type.asGroupType().getFields();
                String[] fieldNames = new String[fields.size()];
                SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i);
                    SeaTunnelDataType<?> seaTunnelDataType = parquetType2SeaTunnelType(fields.get(i));
                    fieldNames[i] = fieldType.getName();
                    seaTunnelDataTypes[i] = seaTunnelDataType;
                }
                return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
            } else {
                switch (logicalTypeAnnotation.toOriginalType()) {
                    case MAP:
                        GroupType groupType = type.asGroupType().getType(0).asGroupType();
                        SeaTunnelDataType<?> keyType = parquetType2SeaTunnelType(groupType.getType(0));
                        SeaTunnelDataType<?> valueType = parquetType2SeaTunnelType(groupType.getType(1));
                        return new MapType<>(keyType, valueType);
                    case LIST:
                        Type elementType;
                        try {
                            elementType = type.asGroupType().getType(0).asGroupType().getType(0);
                        } catch (Exception e) {
                            elementType = type.asGroupType().getType(0);
                        }
                        SeaTunnelDataType<?> fieldType = parquetType2SeaTunnelType(elementType);
                        switch (fieldType.getSqlType()) {
                            case STRING:
                                return ArrayType.STRING_ARRAY_TYPE;
                            case BOOLEAN:
                                return ArrayType.BOOLEAN_ARRAY_TYPE;
                            case TINYINT:
                                return ArrayType.BYTE_ARRAY_TYPE;
                            case SMALLINT:
                                return ArrayType.SHORT_ARRAY_TYPE;
                            case INT:
                                return ArrayType.INT_ARRAY_TYPE;
                            case BIGINT:
                                return ArrayType.LONG_ARRAY_TYPE;
                            case FLOAT:
                                return ArrayType.FLOAT_ARRAY_TYPE;
                            case DOUBLE:
                                return ArrayType.DOUBLE_ARRAY_TYPE;
                            default:
                                String errorMsg = String.format("SeaTunnel array type not supported this genericType [%s] yet", fieldType);
                                throw new UnsupportedOperationException(errorMsg);
                        }
                    default:
                        throw new UnsupportedOperationException("SeaTunnel file connector not support this nest type");
                }
            }
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private BigDecimal bytes2Decimal(byte[] bytesArray, int precision, int scale) {
        Binary value = Binary.fromConstantByteArray(bytesArray);
        if (precision <= 18) {
            ByteBuffer buffer = value.toByteBuffer();
            byte[] bytes = buffer.array();
            int start = buffer.arrayOffset() + buffer.position();
            int end = buffer.arrayOffset() + buffer.limit();
            long unscaled = 0L;
            int i = start;
            while (i < end) {
                unscaled = unscaled << 8 | bytes[i] & 0xff;
                i++;
            }
            int bits = 8 * (end - start);
            long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
            if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
                return new BigDecimal(unscaledNew);
            } else {
                return BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
            }
        } else {
            return new BigDecimal(new BigInteger(value.getBytes()), scale);
        }
    }

    @Override
    boolean checkFileType(String path) {
        boolean checkResult;
        byte[] magic = new byte[PARQUET_MAGIC.length];
        try {
            Configuration configuration = getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            Path filePath = new Path(path);
            FSDataInputStream in = fileSystem.open(filePath);
            // try to get header information in a parquet file
            in.seek(0);
            in.readFully(magic);
            checkResult = Arrays.equals(magic, PARQUET_MAGIC);
            in.close();
            return checkResult;
        } catch (FilePluginException | IOException e) {
            String errorMsg = String.format("Check parquet file [%s] error", path);
            throw new RuntimeException(errorMsg, e);
        }
    }
}
