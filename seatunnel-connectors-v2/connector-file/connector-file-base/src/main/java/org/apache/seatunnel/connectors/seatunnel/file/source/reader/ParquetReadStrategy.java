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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
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
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {
    private static final byte[] PARQUET_MAGIC = new byte[]{(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};
    private static final long NANOS_PER_MILLISECOND = 1000000;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;

    @Override
    public void read(String path, Collector<SeaTunnelRow> output) throws FileConnectorException, IOException {
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg = String.format("This file [%s] is not a parquet file, please check the format of this file", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(filePath, getConfiguration());
        int fieldsCount = seaTunnelRowType.getTotalFields();
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        GenericRecord record;
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(hadoopInputFile)
                .withDataModel(dataModel)
                .build()) {
            while ((record = reader.read()) != null) {
                Object[] fields;
                if (isMergePartition) {
                    int index = fieldsCount;
                    fields = new Object[fieldsCount + partitionsMap.size()];
                    for (String value : partitionsMap.values()) {
                        fields[index++] = value;
                    }
                } else {
                    fields = new Object[fieldsCount];
                }
                for (int i = 0; i < fieldsCount; i++) {
                    Object data = record.get(i);
                    fields[i] = resolveObject(data, seaTunnelRowType.getFieldType(i));
                }
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                output.collect(seaTunnelRow);
            }
        }
    }

    private Object resolveObject(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                ArrayList<Object> origArray = new ArrayList<>();
                ((GenericData.Array<?>) field).iterator().forEachRemaining(origArray::add);
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                switch (elementType.getSqlType()) {
                    case STRING:
                        return origArray.toArray(TYPE_ARRAY_STRING);
                    case BOOLEAN:
                        return origArray.toArray(TYPE_ARRAY_BOOLEAN);
                    case TINYINT:
                        return origArray.toArray(TYPE_ARRAY_BYTE);
                    case SMALLINT:
                        return origArray.toArray(TYPE_ARRAY_SHORT);
                    case INT:
                        return origArray.toArray(TYPE_ARRAY_INTEGER);
                    case BIGINT:
                        return origArray.toArray(TYPE_ARRAY_LONG);
                    case FLOAT:
                        return origArray.toArray(TYPE_ARRAY_FLOAT);
                    case DOUBLE:
                        return origArray.toArray(TYPE_ARRAY_DOUBLE);
                    default:
                        String errorMsg = String.format("SeaTunnel array type not support this type [%s] now", fieldType.getSqlType());
                        throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
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
            case DECIMAL:
            case DATE:
                return field;
            case STRING:
                return field.toString();
            case TINYINT:
                return Byte.parseByte(field.toString());
            case SMALLINT:
                return Short.parseShort(field.toString());
            case NULL:
                return null;
            case BYTES:
                ByteBuffer buffer = (ByteBuffer) field;
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes, 0, bytes.length);
                return bytes;
            case TIMESTAMP:
                if (field instanceof GenericData.Fixed) {
                    Binary binary = Binary.fromConstantByteArray(((GenericData.Fixed) field).bytes());
                    NanoTime nanoTime = NanoTime.fromBinary(binary);
                    int julianDay = nanoTime.getJulianDay();
                    long nanosOfDay = nanoTime.getTimeOfDayNanos();
                    long timestamp = (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLISECOND;
                    return new Timestamp(timestamp).toLocalDateTime();
                }
                Instant instant = Instant.ofEpochMilli((long) field);
                return LocalDateTime.ofInstant(instant, ZoneId.of("+8"));
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
                throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "SeaTunnel not support this data type now");
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(HadoopConf hadoopConf, String path) throws FileConnectorException {
        Path filePath = new Path(path);
        ParquetMetadata metadata;
        try {
            HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(filePath, getConfiguration(hadoopConf));
            ParquetFileReader reader = ParquetFileReader.open(hadoopInputFile);
            metadata = reader.getFooter();
            reader.close();
        } catch (IOException e) {
            String errorMsg = String.format("Create parquet reader for this file [%s] failed", path);
            throw new FileConnectorException(CommonErrorCode.READER_OPERATION_FAILED, errorMsg, e);
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
        seaTunnelRowTypeWithPartition = mergePartitionTypes(path, seaTunnelRowType);
        return getActualSeaTunnelRowTypeInfo();
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
                            throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
                    }
                case INT64:
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
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
                    if (type.getLogicalTypeAnnotation() == null) {
                        return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                    }
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
                    throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
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
                                throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
                        }
                    default:
                        throw new FileConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "SeaTunnel file connector not support this nest type");
                }
            }
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
        } catch (IOException e) {
            String errorMsg = String.format("Check parquet file [%s] failed", path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
    }
}
