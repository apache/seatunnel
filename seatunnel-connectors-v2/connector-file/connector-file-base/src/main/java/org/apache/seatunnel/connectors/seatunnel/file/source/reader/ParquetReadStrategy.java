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
import org.apache.seatunnel.api.table.catalog.TablePath;
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
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.apache.avro.Conversions;
import org.apache.avro.SchemaParseException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.apache.seatunnel.api.table.type.TypeUtil.canConvert;

@Slf4j
public class ParquetReadStrategy extends AbstractReadStrategy {
    private static final byte[] PARQUET_MAGIC =
            new byte[] {(byte) 'P', (byte) 'A', (byte) 'R', (byte) '1'};
    private static final long NANOS_PER_MILLISECOND = 1000000;
    private static final long MILLIS_PER_DAY = TimeUnit.DAYS.toMillis(1L);
    private static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
    private static final String PARQUET = "Parquet";

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        this.read(new FileSourceSplit(path), output);
    }

    @Override
    public void read(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        try {
            readWithAvro(split, output);
        } catch (RuntimeException e) {
            if (!isIllegalAvroFieldNameException(e)) {
                throw e;
            }
            log.warn(
                    "Failed to read parquet file [{}] with Avro reader due to illegal Avro field"
                            + " name, fallback to native parquet reader",
                    split.getFilePath(),
                    e);
            readWithNativeParquet(split, output);
        }
    }

    private void readWithAvro(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException, FileConnectorException {
        String tableId = split.getTableId();
        String path = split.getFilePath();
        if (Boolean.FALSE.equals(checkFileType(path))) {
            String errorMsg =
                    String.format(
                            "This file [%s] is not a parquet file, please check the format of this file",
                            path);
            throw new FileConnectorException(FileConnectorErrorCode.FILE_TYPE_INVALID, errorMsg);
        }
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        HadoopInputFile hadoopInputFile =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        (configuration, userGroupInformation) ->
                                HadoopInputFile.fromPath(filePath, configuration));
        int fieldsCount = seaTunnelRowType.getTotalFields();
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        final boolean useSplitRange =
                enableSplitFile && split.getStart() >= 0 && split.getLength() > 0;
        GenericRecord record;
        AvroParquetReader.Builder<GenericData.Record> builder =
                AvroParquetReader.<GenericData.Record>builder(hadoopInputFile)
                        .withDataModel(dataModel);
        if (useSplitRange) {
            long start = split.getStart();
            long end = start + split.getLength();
            builder.withFileRange(start, end);
        }
        try (ParquetReader<GenericData.Record> reader = builder.build()) {
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
                    String fieldName = seaTunnelRowType.getFieldName(i);
                    Object data = record.hasField(fieldName) ? record.get(fieldName) : null;
                    fields[i] = resolveObject(data, seaTunnelRowType.getFieldType(i));
                }
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                seaTunnelRow.setTableId(tableId);
                output.collect(seaTunnelRow);
            }
        }
    }

    private void readWithNativeParquet(FileSourceSplit split, Collector<SeaTunnelRow> output)
            throws IOException {
        String tableId = split.getTableId();
        String path = split.getFilePath();
        Path filePath = new Path(path);
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        int fieldsCount = seaTunnelRowType.getTotalFields();
        final boolean useSplitRange =
                enableSplitFile && split.getStart() >= 0 && split.getLength() > 0;
        ParquetReader<Group> reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        (configuration, userGroupInformation) -> {
                            ParquetReader.Builder<Group> builder =
                                    ParquetReader.builder(new GroupReadSupport(), filePath)
                                            .withConf(configuration);
                            if (useSplitRange) {
                                long start = split.getStart();
                                long end = start + split.getLength();
                                builder.withFileRange(start, end);
                            }
                            return builder.build();
                        });
        try (ParquetReader<Group> closeableReader = reader) {
            Group record;
            while ((record = closeableReader.read()) != null) {
                GroupType recordType = record.getType();
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
                    String fieldName = seaTunnelRowType.getFieldName(i);
                    if (!recordType.containsField(fieldName)) {
                        fields[i] = null;
                        continue;
                    }
                    int fieldIndex = recordType.getFieldIndex(fieldName);
                    fields[i] =
                            resolveGroupObject(
                                    record,
                                    recordType.getType(fieldIndex),
                                    fieldIndex,
                                    seaTunnelRowType.getFieldType(i));
                }
                SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fields);
                seaTunnelRow.setTableId(tableId);
                output.collect(seaTunnelRow);
            }
        }
    }

    private boolean isIllegalAvroFieldNameException(Throwable throwable) {
        while (throwable != null) {
            String message = throwable.getMessage();
            if ((throwable instanceof SchemaParseException
                            || throwable.getClass().getName().endsWith(".SchemaParseException"))
                    && message != null
                    && message.contains("Illegal character in")) {
                return true;
            }
            throwable = throwable.getCause();
        }
        return false;
    }

    private Object resolveGroupObject(
            Group group, Type parquetType, int fieldIndex, SeaTunnelDataType<?> fieldType) {
        if (group.getFieldRepetitionCount(fieldIndex) == 0) {
            return null;
        }
        if (!parquetType.isPrimitive()) {
            return resolveGroupType(group, parquetType, fieldIndex, fieldType);
        }
        Object field = readPrimitiveGroupObject(group, parquetType.asPrimitiveType(), fieldIndex);
        if (field instanceof LocalDateTime || field instanceof LocalDate) {
            return field;
        }
        return resolveObject(field, fieldType);
    }

    private Object resolveGroupType(
            Group group, Type parquetType, int fieldIndex, SeaTunnelDataType<?> fieldType) {
        GroupType groupType = parquetType.asGroupType();
        LogicalTypeAnnotation logicalTypeAnnotation = groupType.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            SeaTunnelRowType rowType = (SeaTunnelRowType) fieldType;
            Group childGroup = group.getGroup(fieldIndex, 0);
            Object[] objects = new Object[rowType.getTotalFields()];
            for (int i = 0; i < rowType.getTotalFields(); i++) {
                objects[i] =
                        resolveGroupObject(
                                childGroup, groupType.getType(i), i, rowType.getFieldType(i));
            }
            return new SeaTunnelRow(objects);
        }
        OriginalType originalType = logicalTypeAnnotation.toOriginalType();
        if (originalType == OriginalType.LIST) {
            return readList(group, groupType, fieldIndex, fieldType);
        }
        if (originalType == OriginalType.MAP) {
            return readMap(group, groupType, fieldIndex, fieldType);
        }
        throw CommonError.convertToSeaTunnelTypeError(
                PARQUET, parquetType.toString(), parquetType.getName());
    }

    /**
     * Reads a LIST field from a Parquet Group using the native Group API. Handles both the standard
     * 3-level LIST encoding (group → repeated list → element) and the legacy 2-level encoding
     * (group → repeated element directly).
     */
    private Object readList(
            Group group,
            GroupType parentGroupType,
            int listFieldIndex,
            SeaTunnelDataType<?> fieldType) {
        // Check if the LIST field is present
        if (group.getFieldRepetitionCount(listFieldIndex) == 0) {
            return new Object[0];
        }

        ArrayType<?, ?> arrayType = (ArrayType<?, ?>) fieldType;
        SeaTunnelDataType<?> elementType = arrayType.getElementType();
        Group listGroup = group.getGroup(listFieldIndex, 0);
        GroupType listGroupType = parentGroupType;

        Type repeatedType = listGroupType.getType(0);
        // Number of repeated elements within the LIST group
        int numElements = listGroup.getFieldRepetitionCount(0);

        // Determine 3-level vs 2-level LIST encoding.
        // 3-level: LIST group → REPEATED group (1 field) → element
        // 2-level: LIST group → REPEATED element directly (legacy)
        boolean isThreeLevel = false;
        if (!repeatedType.isPrimitive()) {
            GroupType repeatedGroupType = repeatedType.asGroupType();
            LogicalTypeAnnotation repeatedAnnotation = repeatedGroupType.getLogicalTypeAnnotation();
            if (repeatedAnnotation == null
                    || (repeatedAnnotation.toOriginalType() != OriginalType.LIST
                            && repeatedAnnotation.toOriginalType() != OriginalType.MAP)) {
                if (repeatedGroupType.getFieldCount() == 1) {
                    isThreeLevel = true;
                }
            }
        }

        List<Object> result = new ArrayList<>(numElements);
        if (isThreeLevel) {
            read3LevelList(
                    listGroup, listGroupType, repeatedType, numElements, elementType, result);
        } else {
            read2LevelList(
                    listGroup, repeatedType, numElements, elementType, listGroupType, result);
        }
        return convertListResult(result, elementType);
    }

    private Object convertListResult(List<Object> result, SeaTunnelDataType<?> elementType) {
        switch (elementType.getSqlType()) {
            case STRING:
                return result.toArray(TYPE_ARRAY_STRING);
            case BOOLEAN:
                return result.toArray(TYPE_ARRAY_BOOLEAN);
            case TINYINT:
                return result.toArray(TYPE_ARRAY_BYTE);
            case SMALLINT:
                return result.toArray(TYPE_ARRAY_SHORT);
            case INT:
                return result.toArray(TYPE_ARRAY_INTEGER);
            case BIGINT:
                return result.toArray(TYPE_ARRAY_LONG);
            case FLOAT:
                return result.toArray(TYPE_ARRAY_FLOAT);
            case DOUBLE:
                return result.toArray(TYPE_ARRAY_DOUBLE);
            case DECIMAL:
                return result.toArray(TYPE_ARRAY_BIG_DECIMAL);
            case DATE:
                return result.toArray(TYPE_ARRAY_LOCAL_DATE);
            case TIMESTAMP:
                return result.toArray(TYPE_ARRAY_LOCAL_DATETIME);
            case BYTES:
                byte[][] bytesArray = new byte[result.size()][];
                for (int i = 0; i < result.size(); i++) {
                    Object element = result.get(i);
                    if (element instanceof ByteBuffer) {
                        ByteBuffer buffer = (ByteBuffer) element;
                        byte[] bytes = new byte[buffer.remaining()];
                        buffer.get(bytes, 0, bytes.length);
                        bytesArray[i] = bytes;
                    } else if (element instanceof byte[]) {
                        bytesArray[i] = (byte[]) element;
                    }
                }
                return bytesArray;
            default:
                return result.toArray(new Object[0]);
        }
    }

    /**
     * Reads elements from a 3-level encoded Parquet LIST. Structure: listGroup → REPEATED wrapper
     * (field 0) → actual element (field 0 within wrapper).
     */
    private void read3LevelList(
            Group listGroup,
            GroupType listGroupType,
            Type repeatedType,
            int numElements,
            SeaTunnelDataType<?> elementType,
            List<Object> result) {
        Type elementTypeInParquet = repeatedType.asGroupType().getType(0);
        for (int i = 0; i < numElements; i++) {
            Group repeatedInstance = listGroup.getGroup(0, i);
            if (repeatedInstance.getFieldRepetitionCount(0) == 0) {
                result.add(null);
            } else {
                result.add(
                        resolveGroupObject(repeatedInstance, elementTypeInParquet, 0, elementType));
            }
        }
    }

    /**
     * Reads elements from a 2-level encoded Parquet LIST (legacy). Structure: listGroup → REPEATED
     * element directly at field index 0, with each instance accessed by repetition index.
     */
    private void read2LevelList(
            Group listGroup,
            Type repeatedType,
            int numElements,
            SeaTunnelDataType<?> elementType,
            GroupType listGroupType,
            List<Object> result) {
        if (repeatedType.isPrimitive()) {
            // 2-level LIST with primitive repeated elements: read each instance by index
            PrimitiveType primitiveElementType = repeatedType.asPrimitiveType();
            for (int i = 0; i < numElements; i++) {
                result.add(readPrimitiveGroupObjectByIndex(listGroup, primitiveElementType, 0, i));
            }
        } else {
            // 2-level LIST with group repeated elements (e.g., LIST of ROW)
            GroupType elementGroupType = repeatedType.asGroupType();
            LogicalTypeAnnotation elemAnnotation = elementGroupType.getLogicalTypeAnnotation();
            if (elemAnnotation != null) {
                throw CommonError.convertToSeaTunnelTypeError(
                        PARQUET,
                        "2-level LIST with annotated group element",
                        listGroupType.getName());
            }
            SeaTunnelRowType rowType = (SeaTunnelRowType) elementType;
            for (int i = 0; i < numElements; i++) {
                Group elementGroup = listGroup.getGroup(0, i);
                Object[] objects = new Object[rowType.getTotalFields()];
                for (int j = 0; j < rowType.getTotalFields(); j++) {
                    objects[j] =
                            resolveGroupObject(
                                    elementGroup,
                                    elementGroupType.getType(j),
                                    j,
                                    rowType.getFieldType(j));
                }
                result.add(new SeaTunnelRow(objects));
            }
        }
    }

    /**
     * Reads a single primitive value from a Parquet Group at the given field index and repetition
     * index. This is needed for 2-level LIST encoding where repeated primitive elements are
     * accessed by repetition index.
     */
    private Object readPrimitiveGroupObjectByIndex(
            Group group, PrimitiveType parquetType, int fieldIndex, int repetitionIndex) {
        switch (parquetType.getPrimitiveTypeName()) {
            case BOOLEAN:
                return group.getBoolean(fieldIndex, repetitionIndex);
            case INT32:
                if (parquetType.getOriginalType() == OriginalType.DATE) {
                    return LocalDate.ofEpochDay(group.getInteger(fieldIndex, repetitionIndex));
                }
                return group.getInteger(fieldIndex, repetitionIndex);
            case INT64:
                return group.getLong(fieldIndex, repetitionIndex);
            case INT96:
                return int96ToLocalDateTime(group.getInt96(fieldIndex, repetitionIndex));
            case FLOAT:
                return group.getFloat(fieldIndex, repetitionIndex);
            case DOUBLE:
                return group.getDouble(fieldIndex, repetitionIndex);
            case BINARY:
                return readBinaryGroupObject(
                        group.getBinary(fieldIndex, repetitionIndex), parquetType);
            case FIXED_LEN_BYTE_ARRAY:
                return readFixedLenByteArrayGroupObject(
                        group.getBinary(fieldIndex, repetitionIndex), parquetType);
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        PARQUET, parquetType.toString(), parquetType.getName());
        }
    }

    /**
     * Reads a MAP field from a Parquet Group using the native Group API. Parquet MAP structure:
     * mapGroup → repeated key_value (field 0) → {key (field 0), value (field 1)}
     */
    private Object readMap(
            Group group,
            GroupType parentGroupType,
            int mapFieldIndex,
            SeaTunnelDataType<?> fieldType) {
        MapType<?, ?> mapType = (MapType<?, ?>) fieldType;
        SeaTunnelDataType<?> keyType = mapType.getKeyType();
        SeaTunnelDataType<?> valueType = mapType.getValueType();

        if (group.getFieldRepetitionCount(mapFieldIndex) == 0) {
            return new HashMap<>();
        }

        Group mapGroup = group.getGroup(mapFieldIndex, 0);
        GroupType mapGroupType = parentGroupType.getType(mapFieldIndex).asGroupType();
        GroupType keyValueGroupType = mapGroupType.getType(0).asGroupType();

        int numEntries = mapGroup.getFieldRepetitionCount(0);
        Map<Object, Object> result = new HashMap<>();
        for (int i = 0; i < numEntries; i++) {
            Group keyValue = mapGroup.getGroup(0, i);
            Object key = resolveGroupObject(keyValue, keyValueGroupType.getType(0), 0, keyType);
            Object value = null;
            if (keyValue.getFieldRepetitionCount(1) > 0) {
                value = resolveGroupObject(keyValue, keyValueGroupType.getType(1), 1, valueType);
            }
            result.put(key, value);
        }
        return result;
    }

    private Object readPrimitiveGroupObject(
            Group group, PrimitiveType parquetType, int fieldIndex) {
        switch (parquetType.getPrimitiveTypeName()) {
            case BOOLEAN:
                return group.getBoolean(fieldIndex, 0);
            case INT32:
                if (parquetType.getOriginalType() == OriginalType.DATE) {
                    return LocalDate.ofEpochDay(group.getInteger(fieldIndex, 0));
                }
                return group.getInteger(fieldIndex, 0);
            case INT64:
                return group.getLong(fieldIndex, 0);
            case INT96:
                return int96ToLocalDateTime(group.getInt96(fieldIndex, 0));
            case FLOAT:
                return group.getFloat(fieldIndex, 0);
            case DOUBLE:
                return group.getDouble(fieldIndex, 0);
            case BINARY:
                return readBinaryGroupObject(group.getBinary(fieldIndex, 0), parquetType);
            case FIXED_LEN_BYTE_ARRAY:
                return readFixedLenByteArrayGroupObject(
                        group.getBinary(fieldIndex, 0), parquetType);
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        PARQUET, parquetType.toString(), parquetType.getName());
        }
    }

    private Object readBinaryGroupObject(Binary binary, PrimitiveType parquetType) {
        if (parquetType.getOriginalType() == OriginalType.DECIMAL) {
            return binaryToDecimal(binary, parquetType);
        }
        if (parquetType.getOriginalType() == null) {
            return binary.toByteBuffer();
        }
        return binary.toStringUsingUTF8();
    }

    private Object readFixedLenByteArrayGroupObject(Binary binary, PrimitiveType parquetType) {
        if (parquetType.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
            return binaryToDecimal(binary, parquetType);
        }
        return binary.toByteBuffer();
    }

    private BigDecimal binaryToDecimal(Binary binary, PrimitiveType parquetType) {
        int scale =
                parquetType.getLogicalTypeAnnotation()
                                instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                        ? ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                                        parquetType.getLogicalTypeAnnotation())
                                .getScale()
                        : parquetType.getDecimalMetadata().getScale();
        return new BigDecimal(new BigInteger(binary.getBytes()), scale);
    }

    private LocalDateTime int96ToLocalDateTime(Binary binary) {
        NanoTime nanoTime = NanoTime.fromBinary(binary);
        int julianDay = nanoTime.getJulianDay();
        long nanosOfDay = nanoTime.getTimeOfDayNanos();
        long timestamp =
                (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY
                        + nanosOfDay / NANOS_PER_MILLISECOND;
        return new Timestamp(timestamp).toLocalDateTime();
    }

    private Object resolveObject(Object field, SeaTunnelDataType<?> fieldType) {
        if (field == null) {
            return null;
        }
        switch (fieldType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                SqlType elementSqlType = elementType.getSqlType();

                if (elementSqlType == SqlType.MAP || elementSqlType == SqlType.ARRAY) {
                    ArrayList<Object> nestedList = new ArrayList<>();
                    ((GenericData.Array<?>) field)
                            .iterator()
                            .forEachRemaining(
                                    ele -> nestedList.add(resolveObject(ele, elementType)));
                    return nestedList.toArray(new Object[0]);
                }

                ArrayList<Object> origArray = new ArrayList<>();
                ((GenericData.Array<?>) field)
                        .iterator()
                        .forEachRemaining(
                                ele -> {
                                    if (ele instanceof Utf8) {
                                        origArray.add(ele.toString());
                                    } else {
                                        origArray.add(ele);
                                    }
                                });

                switch (elementSqlType) {
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
                    case BYTES:
                        byte[][] bytesArray = new byte[origArray.size()][];
                        for (int i = 0; i < origArray.size(); i++) {
                            Object element = origArray.get(i);
                            if (element instanceof ByteBuffer) {
                                ByteBuffer buffer = (ByteBuffer) element;
                                byte[] bytes = new byte[buffer.remaining()];
                                buffer.get(bytes, 0, bytes.length);
                                bytesArray[i] = bytes;
                            } else if (element instanceof byte[]) {
                                bytesArray[i] = (byte[]) element;
                            }
                        }
                        return bytesArray;
                    default:
                        String errorMsg =
                                String.format(
                                        "SeaTunnel array type not support this type [%s] now",
                                        elementType.getSqlType());
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
                }
            case MAP:
                HashMap<Object, Object> dataMap = new HashMap<>();
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                HashMap<Object, Object> origDataMap = (HashMap<Object, Object>) field;
                origDataMap.forEach(
                        (key, value) ->
                                dataMap.put(
                                        resolveObject(key, keyType),
                                        resolveObject(value, valueType)));
                return dataMap;
            case BOOLEAN:
                return Boolean.parseBoolean(field.toString());
            case INT:
                return Integer.parseInt(field.toString());
            case BIGINT:
                return Long.parseLong(field.toString());
            case FLOAT:
                return Float.parseFloat(field.toString());
            case DOUBLE:
                return Double.parseDouble(field.toString());
            case DECIMAL:
                if (field instanceof Float || field instanceof Double) {
                    DecimalType decimalType = (DecimalType) fieldType;
                    return new BigDecimal(field.toString())
                            .setScale(decimalType.getScale(), RoundingMode.HALF_UP);
                }
                return field;
            case DATE:
                return field;
            case STRING:
                if (field instanceof ByteBuffer) {
                    ByteBuffer buffer = (ByteBuffer) field;
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes, 0, bytes.length);
                    return new String(bytes);
                }
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
                    Binary binary =
                            Binary.fromConstantByteArray(((GenericData.Fixed) field).bytes());
                    NanoTime nanoTime = NanoTime.fromBinary(binary);
                    int julianDay = nanoTime.getJulianDay();
                    long nanosOfDay = nanoTime.getTimeOfDayNanos();
                    long timestamp =
                            (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * MILLIS_PER_DAY
                                    + nanosOfDay / NANOS_PER_MILLISECOND;
                    return new Timestamp(timestamp).toLocalDateTime();
                }
                Instant instant = Instant.ofEpochMilli((long) field);
                return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
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
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel not support this data type now");
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        return getSeaTunnelRowTypeInfoWithUserConfigRowType(path, null);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(TablePath tablePath, String path)
            throws FileConnectorException {
        return getSeaTunnelRowTypeInfoWithUserConfigRowType(path, null);
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfoWithUserConfigRowType(
            String path, SeaTunnelRowType configRowType) throws FileConnectorException {
        ParquetMetadata metadata;
        try (ParquetFileReader reader =
                hadoopFileSystemProxy.doWithHadoopAuth(
                        ((configuration, userGroupInformation) -> {
                            HadoopInputFile hadoopInputFile =
                                    HadoopInputFile.fromPath(new Path(path), configuration);
                            return ParquetFileReader.open(hadoopInputFile);
                        }))) {
            metadata = reader.getFooter();
        } catch (IOException e) {
            String errorMsg =
                    String.format("Create parquet reader for this file [%s] failed", path);
            throw new FileConnectorException(
                    CommonErrorCodeDeprecated.READER_OPERATION_FAILED, errorMsg, e);
        }

        FileMetaData fileMetaData = metadata.getFileMetaData();
        MessageType originalSchema = fileMetaData.getSchema();
        if (readColumns.isEmpty()) {
            for (int i = 0; i < originalSchema.getFieldCount(); i++) {
                readColumns.add(originalSchema.getFieldName(i));
            }
        }
        String[] fields = new String[readColumns.size()];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[readColumns.size()];
        buildColumnsWithErrorCheck(
                TablePath.DEFAULT,
                IntStream.range(0, readColumns.size()).iterator(),
                i -> {
                    fields[i] = readColumns.get(i);
                    Type type = originalSchema.getType(fields[i]);
                    SeaTunnelDataType<?> configDataType =
                            getConfigFieldType(configRowType, fields[i]);
                    types[i] = parquetType2SeaTunnelType(type, configDataType, fields[i]);
                });

        seaTunnelRowType = new SeaTunnelRowType(fields, types);
        seaTunnelRowTypeWithPartition = mergePartitionTypes(path, seaTunnelRowType);

        log.debug(
                "get seatunnel row type with user config: {}. path: {}",
                getActualSeaTunnelRowTypeInfo(),
                path);
        return getActualSeaTunnelRowTypeInfo();
    }

    private SeaTunnelDataType<?> parquetType2SeaTunnelType(
            Type type, SeaTunnelDataType<?> configType, String name) {
        if (type.isPrimitive()) {
            switch (type.asPrimitiveType().getPrimitiveTypeName()) {
                case INT32:
                    OriginalType originalType = type.asPrimitiveType().getOriginalType();
                    if (originalType == null) {
                        return getFinalType(BasicType.INT_TYPE, configType);
                    }
                    switch (type.asPrimitiveType().getOriginalType()) {
                        case INT_8:
                            return getFinalType(BasicType.BYTE_TYPE, configType);
                        case INT_16:
                            return getFinalType(BasicType.SHORT_TYPE, configType);
                        case INT_32:
                            return getFinalType(BasicType.INT_TYPE, configType);
                        case DATE:
                            return getFinalType(LocalTimeType.LOCAL_DATE_TYPE, configType);
                        default:
                            throw CommonError.convertToSeaTunnelTypeError(
                                    PARQUET, type.toString(), name);
                    }
                case INT64:
                    if (type.asPrimitiveType().getOriginalType() == OriginalType.TIMESTAMP_MILLIS) {
                        return getFinalType(LocalTimeType.LOCAL_DATE_TIME_TYPE, configType);
                    }
                    return getFinalType(BasicType.LONG_TYPE, configType);
                case INT96:
                    return getFinalType(LocalTimeType.LOCAL_DATE_TIME_TYPE, configType);
                case BINARY:
                    if (type.asPrimitiveType().getOriginalType() == null) {
                        return getFinalType(PrimitiveByteArrayType.INSTANCE, configType);
                    }
                    return getFinalType(BasicType.STRING_TYPE, configType);
                case FLOAT:
                    return getFinalType(BasicType.FLOAT_TYPE, configType);
                case DOUBLE:
                    return getFinalType(BasicType.DOUBLE_TYPE, configType);
                case BOOLEAN:
                    return getFinalType(BasicType.BOOLEAN_TYPE, configType);
                case FIXED_LEN_BYTE_ARRAY:
                    if (type.getLogicalTypeAnnotation() == null) {
                        return getFinalType(LocalTimeType.LOCAL_DATE_TIME_TYPE, configType);
                    }
                    String typeInfo =
                            type.getLogicalTypeAnnotation()
                                    .toString()
                                    .replaceAll(SqlType.DECIMAL.toString(), "")
                                    .replaceAll("\\(", "")
                                    .replaceAll("\\)", "");
                    String[] splits = typeInfo.split(",");
                    int precision = Integer.parseInt(splits[0]);
                    int scale = Integer.parseInt(splits[1]);
                    DecimalType decimalType = new DecimalType(precision, scale);
                    return getFinalType(decimalType, configType);
                default:
                    throw CommonError.convertToSeaTunnelTypeError("Parquet", type.toString(), name);
            }
        } else {
            LogicalTypeAnnotation logicalTypeAnnotation =
                    type.asGroupType().getLogicalTypeAnnotation();
            if (logicalTypeAnnotation == null) {
                // struct type
                List<Type> fields = type.asGroupType().getFields();
                String[] fieldNames = new String[fields.size()];
                SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Type fieldType = fields.get(i);
                    SeaTunnelDataType<?> configDataType = null;
                    if (configType instanceof SeaTunnelRowType) {
                        SeaTunnelRowType configRowType = (SeaTunnelRowType) configType;
                        if (configRowType.getFieldTypes().length > i) {
                            configDataType = configRowType.getFieldType(i);
                        }
                    }
                    SeaTunnelDataType<?> seaTunnelDataType =
                            parquetType2SeaTunnelType(fields.get(i), configDataType, name);
                    fieldNames[i] = fieldType.getName();
                    seaTunnelDataTypes[i] = seaTunnelDataType;
                }
                return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
            } else {
                switch (logicalTypeAnnotation.toOriginalType()) {
                    case MAP:
                        GroupType groupType = type.asGroupType().getType(0).asGroupType();
                        if (configType instanceof MapType) {
                            SeaTunnelDataType<?> keyDataType =
                                    ((MapType<?, ?>) configType).getKeyType();
                            SeaTunnelDataType<?> valueDataType =
                                    ((MapType<?, ?>) configType).getValueType();
                            keyDataType =
                                    parquetType2SeaTunnelType(
                                            groupType.getType(0), keyDataType, name);
                            valueDataType =
                                    parquetType2SeaTunnelType(
                                            groupType.getType(1), valueDataType, name);

                            return new MapType<>(keyDataType, valueDataType);
                        } else {
                            return new MapType<>(
                                    parquetType2SeaTunnelType(groupType.getType(0), null, name),
                                    parquetType2SeaTunnelType(groupType.getType(1), null, name));
                        }
                    case LIST:
                        Type elementType;
                        Type firstLevel = type.asGroupType().getType(0);

                        if (firstLevel.isPrimitive()) {
                            elementType = firstLevel;
                        } else {
                            GroupType firstLevelGroup = firstLevel.asGroupType();
                            LogicalTypeAnnotation firstLevelAnnotation =
                                    firstLevelGroup.getLogicalTypeAnnotation();

                            if (firstLevelAnnotation != null
                                    && (firstLevelAnnotation.toOriginalType() == OriginalType.LIST
                                            || firstLevelAnnotation.toOriginalType()
                                                    == OriginalType.MAP)) {
                                elementType = firstLevel;
                            } else if (firstLevelGroup.getFieldCount() == 1) {
                                elementType = firstLevelGroup.getType(0);
                            } else {
                                elementType = firstLevel;
                            }
                        }

                        SeaTunnelDataType<?> fieldType =
                                parquetType2SeaTunnelType(elementType, null, name);
                        if (configType instanceof ArrayType) {
                            SeaTunnelDataType<?> seaTunnelDataType =
                                    ((ArrayType) configType).getElementType();
                            fieldType =
                                    parquetType2SeaTunnelType(elementType, seaTunnelDataType, name);
                        }
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
                            case BYTES:
                                return ArrayType.of(PrimitiveByteArrayType.INSTANCE);
                            case ARRAY:
                            case MAP:
                                return ArrayType.of(fieldType);
                            default:
                                throw CommonError.convertToSeaTunnelTypeError(
                                        PARQUET, type.toString(), name);
                        }
                    default:
                        throw CommonError.convertToSeaTunnelTypeError(
                                PARQUET, type.toString(), name);
                }
            }
        }
    }

    @Override
    boolean checkFileType(String path) {
        boolean checkResult;
        byte[] magic = new byte[PARQUET_MAGIC.length];
        try {
            FSDataInputStream in = hadoopFileSystemProxy.getInputStream(path);
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

    private SeaTunnelDataType<?> getFinalType(
            SeaTunnelDataType<?> fileType, SeaTunnelDataType<?> configType) {
        if (configType == null) {
            return fileType;
        }
        return canConvert(fileType, configType) ? configType : fileType;
    }

    private SeaTunnelDataType<?> getConfigFieldType(
            SeaTunnelRowType configRowType, String fieldName) {

        if (configRowType == null) {
            return null;
        }

        int fieldIndex = Arrays.asList(configRowType.getFieldNames()).indexOf(fieldName);

        return fieldIndex == -1 ? null : configRowType.getFieldType(fieldIndex);
    }
}
