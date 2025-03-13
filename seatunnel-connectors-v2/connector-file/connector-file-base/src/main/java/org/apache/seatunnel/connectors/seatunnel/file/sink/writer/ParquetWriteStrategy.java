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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import lombok.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.JulianFields;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ParquetWriteStrategy extends AbstractWriteStrategy<ParquetWriter<GenericRecord>> {
    private final LinkedHashMap<String, ParquetWriter<GenericRecord>> beingWrittenWriter;
    private AvroSchemaConverter schemaConverter;
    private Schema schema;
    private Set<String> writePathsAsInt96;
    public static final int[] PRECISION_TO_BYTE_COUNT = new int[38];

    static {
        for (int prec = 1; prec <= 38; prec++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[prec - 1] =
                    (int) Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public ParquetWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void init(HadoopConf conf, String jobId, String uuidPrefix, int subTaskIndex) {
        super.init(conf, jobId, uuidPrefix, subTaskIndex);
        Configuration configuration = getConfiguration(hadoopConf);
        writePathsAsInt96 = new HashSet<>(fileSinkConfig.getParquetAvroWriteFixedAsInt96());
        if (fileSinkConfig.getParquetWriteTimestampAsInt96()) {
            List<String> timestampFields = new ArrayList<>();
            for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
                if (SqlType.TIMESTAMP.equals(seaTunnelRowType.getFieldType(i).getSqlType())) {
                    timestampFields.add(seaTunnelRowType.getFieldName(i));
                }
            }
            writePathsAsInt96.addAll(timestampFields);
        }
        if (!writePathsAsInt96.isEmpty()) {
            configuration.set(
                    AvroWriteSupport.WRITE_FIXED_AS_INT96, String.join(",", writePathsAsInt96));
        }
        schemaConverter = new AvroSchemaConverter(configuration);
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ParquetWriter<GenericRecord> writer = getOrCreateOutputStream(filePath);
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        for (Integer integer : sinkColumnsIndexInRow) {
            String fieldName = seaTunnelRowType.getFieldName(integer);
            Object field = seaTunnelRow.getField(integer);
            recordBuilder.set(
                    fieldName.toLowerCase(),
                    resolveObject(fieldName, field, seaTunnelRowType.getFieldType(integer)));
        }
        GenericData.Record record = recordBuilder.build();
        try {
            writer.write(record);
        } catch (IOException e) {
            throw CommonError.fileOperationFailed("ParquetFile", "write", filePath, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach(
                (k, v) -> {
                    try {
                        v.close();
                    } catch (IOException e) {
                        String errorMsg =
                                String.format(
                                        "Close file [%s] parquet writer failed, error msg: [%s]",
                                        k, e.getMessage());
                        throw new FileConnectorException(
                                CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, errorMsg, e);
                    }
                    needMoveFiles.put(k, getTargetLocation(k));
                });
        this.beingWrittenWriter.clear();
    }

    @Override
    public ParquetWriter<GenericRecord> getOrCreateOutputStream(@NonNull String filePath) {
        if (schema == null) {
            schema = buildAvroSchemaWithRowType(seaTunnelRowType, sinkColumnsIndexInRow);
        }
        ParquetWriter<GenericRecord> writer = this.beingWrittenWriter.get(filePath);
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        if (writer == null) {
            Path path = new Path(filePath);
            // initialize the kerberos login
            return hadoopFileSystemProxy.doWithHadoopAuth(
                    (configuration, userGroupInformation) -> {
                        try {
                            if (!writePathsAsInt96.isEmpty()) {
                                configuration.set(
                                        AvroWriteSupport.WRITE_FIXED_AS_INT96,
                                        String.join(",", writePathsAsInt96));
                            }
                            HadoopOutputFile outputFile =
                                    HadoopOutputFile.fromPath(path, getConfiguration(hadoopConf));
                            ParquetWriter<GenericRecord> newWriter =
                                    AvroParquetWriter.<GenericRecord>builder(outputFile)
                                            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                                            .withDataModel(dataModel)
                                            .withConf(configuration)
                                            // use parquet v1 to improve compatibility
                                            .withWriterVersion(
                                                    ParquetProperties.WriterVersion.PARQUET_1_0)
                                            .withCompressionCodec(
                                                    compressFormat.getParquetCompression())
                                            .withSchema(schema)
                                            .build();
                            this.beingWrittenWriter.put(filePath, newWriter);
                            return newWriter;
                        } catch (IOException e) {
                            String errorMsg =
                                    String.format(
                                            "Get parquet writer for file [%s] error", filePath);
                            throw new FileConnectorException(
                                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, errorMsg, e);
                        }
                    });
        }
        return writer;
    }

    private Object resolveObject(String name, Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        if (data == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                ArrayList<Object> records = new ArrayList<>(((Object[]) data).length);
                for (Object object : (Object[]) data) {
                    Object resolvedObject = resolveObject(name, object, elementType);
                    records.add(resolvedObject);
                }
                return records;
            case MAP:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DECIMAL:
            case DATE:
                return data;
            case TIMESTAMP:
                if (writePathsAsInt96.contains(name)) {
                    LocalDateTime localDateTime = (LocalDateTime) data;
                    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    calendar.setTime(
                            Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant()));
                    int julianDays =
                            (int)
                                    JulianFields.JULIAN_DAY.getFrom(
                                            LocalDate.of(
                                                    calendar.get(Calendar.YEAR),
                                                    calendar.get(Calendar.MONTH) + 1,
                                                    calendar.get(Calendar.DAY_OF_MONTH)));
                    long timeOfDayNanos =
                            TimeUnit.HOURS.toNanos(calendar.get(Calendar.HOUR_OF_DAY))
                                    + TimeUnit.MINUTES.toNanos(calendar.get(Calendar.MINUTE))
                                    + TimeUnit.SECONDS.toNanos(calendar.get(Calendar.SECOND))
                                    + TimeUnit.MILLISECONDS.toNanos(
                                            calendar.get(Calendar.MILLISECOND));
                    NanoTime nanoTime = new NanoTime(julianDays, timeOfDayNanos);
                    return new GenericData.Fixed(
                            schema.getField(name).schema(), nanoTime.toBinary().getBytes());
                }
                return ((LocalDateTime) data)
                        .atZone(ZoneId.systemDefault())
                        .toInstant()
                        .toEpochMilli();
            case BYTES:
                if (writePathsAsInt96.contains(name)) {
                    return new GenericData.Fixed(schema.getField(name).schema(), (byte[]) data);
                }
                return ByteBuffer.wrap((byte[]) data);
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                List<Integer> sinkColumnsIndex =
                        IntStream.rangeClosed(0, fieldNames.length - 1)
                                .boxed()
                                .collect(Collectors.toList());
                Schema recordSchema =
                        buildAvroSchemaWithRowType(
                                (SeaTunnelRowType) seaTunnelDataType, sinkColumnsIndex);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(fieldNames[i], seaTunnelRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel file connector is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    public Type seaTunnelDataType2ParquetDataType(
            String fieldName, SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                return Types.optionalGroup()
                        .as(OriginalType.LIST)
                        .addField(
                                Types.repeatedGroup()
                                        .addField(
                                                seaTunnelDataType2ParquetDataType(
                                                        "array_element", elementType))
                                        .named("bag"))
                        .named(fieldName);
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) seaTunnelDataType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) seaTunnelDataType).getValueType();
                return ConversionPatterns.mapType(
                        Type.Repetition.OPTIONAL,
                        fieldName,
                        seaTunnelDataType2ParquetDataType("key", keyType),
                        seaTunnelDataType2ParquetDataType("value", valueType));
            case STRING:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
            case BOOLEAN:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TINYINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .as(OriginalType.INT_8)
                        .named(fieldName);
            case SMALLINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .as(OriginalType.INT_16)
                        .named(fieldName);
            case INT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DATE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.dateType())
                        .as(OriginalType.DATE)
                        .named(fieldName);
            case BIGINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TIMESTAMP:
                if (writePathsAsInt96.contains(fieldName)) {
                    return Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL)
                            .named(fieldName);
                }
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .as(OriginalType.TIMESTAMP_MILLIS)
                        .named(fieldName);
            case FLOAT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DOUBLE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DECIMAL:
                int precision = ((DecimalType) seaTunnelDataType).getPrecision();
                int scale = ((DecimalType) seaTunnelDataType).getScale();
                return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(PRECISION_TO_BYTE_COUNT[precision - 1])
                        .as(OriginalType.DECIMAL)
                        .precision(precision)
                        .scale(scale)
                        .named(fieldName);
            case BYTES:
                if (writePathsAsInt96.contains(fieldName)) {
                    return Types.primitive(
                                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                                    Type.Repetition.OPTIONAL)
                            .length(12)
                            .named(fieldName);
                }
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                Type[] types = new Type[fieldTypes.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    Type type = seaTunnelDataType2ParquetDataType(fieldNames[i], fieldTypes[i]);
                    types[i] = type;
                }
                return Types.optionalGroup().addFields(types).named(fieldName);
            case NULL:
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel file connector is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private Schema buildAvroSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType, List<Integer> sinkColumnsIndex) {
        ArrayList<Type> types = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        sinkColumnsIndex.forEach(
                index -> {
                    Type type =
                            seaTunnelDataType2ParquetDataType(
                                    fieldNames[index].toLowerCase(), fieldTypes[index]);
                    types.add(type);
                });
        MessageType seaTunnelRow =
                Types.buildMessage().addFields(types.toArray(new Type[0])).named("SeaTunnelRecord");
        return schemaConverter.convert(seaTunnelRow);
    }
}
