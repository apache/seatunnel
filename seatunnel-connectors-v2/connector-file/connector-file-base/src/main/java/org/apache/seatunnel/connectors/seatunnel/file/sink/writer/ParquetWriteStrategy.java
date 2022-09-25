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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;

import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ParquetWriteStrategy extends AbstractWriteStrategy {
    private final Map<String, ParquetWriter<GenericRecord>> beingWrittenWriter;

    public ParquetWriteStrategy(TextFileSinkConfig textFileSinkConfig) {
        super(textFileSinkConfig);
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ParquetWriter<GenericRecord> writer = getOrCreateWriter(filePath);
        Schema schema = buildSchemaWithRowType();
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        sinkColumnsIndexInRow.forEach(index -> {
            String fieldName = seaTunnelRowType.getFieldName(index);
            Object field = seaTunnelRow.getField(index);
            recordBuilder.set(fieldName, resolveObject(field, seaTunnelRowType.getFieldType(index)));
        });
        GenericData.Record record = recordBuilder.build();
        try {
            writer.write(record);
        } catch (IOException e) {
            String errorMsg = String.format("Write data to file [%s] error", filePath);
            throw new RuntimeException(errorMsg, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach((k, v) -> {
            try {
                v.close();
            } catch (IOException e) {
                String errorMsg = String.format("Close file [%s] parquet writer failed, error msg: [%s]", k, e.getMessage());
                throw new RuntimeException(errorMsg, e);
            }
            needMoveFiles.put(k, getTargetLocation(k));
        });
    }

    private ParquetWriter<GenericRecord> getOrCreateWriter(@NonNull String filePath) {
        ParquetWriter<GenericRecord> writer = this.beingWrittenWriter.get(filePath);
        if (writer == null) {
            Schema schema = buildSchemaWithRowType();
            Path path = new Path(filePath);
            try {
                HadoopOutputFile outputFile = HadoopOutputFile.fromPath(path, getConfiguration(hadoopConf));
                ParquetWriter<GenericRecord> newWriter = AvroParquetWriter.<GenericRecord>builder(outputFile)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        // use parquet v1 to improve compatibility
                        .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                        // Temporarily use snappy compress
                        // I think we can use the compress option in config to control this
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withSchema(schema)
                        .build();
                this.beingWrittenWriter.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get parquet writer for file [%s] error", filePath);
                throw new RuntimeException(errorMsg, e);
            }
        }
        return writer;
    }

    private Object resolveObject(Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                return new GenericData.Array<>(Schema.createArray(seaTunnelDataType2ParquetSchema(elementType)), Arrays.asList((Object[]) data));
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
            case BYTES:
                return data;
            case DATE:
                return ((LocalDate) data).toEpochDay();
            case TIMESTAMP:
                return ((LocalDateTime) data).toEpochSecond(ZoneOffset.UTC);
            case DECIMAL:
                byte[] bytes = ((BigDecimal) data).toPlainString().getBytes();
                return new GenericData.Fixed(seaTunnelDataType2ParquetSchema(seaTunnelDataType), bytes);
            case ROW:
            default:
                String errorMsg = String.format("SeaTunnel file connector is not supported for this data type [%s]", seaTunnelDataType.getSqlType());
                throw new UnsupportedOperationException(errorMsg);
        }
    }

    private Schema seaTunnelDataType2ParquetSchema(SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                BasicType<?> elementType = ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
//                Schema.Field elementField = new Schema.Field("array_element", seaTunnelDataType2ParquetSchema(elementType));
//                Schema element = Schema.createRecord("array_element", null, null, false, Collections.singletonList(elementField));
                return Schema.createArray(seaTunnelDataType2ParquetSchema(elementType));
            case MAP:
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) seaTunnelDataType).getValueType();
                return Schema.createMap(seaTunnelDataType2ParquetSchema(valueType));
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case TINYINT:
            case SMALLINT:
            case INT:
            case DATE:
                return Schema.create(Schema.Type.INT);
            case BIGINT:
            case TIMESTAMP:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case DECIMAL:
                return Schema.createFixed("decimal", null,  null, ((DecimalType) seaTunnelDataType).getPrecision());
            case NULL:
                return Schema.create(Schema.Type.NULL);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case ROW:
                ArrayList<Schema.Field> fields = new ArrayList<>();
                SeaTunnelDataType<?>[] fieldTypes = ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                for (int i = 0; i < fieldTypes.length; i++) {
                    Schema schema = seaTunnelDataType2ParquetSchema(fieldTypes[i]);
                    Schema.Field field = new Schema.Field(fieldNames[i], schema, null, null);
                    fields.add(field);
                }
                return Schema.createRecord("SeaTunnelRecord",
                        "The record generated by SeaTunnel file connector",
                        "org.apache.parquet.avro",
                        false,
                        fields);
            default:
                String errorMsg = String.format("SeaTunnel file connector is not supported for this data type [%s]", seaTunnelDataType.getSqlType());
                throw new UnsupportedOperationException(errorMsg);
        }
    }

    private Schema buildSchemaWithRowType() {
        ArrayList<Schema.Field> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        sinkColumnsIndexInRow.forEach(index -> {
            Schema schema = seaTunnelDataType2ParquetSchema(fieldTypes[index]);
            Schema.Field field = new Schema.Field(fieldNames[index], schema);
            fields.add(field);
        });
        return Schema.createRecord("SeaTunnelRecord",
                "The record generated by SeaTunnel file connector",
                "org.apache.parquet.avro",
                false,
                fields);
    }
}
