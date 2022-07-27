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

package org.apache.seatunnel.connectors.seatunnel.file.sink.local;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystem;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractTransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.PartitionDirNameGenerator;

import lombok.NonNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalParquetTransactionStateFileWriter extends AbstractTransactionStateFileWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalParquetTransactionStateFileWriter.class);
    private final Configuration configuration = new Configuration();
    private Map<String, ParquetWriter<GenericRecord>> beingWrittenWriter;

    public LocalParquetTransactionStateFileWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                                                 @NonNull TransactionFileNameGenerator transactionFileNameGenerator,
                                                 @NonNull PartitionDirNameGenerator partitionDirNameGenerator,
                                                 @NonNull List<Integer> sinkColumnsIndexInRow, @NonNull String tmpPath,
                                                 @NonNull String targetPath,
                                                 @NonNull String jobId,
                                                 int subTaskIndex,
                                                 @NonNull FileSystem fileSystem) {
        super(seaTunnelRowTypeInfo, transactionFileNameGenerator, partitionDirNameGenerator, sinkColumnsIndexInRow, tmpPath, targetPath, jobId, subTaskIndex, fileSystem);
        beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        ParquetWriter<GenericRecord> writer = getOrCreateWriter(filePath);
        Schema schema = buildSchemaWithRowType();
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        sinkColumnsIndexInRow.forEach(index -> {
            if (seaTunnelRowTypeInfo.getFieldType(index).equals(BasicType.STRING_TYPE)) {
                recordBuilder.set(seaTunnelRowTypeInfo.getFieldName(index), seaTunnelRow.getField(index).toString());
            } else {
                recordBuilder.set(seaTunnelRowTypeInfo.getFieldName(index), seaTunnelRow.getField(index));
            }
        });
        GenericData.Record record = recordBuilder.build();
        try {
            writer.write(record);
        } catch (IOException e) {
            String errorMsg = String.format("Write data to parquet file [%s] error", filePath);
            throw new RuntimeException(errorMsg, e);
        }
    }

    @Override
    public void finishAndCloseWriteFile() {
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

    @Override
    public void beginTransaction(String transactionId) {
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void abortTransaction(String transactionId) {
        this.beingWrittenWriter = new HashMap<>();
    }

    private ParquetWriter<GenericRecord> getOrCreateWriter(@NonNull String filePath) {
        ParquetWriter<GenericRecord> writer = this.beingWrittenWriter.get(filePath);
        if (writer == null) {
            Schema schema = buildSchemaWithRowType();
            Path path = new Path(filePath);
            try {
                // In order to write file to local file system we should use empty configuration object
                HadoopOutputFile outputFile = HadoopOutputFile.fromPath(path, configuration);
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

    private Schema buildSchemaWithRowType() {
        ArrayList<Schema.Field> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowTypeInfo.getFieldTypes();
        String[] fieldNames = seaTunnelRowTypeInfo.getFieldNames();
        sinkColumnsIndexInRow.forEach(index -> {
            if (BasicType.BOOLEAN_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.BOOLEAN), null, null);
                fields.add(field);
            } else if (BasicType.SHORT_TYPE.equals(fieldTypes[index]) || BasicType.INT_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.INT), null, null);
                fields.add(field);
            } else if (BasicType.LONG_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.LONG), null, null);
                fields.add(field);
            } else if (BasicType.FLOAT_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.FLOAT), null, null);
                fields.add(field);
            } else if (BasicType.DOUBLE_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.DOUBLE), null, null);
                fields.add(field);
            } else if (BasicType.STRING_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.STRING), null, null);
                fields.add(field);
            } else if (BasicType.BYTE_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.BYTES), null, null);
                fields.add(field);
            } else if (BasicType.VOID_TYPE.equals(fieldTypes[index])) {
                Schema.Field field = new Schema.Field(fieldNames[index], Schema.create(Schema.Type.NULL), null, null);
                fields.add(field);
            }
        });
        return Schema.createRecord("SeatunnelRecord",
                "The record generated by seatunnel file connector",
                "org.apache.parquet.avro",
                false,
                fields);
    }
}
