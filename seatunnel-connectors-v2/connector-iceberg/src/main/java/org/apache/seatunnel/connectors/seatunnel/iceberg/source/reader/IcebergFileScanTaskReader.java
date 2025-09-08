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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.reader;

import org.apache.seatunnel.shade.com.google.common.collect.Sets;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.IcebergRecordProjection;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;

import lombok.Builder;
import lombok.NonNull;

import java.io.Closeable;
import java.util.Map;

import static org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader;

@Builder
public class IcebergFileScanTaskReader implements Closeable {

    private final FileIO fileIO;
    private final Schema tableSchema;
    private final Schema projectedSchema;
    private final boolean caseSensitive;
    private final boolean reuseContainers;

    public CloseableIterator<Record> open(@NonNull FileScanTask task) {
        CloseableIterable<Record> iterable = icebergGenericRead(task);
        return iterable.iterator();
    }

    private CloseableIterable<Record> icebergGenericRead(FileScanTask task) {
        DeleteFilter<Record> deletes =
                new GenericDeleteFilter(fileIO, task, tableSchema, projectedSchema);
        Schema readSchema = deletes.requiredSchema();

        CloseableIterable<Record> records = openFile(task, readSchema);
        records = deletes.filter(records);
        records = applyResidual(records, readSchema, task.residual());

        if (!projectedSchema.sameSchema(readSchema)) {
            // filter metadata columns
            records =
                    CloseableIterable.transform(
                            records,
                            record ->
                                    new IcebergRecordProjection(
                                            record,
                                            readSchema.asStruct(),
                                            projectedSchema.asStruct()));
        }
        return records;
    }

    private CloseableIterable<Record> applyResidual(
            CloseableIterable<Record> records, Schema recordSchema, Expression residual) {
        if (residual != null && residual != Expressions.alwaysTrue()) {
            InternalRecordWrapper wrapper = new InternalRecordWrapper(recordSchema.asStruct());
            Evaluator filter = new Evaluator(recordSchema.asStruct(), residual, caseSensitive);
            return CloseableIterable.filter(records, record -> filter.eval(wrapper.wrap(record)));
        }

        return records;
    }

    private CloseableIterable<Record> openFile(FileScanTask task, Schema fileProjection) {
        if (task.isDataTask()) {
            throw new IcebergConnectorException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION, "Cannot read data task.");
        }
        InputFile input = fileIO.newInputFile(task.file().path().toString());
        Map<Integer, ?> partition =
                PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);

        switch (task.file().format()) {
            case AVRO:
                Avro.ReadBuilder avro =
                        Avro.read(input)
                                .project(fileProjection)
                                .createReaderFunc(
                                        avroSchema ->
                                                DataReader.create(
                                                        fileProjection, avroSchema, partition))
                                .split(task.start(), task.length());
                if (reuseContainers) {
                    avro.reuseContainers();
                }
                return avro.build();
            case PARQUET:
                Parquet.ReadBuilder parquet =
                        Parquet.read(input)
                                .caseSensitive(caseSensitive)
                                .project(fileProjection)
                                .createReaderFunc(
                                        fileSchema ->
                                                buildReader(fileProjection, fileSchema, partition))
                                .split(task.start(), task.length())
                                .filter(task.residual());
                if (reuseContainers) {
                    parquet.reuseContainers();
                }
                return parquet.build();
            case ORC:
                Schema projectionWithoutConstantAndMetadataFields =
                        TypeUtil.selectNot(
                                fileProjection,
                                Sets.union(partition.keySet(), MetadataColumns.metadataFieldIds()));
                ORC.ReadBuilder orc =
                        ORC.read(input)
                                .caseSensitive(caseSensitive)
                                .project(projectionWithoutConstantAndMetadataFields)
                                .createReaderFunc(
                                        fileSchema ->
                                                GenericOrcReader.buildReader(
                                                        fileProjection, fileSchema, partition))
                                .split(task.start(), task.length())
                                .filter(task.residual());
                return orc.build();
            default:
                throw new IcebergConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        String.format(
                                "Cannot read %s file: %s",
                                task.file().format().name(), task.file().path()));
        }
    }

    @Override
    public void close() {
        fileIO.close();
    }
}
