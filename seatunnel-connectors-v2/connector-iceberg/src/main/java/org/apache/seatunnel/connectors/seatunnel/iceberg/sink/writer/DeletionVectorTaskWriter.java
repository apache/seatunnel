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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.connectors.seatunnel.iceberg.data.IcebergInternalRecordWrapper;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.io.RollingEqualityDeleteWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Equality delta writes for v3, using deletion vectors only for rows in this writer's new files.
 * Committed rows are still deleted by equality key. No existing data file or deletion vector is
 * rewritten, so loading or merging previous deletion vectors is unnecessary.
 */
class DeletionVectorTaskWriter implements TaskWriter<Record> {

    private final PartitionSpec spec;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final long targetFileSize;
    private final boolean upsert;
    private final PartitionKey partitionKey;
    private final IcebergInternalRecordWrapper rowWrapper;
    private final IcebergInternalRecordWrapper keyWrapper;
    private final RecordProjection keyProjection;
    private final Schema keySchema;
    private final FileWriterFactory<Record> writerFactory;
    private final DVFileWriter dvWriter;
    private final Map<PartitionKey, PartitionWriter> writers = new HashMap<>();
    // Only files created by this attempt may be removed by abort, including incomplete files.
    private final Set<String> createdFiles = new HashSet<>();
    // Retain the shared Puffin stream so failed DV close cannot leave it open during abort.
    private PositionOutputStream dvOutput;
    private boolean closed;
    // Sticky write/close failure: complete must never publish a partial delta after an error.
    private IOException failure;

    DeletionVectorTaskWriter(
            PartitionSpec spec,
            FileFormat format,
            FileAppenderFactory<Record> appenderFactory,
            OutputFileFactory fileFactory,
            OutputFileFactory dvFileFactory,
            FileIO io,
            long targetFileSize,
            Schema schema,
            Set<Integer> identifierFieldIds,
            boolean upsert) {
        this.spec = spec;
        this.fileFactory = fileFactory;
        this.io = io;
        this.targetFileSize = targetFileSize;
        this.upsert = upsert;
        this.partitionKey = new PartitionKey(spec, schema);
        this.rowWrapper = new IcebergInternalRecordWrapper(schema.asStruct());
        this.keySchema = TypeUtil.select(schema, identifierFieldIds);
        this.keyWrapper = new IcebergInternalRecordWrapper(keySchema.asStruct());
        this.keyProjection = RecordProjection.create(schema, keySchema);
        this.writerFactory =
                new FileWriterFactory<Record>() {
                    @Override
                    public DataWriter<Record> newDataWriter(
                            EncryptedOutputFile file, PartitionSpec ignored, StructLike partition) {
                        createdFiles.add(file.encryptingOutputFile().location());
                        return appenderFactory.newDataWriter(file, format, partition);
                    }

                    @Override
                    public EqualityDeleteWriter<Record> newEqualityDeleteWriter(
                            EncryptedOutputFile file, PartitionSpec ignored, StructLike partition) {
                        createdFiles.add(file.encryptingOutputFile().location());
                        return appenderFactory.newEqDeleteWriter(file, format, partition);
                    }

                    @Override
                    public PositionDeleteWriter<Record> newPositionDeleteWriter(
                            EncryptedOutputFile file, PartitionSpec ignored, StructLike partition) {
                        throw new UnsupportedOperationException("v3 requires deletion vectors");
                    }
                };
        this.dvWriter =
                new BaseDVFileWriter(
                        () -> {
                            OutputFile file = dvFileFactory.newOutputFile().encryptingOutputFile();
                            createdFiles.add(file.location());
                            return new OutputFile() {
                                @Override
                                public PositionOutputStream create() {
                                    dvOutput = file.create();
                                    return dvOutput;
                                }

                                @Override
                                public PositionOutputStream createOrOverwrite() {
                                    dvOutput = file.createOrOverwrite();
                                    return dvOutput;
                                }

                                @Override
                                public String location() {
                                    return file.location();
                                }

                                @Override
                                public InputFile toInputFile() {
                                    return file.toInputFile();
                                }
                            };
                        },
                        path -> null);
    }

    @Override
    public void write(Record record) throws IOException {
        if (closed || failure != null) {
            throw new IOException(
                    "Cannot write to a closed or failed Iceberg delta writer", failure);
        }
        try {
            if (!(record instanceof IcebergRecord)) {
                throw new IllegalArgumentException("Expected an IcebergRecord with a row kind");
            }
            IcebergRecord row = (IcebergRecord) record;
            partitionKey.partition(rowWrapper.wrap(row));
            PartitionWriter writer = writers.get(partitionKey);
            if (writer == null) {
                PartitionKey partition = partitionKey.copy();
                writer = new PartitionWriter(partition);
                writers.put(partition, writer);
            }
            Record key = keyProjection.wrap(row);
            switch (row.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    if (upsert) {
                        writer.delete(key);
                    }
                    writer.insert(row, key);
                    break;
                case UPDATE_BEFORE:
                    if (!upsert) {
                        writer.delete(key);
                    }
                    break;
                case DELETE:
                    writer.delete(key);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown row kind: " + row.getRowKind());
            }
        } catch (RuntimeException e) {
            failure = new IOException("Failed to write Iceberg v3 delta", e);
            throw e;
        }
    }

    /** Close all file writers before exposing a checkpoint's files to the committer. */
    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            for (PartitionWriter writer : writers.values()) {
                closeWriter(writer.dataWriter);
                closeWriter(writer.deleteWriter);
                writer.rows.clear();
            }
            closeWriter(dvWriter);
            if (failure != null) {
                // Puffin's close can fail before closing its underlying output stream.
                closeWriter(dvOutput);
            }
            dvOutput = null;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void closeWriter(Closeable writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException | RuntimeException e) {
                if (failure == null) {
                    failure = new IOException("Failed to close Iceberg v3 delta writer", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
    }

    @Override
    public WriteResult complete() throws IOException {
        close();
        WriteResult.Builder result = WriteResult.builder();
        for (PartitionWriter writer : writers.values()) {
            if (writer.dataWriter != null) {
                result.addDataFiles(writer.dataWriter.result().dataFiles());
            }
            if (writer.deleteWriter != null) {
                result.addDeleteFiles(writer.deleteWriter.result().deleteFiles());
            }
        }
        return result.addDeleteFiles(dvWriter.result().deleteFiles())
                .addReferencedDataFiles(dvWriter.result().referencedDataFiles())
                .build();
    }

    /** Delete only this attempt's new files, including a shared Puffin file exactly once. */
    @Override
    public void abort() throws IOException {
        try {
            close();
        } catch (IOException ignored) {
            // A failed close must not prevent cleanup of files created before the failure.
        }
        IOException cleanupFailure = null;
        for (String path : createdFiles) {
            try {
                io.deleteFile(path);
            } catch (RuntimeException e) {
                if (cleanupFailure == null) {
                    cleanupFailure = new IOException("Failed to clean up Iceberg delta files", e);
                } else {
                    cleanupFailure.addSuppressed(e);
                }
            }
        }
        if (cleanupFailure != null) {
            throw cleanupFailure;
        }
    }

    private class PartitionWriter {
        private final PartitionKey partition;
        // Latest live positions for keys in this writer's new files, never committed table rows.
        private final StructLikeMap<PathOffset> rows = StructLikeMap.create(keySchema.asStruct());
        private RollingDataWriter<Record> dataWriter;
        private RollingEqualityDeleteWriter<Record> deleteWriter;

        private PartitionWriter(PartitionKey partition) {
            this.partition = partition;
        }

        /**
         * Records the new row's position before the rolling write can advance to another file.
         * Replacing a key invalidates only its previous position in this attempt's new files.
         */
        private void insert(IcebergRecord row, Record key) {
            if (dataWriter == null) {
                dataWriter =
                        new RollingDataWriter<>(
                                writerFactory, fileFactory, io, targetFileSize, spec, partition);
            }
            StructLike copiedKey = StructLikeUtil.copy(keyWrapper.wrap(key));
            PathOffset current =
                    new PathOffset(
                            dataWriter.currentFilePath().toString(), dataWriter.currentFileRows());
            PathOffset previous = rows.put(copiedKey, current);
            if (previous != null) {
                dvWriter.delete(previous.path, previous.offset, spec, partition);
            }
            dataWriter.write(row);
        }

        /**
         * Uses a deletion vector when the key has a known position in this attempt; otherwise an
         * equality delete removes matching committed rows without rewriting existing data or DVs.
         */
        private void delete(Record key) {
            PathOffset previous = rows.remove(keyWrapper.wrap(key));
            if (previous != null) {
                dvWriter.delete(previous.path, previous.offset, spec, partition);
            } else {
                if (deleteWriter == null) {
                    deleteWriter =
                            new RollingEqualityDeleteWriter<>(
                                    writerFactory,
                                    fileFactory,
                                    io,
                                    targetFileSize,
                                    spec,
                                    partition);
                }
                deleteWriter.write(key);
            }
        }
    }

    private static class PathOffset {
        private final String path;
        private final long offset;

        private PathOffset(String path, long offset) {
            this.path = path;
            this.offset = offset;
        }
    }
}
