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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDataConverter;

import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.ArrayUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Slf4j
public class IcebergSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private SinkWriter.Context context;

    private Schema tableSchema;

    private SeaTunnelRowType seaTunnelRowType;

    private IcebergTableLoader icebergTableLoader;

    private SinkConfig sinkConfig;

    private Table table;

    private List<Record> pendingRows = new ArrayList<>();

    private DataConverter defaultDataConverter;

    private static final int FORMAT_V2 = 2;

    private final FileFormat format;

    private PartitionKey partition = null;

    private OutputFileFactory fileFactory = null;

    FileAppenderFactory<Record> appenderFactory = null;

    public IcebergSinkWriter(
            SinkWriter.Context context,
            Schema tableSchema,
            SeaTunnelRowType seaTunnelRowType,
            SinkConfig sinkConfig) {

        this.context = context;
        this.sinkConfig = sinkConfig;
        this.tableSchema = tableSchema;
        this.seaTunnelRowType = seaTunnelRowType;
        defaultDataConverter = new DefaultDataConverter(seaTunnelRowType, tableSchema);

        if (Objects.isNull(icebergTableLoader)) {
            icebergTableLoader = IcebergTableLoader.create(sinkConfig);
            icebergTableLoader.open();
        }

        if (Objects.isNull(table)) {
            table = icebergTableLoader.loadTable();
        }

        this.format = FileFormat.valueOf(sinkConfig.getFileFormat().toUpperCase(Locale.ENGLISH));
        this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();
        this.partition = createPartitionKey();
        this.appenderFactory = createAppenderFactory(null, null, null);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        pendingRows.add(defaultDataConverter.toIcebergStruct(element));

        if (pendingRows.size() >= sinkConfig.getBatchSize()) {
            writeData();
        }
    }

    @Override
    public void close() throws IOException {
        writeData();

        if (Objects.nonNull(icebergTableLoader)) {
            icebergTableLoader.close();
            pendingRows.clear();
        }
    }

    private PartitionKey createPartitionKey() {
        if (table.spec().isUnpartitioned()) {
            return null;
        }

        Record record = GenericRecord.create(table.schema());
        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.partition(record);

        return partitionKey;
    }

    protected FileAppenderFactory<Record> createAppenderFactory(
            List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema) {
        return new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                ArrayUtil.toIntArray(equalityFieldIds),
                eqDeleteSchema,
                posDeleteRowSchema);
    }

    private DataFile prepareDataFile(
            List<Record> rowSet, FileAppenderFactory<Record> appenderFactory) throws IOException {
        DataWriter<Record> writer =
                appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);

        try (DataWriter<Record> closeableWriter = writer) {
            for (Record row : rowSet) {
                closeableWriter.write(row);
            }
        }
        return writer.toDataFile();
    }

    private EncryptedOutputFile createEncryptedOutputFile() {
        if (Objects.isNull(partition)) {
            return fileFactory.newOutputFile();
        } else {
            return fileFactory.newOutputFile(partition);
        }
    }

    private void writeData() throws IOException {
        if (pendingRows.size() > 0) {
            DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
            table.newRowDelta().addRows(dataFile).commit();

            pendingRows.clear();
        }
    }
}
