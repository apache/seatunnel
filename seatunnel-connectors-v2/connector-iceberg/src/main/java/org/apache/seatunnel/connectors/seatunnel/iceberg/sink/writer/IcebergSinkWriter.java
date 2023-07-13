package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DataConverter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.DefaultDataConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 *
 *
 * @author mustard
 * @version 1.0
 * Create by 2023-07-05
 */
@Slf4j
public class IcebergSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private Context context;

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


    public IcebergSinkWriter(
            Context context,
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

        log.info("mustard: sink max row count: {}", sinkConfig.getMaxRow());
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        pendingRows.add(defaultDataConverter.toIcebergStruct(element));

        if (pendingRows.size() >= sinkConfig.getMaxRow()) {
            FileAppenderFactory<Record> appenderFactory = createAppenderFactory(null, null, null);
            DataFile dataFile = prepareDataFile(pendingRows, appenderFactory);
            table.newRowDelta().addRows(dataFile).commit();
            pendingRows.clear();
        }
    }

    @Override
    public void close() throws IOException {
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

    protected FileAppenderFactory<Record> createAppenderFactory(List<Integer> equalityFieldIds, Schema eqDeleteSchema, Schema posDeleteRowSchema) {
        return new GenericAppenderFactory(
                table.schema(),
                table.spec(),
                ArrayUtil.toIntArray(equalityFieldIds),
                eqDeleteSchema,
                posDeleteRowSchema
        );
    }

    private DataFile prepareDataFile(List<Record> rowSet, FileAppenderFactory<Record> appenderFactory) throws IOException {
        DataWriter<Record> writer = appenderFactory.newDataWriter(createEncryptedOutputFile(), format, partition);

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
}
