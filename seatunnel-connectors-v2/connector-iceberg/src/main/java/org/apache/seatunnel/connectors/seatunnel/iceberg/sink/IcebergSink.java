package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import com.google.auto.service.AutoService;
import lombok.SneakyThrows;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.data.IcebergTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.IcebergSinkWriter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@AutoService(SeaTunnelSink.class)
public class IcebergSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SinkConfig sinkConfig;

    private Schema tableSchema;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Iceberg";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.sinkConfig = SinkConfig.loadConfig(pluginConfig);
        this.tableSchema = loadIcebergSchema(sinkConfig);
        this.seaTunnelRowType = loadSeaTunnelRowType(tableSchema, pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new IcebergSinkWriter(context, tableSchema, seaTunnelRowType, sinkConfig);
    }

    @SneakyThrows
    private Schema loadIcebergSchema(SinkConfig sinkConfig) {
        try (IcebergTableLoader icebergTableLoader = IcebergTableLoader.create(sinkConfig)) {
            icebergTableLoader.open();
            return icebergTableLoader.loadTable().schema();
        }
    }

    private SeaTunnelRowType loadSeaTunnelRowType(Schema tableSchema, Config pluginConfig) {
        List<String> columnNames = new ArrayList<>(tableSchema.columns().size());
        List<SeaTunnelDataType<?>> columnDataTypes = new ArrayList<>(tableSchema.columns().size());

        for (Types.NestedField column : tableSchema.columns()) {
            columnNames.add(column.name());
            columnDataTypes.add(IcebergTypeMapper.mapping(column.type()));
        }

        SeaTunnelRowType originalRowType = new SeaTunnelRowType(
                columnNames.toArray(new String[0]),
                columnDataTypes.toArray(new SeaTunnelDataType[0])
        );

        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, CommonConfig.KEY_FIELDS.key());

        if (checkResult.isSuccess()) {
            SeaTunnelRowType projectedRowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();

            for (int i = 0; i < projectedRowType.getFieldNames().length; i++) {
                String fieldName = projectedRowType.getFieldName(i);
                SeaTunnelDataType<?> projectedFieldType = projectedRowType.getFieldType(i);
                int originalFieldIndex = originalRowType.indexOf(fieldName);
                SeaTunnelDataType<?> originalFieldType =
                        originalRowType.getFieldType(originalFieldIndex);
                checkArgument(
                        projectedFieldType.equals(originalFieldType),
                        String.format(
                                "Illegal field: %s, original: %s <-> projected: %s",
                                fieldName, originalFieldType, projectedFieldType));
            }

            return projectedRowType;
        }
        return originalRowType;
    }
}
