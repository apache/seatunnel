package org.apache.seatunnel.format.protobuf;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;

import java.io.IOException;
import java.util.Optional;

public class ProtobufDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = -7907358485475741366L;

    private final SeaTunnelRowType rowType;
    private final ProtobufToRowConverter converter;
    private final CatalogTable catalogTable;
    private final String protoContent;
    private final String messageName;

    public ProtobufDeserializationSchema(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();
        this.messageName = catalogTable.getOptions().get("protobuf_message_name");
        this.protoContent = catalogTable.getOptions().get("protobuf_schema");
        this.converter = new ProtobufToRowConverter(protoContent, messageName);
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        Descriptors.Descriptor descriptor = this.converter.getDescriptor();
        DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, message);
        SeaTunnelRow seaTunnelRow = this.converter.converter(descriptor, dynamicMessage, rowType);
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            seaTunnelRow.setTableId(tablePath.toString());
        }
        return seaTunnelRow;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }
}
