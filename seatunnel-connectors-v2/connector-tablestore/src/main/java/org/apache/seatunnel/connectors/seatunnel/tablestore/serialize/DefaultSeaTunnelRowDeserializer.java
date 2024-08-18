package org.apache.seatunnel.connectors.seatunnel.tablestore.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.alicloud.openservices.tablestore.model.StreamRecord;

import java.util.ArrayList;
import java.util.List;

public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    @Override
    public SeaTunnelRow deserialize(StreamRecord r) {
        List<Object> fields = new ArrayList<>();
        r.getColumns()
                .forEach(
                        k -> {
                            fields.add(k.getColumn().getValue());
                        });
        return new SeaTunnelRow(fields.toArray());
    }
}
