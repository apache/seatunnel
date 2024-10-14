package org.apache.seatunnel.connectors.seatunnel.sls.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import com.aliyun.openservices.log.common.LogContent;
import com.aliyun.openservices.log.common.LogItem;

import java.util.ArrayList;
import java.util.List;

public class SeatunnelRowSerialization {
    JsonSerializationSchema jsonSerializationSchema;

    public SeatunnelRowSerialization(SeaTunnelRowType rowType) {
        this.jsonSerializationSchema = new JsonSerializationSchema(rowType);
    }

    public List<LogItem> serializeRow(SeaTunnelRow row) {
        List<LogItem> logGroup = new ArrayList<LogItem>();
        LogItem logItem = new LogItem();
        String rowJson = new String(jsonSerializationSchema.serialize(row));
        LogContent content = new LogContent("content", rowJson);
        logItem.PushBack(content);
        logGroup.add(logItem);
        return logGroup;
    }
}
