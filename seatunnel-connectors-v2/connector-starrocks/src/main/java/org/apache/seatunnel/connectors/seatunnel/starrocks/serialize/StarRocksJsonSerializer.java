package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksJsonSerializer extends StarRocksBaseSerializer implements StarRocksISerializer {

    private static final long serialVersionUID = 1L;
    
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper mapper = new ObjectMapper();

    public StarRocksJsonSerializer(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        Map<String, Object> rowMap = new HashMap<>(row.getFields().length);

        for (int i = 0; i < row.getFields().length; i++) {
            String value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            rowMap.put(seaTunnelRowType.getFieldName(i), value);
        }
        try {
            return mapper.writeValueAsString(rowMap);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("serialize err", e);
        }
    }
}
