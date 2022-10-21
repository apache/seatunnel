package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

public class StarRocksCsvSerializer extends StarRocksBaseSerializer implements StarRocksISerializer {
    
    private static final long serialVersionUID = 1L;

    private final String columnSeparator;
    private final SeaTunnelRowType seaTunnelRowType;

    public StarRocksCsvSerializer(String sp, SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.columnSeparator = StarRocksDelimiterParser.parse(sp, "\t");
    }

    @Override
    public String serialize(SeaTunnelRow row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getFields().length; i++) {
            String value = convert(seaTunnelRowType.getFieldType(i), row.getField(i));
            sb.append(null == value ? "\\N" : value);
            if (i < row.getFields().length - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }

}
