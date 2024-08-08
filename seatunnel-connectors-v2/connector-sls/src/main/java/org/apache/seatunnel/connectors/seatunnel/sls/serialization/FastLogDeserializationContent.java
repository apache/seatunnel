package org.apache.seatunnel.connectors.seatunnel.sls.serialization;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.*;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.*;

public class FastLogDeserializationContent implements DeserializationSchema<SeaTunnelRow>,FastLogDeserialization<SeaTunnelRow> {


    public static final DateTimeFormatter TIME_FORMAT;
    private final CatalogTable catalogTable;



    static {
        TIME_FORMAT = (new DateTimeFormatterBuilder()).appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    }
    public FastLogDeserializationContent(CatalogTable catalogTable){
        this.catalogTable=catalogTable;
    }
    @Override
    public SeaTunnelRow deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }


    public void deserialize(List<LogGroupData> logGroupDatas, Collector<SeaTunnelRow> out) throws IOException {
        for (LogGroupData logGroupData : logGroupDatas){
            FastLogGroup logs = logGroupData.GetFastLogGroup();
            for (FastLog log : logs.getLogs()){
                SeaTunnelRow seaTunnelRow = convertFastLogContent(log);
                out.collect(seaTunnelRow);
            }
        }
    }

    private SeaTunnelRow convertFastLogContent(FastLog log) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        List<Object> transformedRow = new ArrayList<>(rowType.getTotalFields());
        // json format
        StringBuilder jsonStringBuilder = new StringBuilder();
        jsonStringBuilder.append("{");
        log.getContents().forEach((content)-> jsonStringBuilder.append("\"").append(content.getKey()).append("\":\"").append( content.getValue()).append("\","));
        jsonStringBuilder.deleteCharAt(jsonStringBuilder.length() - 1); // 删除最后一个逗号
        jsonStringBuilder.append("}");
        // content field
        transformedRow.add(jsonStringBuilder.toString());
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(transformedRow.toArray());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        seaTunnelRow.setTableId(catalogTable.getTableId().getTableName());
        return seaTunnelRow;
    }

}
