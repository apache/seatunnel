package io.github.interestinglab.waterdrop.flink.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;

public class JsonRowInputFormat extends DelimitedInputFormat<Row> implements  ResultTypeQueryable<Row> {


    private RowTypeInfo rowTypeInfo;

    private static final byte CARRIAGE_RETURN = (byte) '\r';

    private static final byte NEW_LINE = (byte) '\n';

    private String charsetName = "UTF-8";

    public JsonRowInputFormat(Path filePath, Configuration configuration, RowTypeInfo rowTypeInfo) {
        super(filePath, configuration);
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public Row readRecord(Row reuse, byte[] bytes, int offset, int numBytes) throws IOException {
        if (this.getDelimiter() != null && this.getDelimiter().length == 1
                && this.getDelimiter()[0] == NEW_LINE && offset + numBytes >= 1
                && bytes[offset + numBytes - 1] == CARRIAGE_RETURN) {
            numBytes -= 1;
        }

        String str = new String(bytes, offset, numBytes, this.charsetName);
        JSONObject json = JSONObject.parseObject(str);
        Row reuseRow;
        if (reuse == null){
            reuseRow = new Row(rowTypeInfo.getArity());
        }else {
            reuseRow = reuse;
        }
        setJsonRow(reuseRow, json, rowTypeInfo);
        return reuseRow;
    }

    private void setJsonRow(Row row, JSONObject json, RowTypeInfo rowTypeInfo) {
        String[] fieldNames = rowTypeInfo.getFieldNames();
        int i = 0;
        for (String name : fieldNames) {
            Object value = json.get(name);
            if (value instanceof JSONObject) {
                TypeInformation information = rowTypeInfo.getTypeAt(name);
                Row r = new Row(information.getArity());
                setJsonRow(r, (JSONObject) value, (RowTypeInfo) information);
                row.setField(i++, r);
            } else if (value instanceof JSONArray) {
                ObjectArrayTypeInfo information = (ObjectArrayTypeInfo) rowTypeInfo.getTypeAt(name);
                JSONArray array = (JSONArray) value;
                Object[] objects = new Object[array.size()];
                int j = 0;
                for (Object o : array) {
                    if (o instanceof JSONObject) {
                        TypeInformation componentInfo = information.getComponentInfo();
                        Row r = new Row(componentInfo.getArity());
                        setJsonRow(r, (JSONObject) o, (RowTypeInfo) componentInfo);
                        objects[j++] = r;
                    } else {
                        objects[j++] = o;
                    }
                }
                row.setField(i++, objects);
                i++;
            } else {
                row.setField(i++, value);
            }
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

}
