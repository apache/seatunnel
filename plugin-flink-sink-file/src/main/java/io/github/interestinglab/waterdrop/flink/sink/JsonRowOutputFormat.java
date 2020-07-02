package io.github.interestinglab.waterdrop.flink.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

public class JsonRowOutputFormat extends FileOutputFormat<Row> {

    private static final long serialVersionUID = 1L;

    private static final int NEWLINE = '\n';

    private String charsetName;

    private transient Charset charset;

    private RowTypeInfo rowTypeInfo;


    public JsonRowOutputFormat(Path outputPath,RowTypeInfo rowTypeInfo) {
        this(outputPath, rowTypeInfo,"UTF-8");
    }

    public JsonRowOutputFormat(Path outputPath,RowTypeInfo rowTypeInfo, String charset) {
        super(outputPath);
        this.rowTypeInfo = rowTypeInfo;
        this.charsetName = charset;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) throws IllegalCharsetNameException, UnsupportedCharsetException {
        if (charsetName == null) {
            throw new NullPointerException();
        }

        if (!Charset.isSupported(charsetName)) {
            throw new UnsupportedCharsetException("The charset " + charsetName + " is not supported.");
        }

        this.charsetName = charsetName;
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        try {
            this.charset = Charset.forName(charsetName);
        }
        catch (IllegalCharsetNameException e) {
            throw new IOException("The charset " + charsetName + " is not valid.", e);
        }
        catch (UnsupportedCharsetException e) {
            throw new IOException("The charset " + charsetName + " is not supported.", e);
        }
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        final JSONObject json = getJson(record, rowTypeInfo);
        byte[] bytes = json.toString().getBytes(charset);
        this.stream.write(bytes);
        this.stream.write(NEWLINE);
    }

    private JSONObject getJson(Row record,RowTypeInfo rowTypeInfo){
        String[] fieldNames = rowTypeInfo.getFieldNames();
        int i = 0;
        JSONObject json = new JSONObject();
        for (String name : fieldNames){
            Object field = record.getField(i);
            final TypeInformation type = rowTypeInfo.getTypeAt(i);
            if (type.isBasicType()){
                json.put(name, field);
            }else if (type instanceof ObjectArrayTypeInfo){
                ObjectArrayTypeInfo arrayTypeInfo = (ObjectArrayTypeInfo) type;
                TypeInformation componentInfo = arrayTypeInfo.getComponentInfo();
                JSONArray jsonArray = new JSONArray();
                if (componentInfo instanceof RowTypeInfo){
                    final Row[] rows = (Row[]) field;
                    for (Row r : rows){
                        jsonArray.add(getJson(r,(RowTypeInfo)componentInfo));
                    }
                }else {
                    final Object[] objects = (Object[]) field;
                    for (Object o : objects){
                        jsonArray.add(o);
                    }
                }
                json.put(name,jsonArray);
            }else if (type instanceof RowTypeInfo){
                RowTypeInfo typeInfo = (RowTypeInfo) type;
                json.put(name,getJson((Row)field,typeInfo));
            }
            i++;
        }
        return json;
    }
}
