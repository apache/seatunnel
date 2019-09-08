package io.github.interestinglab.waterdrop.flink.sink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;

/**
 * @author mr_xiong
 * @date 2019-09-07 17:35
 * @description
 */
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
        String[] fieldNames = rowTypeInfo.getFieldNames();
        int i = 0;
        JSONObject json = new JSONObject();
        for (String name : fieldNames){
            json.put(name,record.getField(i++));
        }
        byte[] bytes = json.toString().getBytes(charset);
        this.stream.write(bytes);
        this.stream.write(NEWLINE);
    }
}
