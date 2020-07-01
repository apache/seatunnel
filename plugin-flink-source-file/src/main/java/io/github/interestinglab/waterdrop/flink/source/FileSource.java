package io.github.interestinglab.waterdrop.flink.source;

import com.alibaba.fastjson.JSONObject;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSource;
import io.github.interestinglab.waterdrop.flink.util.SchemaUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.avro.Schema;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;

import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.orc.OrcRowInputFormat;
import org.apache.flink.types.Row;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Map;


public class FileSource implements FlinkBatchSource<Row> {

    private Config config;

    private InputFormat inputFormat;

    private final static String PATH = "path";
    private final static String SOURCE_FORMAT = "format.type";
    private final static String SCHEMA = "schema";

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(inputFormat);
    }


    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config,PATH,SOURCE_FORMAT,SCHEMA);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        String path = config.getString(PATH);
        String format = config.getString(SOURCE_FORMAT);
        String schemaContent = config.getString(SCHEMA);
        Path filePath = new Path(path);
        switch (format) {
            case "json":
                Object jsonSchemaInfo = JSONObject.parse(schemaContent);
                RowTypeInfo jsonInfo = SchemaUtil.getTypeInformation((JSONObject) jsonSchemaInfo);
                JsonRowInputFormat jsonInputFormat = new JsonRowInputFormat(filePath, null, jsonInfo);
                inputFormat = jsonInputFormat;
                break;
            case "parquet":
                final Schema parse = new Schema.Parser().parse(schemaContent);
                final MessageType messageType = new AvroSchemaConverter().convert(parse);
                inputFormat = new ParquetRowInputFormat(filePath, messageType);
                break;
            case "orc":
                OrcRowInputFormat orcRowInputFormat = new OrcRowInputFormat(path, schemaContent, null, 1000);
                this.inputFormat = orcRowInputFormat;
                break;
            case "csv":
                Object csvSchemaInfo = JSONObject.parse(schemaContent);
                TypeInformation[] csvType = SchemaUtil.getCsvType((List<Map<String, String>>) csvSchemaInfo);
                RowCsvInputFormat rowCsvInputFormat = new RowCsvInputFormat(filePath, csvType, true);
                this.inputFormat = rowCsvInputFormat;
                break;
            case "text":
                TextRowInputFormat textInputFormat = new TextRowInputFormat(filePath);
                inputFormat = textInputFormat;
                break;
            default:
                break;
        }

    }
}
