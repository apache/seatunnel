package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

/**
 * @author mr_xiong
 * @date 2019-09-07 13:57
 * @description
 */
public class FileSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);

    private Config config;

    private FileOutputFormat outputFormat;

    private RowTypeInfo rowTypeInfo;

    private Path filePath;

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        final StreamingFileSink<Row> sink = StreamingFileSink
                .forRowFormat(filePath, (Encoder<Row>) (element, stream) -> {
                    PrintStream out = new PrintStream(stream);
                    out.println(element);
                })
                .build();

        return dataStream.addSink(sink);
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        rowTypeInfo = (RowTypeInfo) dataSet.getType();
        return dataSet.output(outputFormat);
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
        return CheckConfigUtil.check(config,"file.path","file.format");
    }

    @Override
    public void prepare() {
        String path = config.getString("file.path");
        String format = config.getString("file.format");
        filePath = new Path(path);
        switch (format) {
            case "json":
                outputFormat = new JsonRowOutputFormat(filePath, rowTypeInfo);
                break;
            case "csv":
                CsvRowOutputFormat csvFormat = new CsvRowOutputFormat(filePath);
                outputFormat = csvFormat;
                break;
            case "text":
                outputFormat = new TextOutputFormat(filePath);
                break;
            default:
                LOG.warn(" unknown file_format [{}],only support json,csv,text", format);
                break;

        }
        if (config.hasPath("file.write.mode")){
            String mode = config.getString("file.write.mode");
            outputFormat.setWriteMode(FileSystem.WriteMode.valueOf(mode));
        }
    }
}
