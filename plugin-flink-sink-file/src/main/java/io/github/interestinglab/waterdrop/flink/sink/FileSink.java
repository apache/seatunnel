package io.github.interestinglab.waterdrop.flink.sink;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.types.Row;

/**
 * @author mr_xiong
 * @date 2019-09-07 13:57
 * @description
 */
public class FileSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row,Row> {

    private Config config;

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {

        Path path = new Path("hdfs://localhost:9000/output/csv");
//        final StreamingFileSink<Row> sink = StreamingFileSink
//                .forBulkFormat(path, ParquetAvroWriters.forReflectRecord(Row.class))
////                .forRowFormat(new Path("/Users/mr_xiong/test.text"), (Encoder<Row>) (element, stream) -> {
////                    PrintStream out = new PrintStream(stream);
////                    out.println(element);
////                })
//                .build();

        CsvRowOutputFormat format = new CsvRowOutputFormat(path);
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        return dataStream.writeUsingOutputFormat(format);
//        return dataStream.writeAsText("hdfs://localhost:9000/output/text", FileSystem.WriteMode.OVERWRITE);
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        Path path = new Path("hdfs://localhost:9000/output/batch/csv");
        CsvRowOutputFormat format = new CsvRowOutputFormat(path);
        format.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        return  dataSet.output(format);
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
        return null;
    }

    @Override
    public void prepare() {

    }
}
