package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.io.IOException;

public class ConsoleSink extends RichOutputFormat<Row> implements FlinkBatchSink<Row, Row>, FlinkStreamSink<Row,Row> {

    private Config config;

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> rowDataSet) {
        return rowDataSet.output(this);
    }

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream.print();
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
        return new CheckResult(true,"");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(Row record) throws IOException {
        System.out.println(record);
    }

    @Override
    public void close() throws IOException {

    }
}
